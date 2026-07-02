import json
import os
import re
import time
import math
import random
import copy
import hashlib
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import boto3

s3 = boto3.client("s3")
lambda_api = boto3.client("lambda")
sqs_api = boto3.client("sqs")
sns_api = boto3.client("sns")
ssm = boto3.client("ssm")
DEFAULT_BL_API_URL = "https://api.baselinker.com/connector.php"
BL_API_TOKEN_SSM_PARAM = os.getenv(
    "BL_API_TOKEN_SSM_PARAM",
    "/baselinker-sync/api-token",
)
DEFAULT_BL_API_TIMEOUT_SEC = 60
DEFAULT_BL_API_MAX_RPM = 90
BL_API_HARD_MAX_RPM = 100
BL_API_WINDOW_SECONDS = 60.0
DEFAULT_BL_MAX_UPSERTS_PER_RUN = 600
DEFAULT_BL_MAX_RECORDS_PER_RUN = 2000
DEFAULT_BL_REMOTE_CACHE_TTL_SEC = 6 * 60 * 60
DEFAULT_BL_BULK_UPDATE_ENABLED = True
DEFAULT_BL_BULK_UPDATE_MAX_ITEMS = 1000
DEFAULT_BL_BULK_UPDATE_MIN_ITEMS = 5
DEFAULT_BL_ETA_MOVING_AVG_ENABLED = True
DEFAULT_BL_ETA_MA_ALPHA = 0.30
DEFAULT_BL_ETA_MA_MIN_RPM = 1
DEFAULT_BL_ETA_MA_BOOTSTRAP_SEC = 45
DEFAULT_BL_ENABLE_SELF_CHAIN = True
DEFAULT_BL_MAX_CHAIN_DEPTH = 200
DEFAULT_BL_PROGRESS_UPDATE_EVERY = 100
DEFAULT_BL_MIN_REMAINING_MS_FOR_CONTINUE = 60_000
DEFAULT_BL_FULL_AUDIT_ENABLED = True
DEFAULT_BL_FULL_AUDIT_DETAILS_LIMIT_PER_TYPE = 20
DEFAULT_BL_FULL_AUDIT_MAX_DETAILS_ROWS = 50000
DEFAULT_BL_SYNC_STATUS_SSM_PARAM = "/baselinker-sync/push-sync-status"
DEFAULT_BUDGET_FX_RATE_SSM_PARAM = "/baselinker-sync/usd-pln-rate"
DEFAULT_NBP_USD_PLN_URL = "https://api.nbp.pl/api/exchangerates/rates/a/usd/?format=json"
DEFAULT_BUDGET_USD_TO_PLN_RATE = 4.0
DEFAULT_BL_RESET_STATE_IF_STATUS_STALE_ENABLED = True
DEFAULT_BL_RESET_STATE_IF_STATUS_STALE_SEC = 60 * 60
DEFAULT_BL_BLOCKED_TOKEN_RESUME_BUFFER_SEC = 5
DEFAULT_BL_BLOCKED_TOKEN_RESUME_JITTER_SEC = 7
DEFAULT_BL_BLOCKED_TOKEN_FALLBACK_DELAY_SEC = 45
DEFAULT_BL_BLOCKED_TOKEN_MAX_INLINE_WAIT_SEC = 70
DEFAULT_BL_CONTINUATION_BLOCKED_MIN_DELAY_SEC = 65
_BL_API_RATE_LIMITER = {
    "max_rpm": DEFAULT_BL_API_MAX_RPM,
    "window_sec": BL_API_WINDOW_SECONDS,
    "timestamps": deque(),
}
POLAND_TZ = ZoneInfo("Europe/Warsaw")
AUDIT_PRICE_TOL = 0.01
AUDIT_NUM_TOL = 0.0001
ProductRecord = Dict[str, Any]


def _clean(value):
    if value is None:
        return ""
    return str(value).strip()


def _pick_text(elem: ET.Element, paths: List[str]) -> str:
    for path in paths:
        node = elem.find(path)
        if node is None:
            continue
        text = _clean(node.text)
        if text:
            return text
    return ""


def _normalize_parent_id(raw_parent_id: str) -> str:
    value = _clean(raw_parent_id)
    if value in {"", "0", "-1"}:
        return ""
    if value.lower() in {"null", "none"}:
        return ""
    return value


def _parse_int(raw_value: str, default: int = 0) -> int:
    value = _clean(raw_value).replace(",", ".")
    if value == "":
        return default
    try:
        return int(float(value))
    except ValueError:
        return default


def _normalize_decimal(raw_value: str, default: str = "0") -> str:
    value = _clean(raw_value).replace(",", ".")
    if value == "":
        return default
    return value


def _normalize_tax_rate(raw_tax_rate: str) -> str:
    value = _clean(raw_tax_rate).replace(",", ".")
    if value == "":
        return ""
    if value.endswith("%"):
        return value
    if re.fullmatch(r"-?\d+(\.\d+)?", value):
        return f"{value}%"
    return value


def _unique_nonempty(values: List[str]) -> List[str]:
    unique_values: List[str] = []
    seen_values: Set[str] = set()

    for value in values:
        if not value:
            continue
        if value in seen_values:
            continue
        seen_values.add(value)
        unique_values.append(value)

    return unique_values


def _collect_images(product_element: ET.Element) -> List[str]:
    image_candidates: List[str] = []

    for tag in ["image", "main_image", "image_url"]:
        image_url = _pick_text(product_element, [tag])
        if image_url != "":
            image_candidates.append(image_url)

    for image_index in range(1, 16):
        image_url = _pick_text(product_element, [f"image_extra_{image_index}"])
        if image_url != "":
            image_candidates.append(image_url)

    for image_element in product_element.findall("images/image"):
        image_url = _clean(image_element.text)
        if image_url != "":
            image_candidates.append(image_url)

    # BaseLinker should receive each image once even if the source feed exposes the
    # same URL through the main image and the extra-images collection.
    return _unique_nonempty(image_candidates)


def _collect_attributes(product_element: ET.Element) -> List[Tuple[str, str]]:
    attributes: List[Tuple[str, str]] = []

    for attribute_element in product_element.findall("attributes/attribute"):
        attribute_name = _pick_text(attribute_element, ["attribute_name", "name"])
        attribute_value = _pick_text(attribute_element, ["attribute_value", "value"])
        if attribute_name == "" and attribute_value == "":
            continue
        attributes.append((attribute_name, attribute_value))

    return attributes


def _parse_records(root: ET.Element) -> List[ProductRecord]:
    product_records: List[ProductRecord] = []

    for product_element in list(root):
        if product_element.tag.lower() not in {"item", "product"}:
            continue

        product_id = _pick_text(product_element, ["id", "product_id"])
        if product_id == "":
            continue

        parent_id = _normalize_parent_id(
            _pick_text(product_element, ["parent_id", "parentId", "ParentId"])
        )

        product_record: ProductRecord = {
            "id": product_id,
            "parent_id": parent_id,
            "name": _pick_text(product_element, ["name"]),
            "sku": _pick_text(product_element, ["sku", "code"]),
            "ean": _pick_text(product_element, ["ean", "upc"]),
            "quantity": _parse_int(
                _pick_text(product_element, ["quantity", "stock_quantity"]),
                0,
            ),
            "price": _normalize_decimal(
                _pick_text(product_element, ["price", "price_gross"]),
                "0",
            ),
            "purchase_price": _normalize_decimal(
                _pick_text(product_element, ["purchase_price"]),
                "",
            ),
            "tax_rate": _normalize_tax_rate(
                _pick_text(product_element, ["tax_rate"])
            ),
            "weight": _normalize_decimal(
                _pick_text(product_element, ["weight"]),
                "0",
            ),
            "width": _normalize_decimal(
                _pick_text(product_element, ["width"]),
                "0",
            ),
            "height": _normalize_decimal(
                _pick_text(product_element, ["height"]),
                "0",
            ),
            "length": _normalize_decimal(
                _pick_text(product_element, ["length"]),
                "0",
            ),
            "description": _pick_text(product_element, ["description"]),
            "description_extra_1": _pick_text(
                product_element,
                ["description_extra_1"],
            ),
            "description_extra_2": _pick_text(
                product_element,
                ["description_extra_2"],
            ),
            "category_name": _pick_text(
                product_element,
                ["category_name", "category_path"],
            ),
            "manufacturer_name": _pick_text(
                product_element,
                ["manufacturer_name", "brand"],
            ),
            "images": _collect_images(product_element),
            "attributes": _collect_attributes(product_element),
        }
        product_records.append(product_record)

    return product_records


def _build_relationships(
    records: List[ProductRecord],
    include_orphans_as_products: bool,
) -> Tuple[List[ProductRecord], Dict[str, List[ProductRecord]], Dict[str, int]]:
    record_ids = {record["id"] for record in records}
    variants_by_parent: Dict[str, List[ProductRecord]] = defaultdict(list)

    orphan_variants = 0
    for record in records:
        parent_id = record["parent_id"]
        if parent_id == "":
            continue
        has_valid_parent = parent_id in record_ids and parent_id != record["id"]
        if has_valid_parent:
            variants_by_parent[parent_id].append(record)
        else:
            orphan_variants += 1

    output_records: List[ProductRecord] = []
    output_variant_products = 0
    output_orphan_products = 0
    dropped_zero_qty_variant_products = 0
    dropped_zero_qty_orphan_products = 0

    for record in records:
        parent_id = record["parent_id"]
        quantity = int(record["quantity"])

        if parent_id == "":
            # A parent can have zero stock because sellable stock is stored on
            # its variants. It must still be exported to preserve relations.
            output_records.append(record)
            continue

        is_valid_variant = parent_id in record_ids and parent_id != record["id"]
        if is_valid_variant:
            if quantity <= 0:
                dropped_zero_qty_variant_products += 1
                continue
            output_records.append(record)
            output_variant_products += 1
            continue

        if include_orphans_as_products:
            if quantity <= 0:
                dropped_zero_qty_orphan_products += 1
                continue
            output_records.append(record)
            output_orphan_products += 1

    stats: Dict[str, int] = {
        "input_records": len(records),
        "output_products": len(output_records),
        "output_variant_products": output_variant_products,
        "output_orphan_products": output_orphan_products,
        "orphan_variants": orphan_variants,
        "dropped_zero_qty_variant_products": dropped_zero_qty_variant_products,
        "dropped_zero_qty_orphan_products": dropped_zero_qty_orphan_products,
    }
    return output_records, variants_by_parent, stats


def _download(url: str, timeout_sec: int, retries: int = 3) -> bytes:
    last_error: Optional[Exception] = None
    headers = {
        "User-Agent": "xml-baselinker-sync/1.0",
        "Accept": "application/xml,text/xml,*/*",
    }

    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url=url, headers=headers, method="GET")
            with urllib.request.urlopen(request, timeout=timeout_sec) as response:
                return response.read()
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(2 ** (attempt - 1))

    raise RuntimeError(
        f"Failed to download source feed after {retries} attempts: {last_error}"
    )


def _env_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value.strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return float(raw_value.strip())
    except Exception:
        return default


def _env_str(name: str, default: str = "") -> str:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return str(raw_value).strip()


def _post_audit_issue_details(sync_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if bool(sync_result.get("has_more_batches")):
        return None
    if not bool(sync_result.get("post_audit_executed")):
        return None

    raw_summary = sync_result.get("post_audit_summary")
    audit_summary = raw_summary if isinstance(raw_summary, dict) else {}
    audit_error = _clean(sync_result.get("post_audit_error", ""))
    difference_count = _parse_int(str(audit_summary.get("diff_total", 0)), 0)
    if difference_count <= 0 and audit_error == "":
        return None

    return {
        "audit_summary": audit_summary,
        "audit_error": audit_error,
        "difference_count": max(0, difference_count),
        "summary_key": _clean(sync_result.get("post_audit_summary_key", "")),
        "details_key": _clean(sync_result.get("post_audit_details_key", "")),
    }


def _build_post_audit_alert_message(
    issue_details: Dict[str, Any],
    run_id: str,
    output_bucket: str,
    admin_portal_url: str,
) -> str:
    audit_summary = issue_details.get("audit_summary")
    if not isinstance(audit_summary, dict):
        audit_summary = {}

    lines = [
        "The post-sync consistency audit requires attention.",
        "",
        f"Run ID: {_clean(run_id) or 'unknown'}",
        f"Detected differences: {int(issue_details.get('difference_count', 0) or 0)}",
        f"Records with differences: {_parse_int(str(audit_summary.get('changed_records', 0)), 0)}",
        f"Missing in BaseLinker: {_parse_int(str(audit_summary.get('missing_in_bl', 0)), 0)}",
        f"Extra in BaseLinker: {_parse_int(str(audit_summary.get('extra_in_bl', 0)), 0)}",
    ]

    audit_error = _clean(issue_details.get("audit_error", ""))
    if audit_error != "":
        lines.extend(["", f"Audit error: {audit_error}"])

    raw_breakdown = audit_summary.get("diff_breakdown")
    if isinstance(raw_breakdown, dict) and raw_breakdown:
        lines.extend(["", "Difference breakdown:"])
        for difference_type in sorted(raw_breakdown):
            difference_count = _parse_int(str(raw_breakdown[difference_type]), 0)
            lines.append(f"- {difference_type}: {difference_count}")

    artifact_locations = []
    bucket_name = _clean(output_bucket)
    for label, key_name in (
        ("Summary", "summary_key"),
        ("Details", "details_key"),
    ):
        object_key = _clean(issue_details.get(key_name, ""))
        if bucket_name != "" and object_key != "":
            artifact_locations.append(f"- {label}: s3://{bucket_name}/{object_key}")
    if artifact_locations:
        lines.extend(["", "Audit artifacts:", *artifact_locations])

    portal_url = _clean(admin_portal_url)
    if portal_url != "":
        lines.extend(["", f"Administration portal: {portal_url}"])

    return "\n".join(lines)


def _publish_post_audit_alert(
    sync_result: Dict[str, Any],
    run_id: str,
    topic_arn: str,
    output_bucket: str,
    admin_portal_url: str,
) -> Dict[str, Any]:
    issue_details = _post_audit_issue_details(sync_result)
    if issue_details is None:
        return {
            "required": False,
            "published": False,
            "message_id": "",
            "error": "",
        }

    notification_topic_arn = _clean(topic_arn)
    if notification_topic_arn == "":
        error_message = "Post-sync audit alert topic is not configured."
        print(f"[post-sync-alert] {error_message}")
        return {
            "required": True,
            "published": False,
            "message_id": "",
            "error": error_message,
        }

    message = _build_post_audit_alert_message(
        issue_details=issue_details,
        run_id=run_id,
        output_bucket=output_bucket,
        admin_portal_url=admin_portal_url,
    )
    try:
        response = sns_api.publish(
            TopicArn=notification_topic_arn,
            Subject="XML-BaseLinker sync: post-sync audit alert",
            Message=message,
        )
    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        print(f"[post-sync-alert] failed to publish notification: {error_message}")
        return {
            "required": True,
            "published": False,
            "message_id": "",
            "error": error_message,
        }

    return {
        "required": True,
        "published": True,
        "message_id": _clean(response.get("MessageId", "")),
        "error": "",
    }


def _extract_blocked_token_until_unix(raw_error: str) -> int:
    text = _clean(raw_error)
    if text == "":
        return 0

    patterns = [
        (r"(\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}:\d{2})", "%d.%m.%Y %H:%M:%S"),
        (r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", "%Y-%m-%d %H:%M:%S"),
        (r"(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})", "%Y/%m/%d %H:%M:%S"),
    ]
    for pattern, fmt in patterns:
        for match in re.finditer(pattern, text):
            value = _clean(match.group(1))
            if value == "":
                continue
            try:
                dt = datetime.strptime(value, fmt).replace(tzinfo=POLAND_TZ)
                return int(dt.timestamp())
            except Exception:
                continue
    return 0


def _compute_blocked_token_resume_unix(
    blocked_until_unix: int,
    fallback_delay_sec: int,
    buffer_sec: int,
    jitter_sec: int,
) -> int:
    now_unix = int(time.time())
    base_unix = int(blocked_until_unix or 0)
    if base_unix <= now_unix:
        base_unix = now_unix + max(1, int(fallback_delay_sec or 1))
    candidate_unix = base_unix + max(0, int(buffer_sec or 0))
    jitter_max = max(0, int(jitter_sec or 0))
    if jitter_max > 0:
        candidate_unix += random.randint(0, jitter_max)
    return int(candidate_unix)


def _json_size_bytes(payload: Dict[str, Any]) -> int:
    serialized_payload = json.dumps(payload, ensure_ascii=False)
    return len(serialized_payload.encode("utf-8"))


def _compact_sync_status_section(status_section: Dict[str, Any]) -> Dict[str, Any]:
    compact_section: Dict[str, Any] = {}
    integer_fields = [
        "global_total_records",
        "global_processed",
        "global_requested",
        "global_updated",
        "global_skipped_unchanged",
        "global_errors_count",
        "sync_cursor_index",
        "errors_count",
    ]
    text_fields = [
        "phase",
        "sync_stage",
        "eta_finish_iso",
        "blocked_token_until_iso",
        "continuation_not_before_iso",
        "post_audit_skipped_reason",
    ]

    for field_name in integer_fields:
        if field_name not in status_section:
            continue
        try:
            compact_section[field_name] = int(
                status_section.get(field_name, 0) or 0
            )
        except Exception:
            continue

    for field_name in text_fields:
        field_value = _clean(status_section.get(field_name, ""))
        if field_value != "":
            compact_section[field_name] = field_value

    for field_name in (
        "has_more_batches",
        "continuation_enqueued",
        "post_audit_alert_required",
        "post_audit_alert_published",
    ):
        if field_name in status_section:
            compact_section[field_name] = bool(
                status_section.get(field_name, False)
            )

    audit_integer_fields = [
        "records_for_sync_total",
        "bl_products_list_total",
        "bl_products_details_total",
        "matched_records",
        "unchanged_records",
        "changed_records",
        "missing_in_bl",
        "extra_in_bl",
        "diff_total",
        "details_rows_written",
        "details_rows_truncated",
        "duration_sec",
    ]
    for summary_field_name in ("pre_audit_summary", "post_audit_summary"):
        audit_summary = status_section.get(summary_field_name)
        if not isinstance(audit_summary, dict):
            continue

        compact_summary: Dict[str, Any] = {}
        for field_name in audit_integer_fields:
            if field_name not in audit_summary:
                continue
            try:
                compact_summary[field_name] = int(
                    audit_summary.get(field_name, 0) or 0
                )
            except Exception:
                continue
        if compact_summary:
            compact_section[summary_field_name] = compact_summary

    for field_name in (
        "pre_audit_summary_key",
        "post_audit_summary_key",
        "pre_audit_error",
        "post_audit_error",
        "post_audit_alert_error",
    ):
        field_value = _clean(status_section.get(field_name, ""))
        if field_value != "":
            compact_section[field_name] = field_value[:512]

    for field_name in (
        "pre_audit_executed",
        "sync_skipped_no_changes",
        "post_audit_executed",
        "post_audit_skipped_no_changes",
    ):
        if field_name in status_section:
            compact_section[field_name] = bool(
                status_section.get(field_name, False)
            )

    return compact_section


def _safe_put_sync_status(ssm_param_name: str, payload: Dict[str, Any]) -> None:
    parameter_name = _clean(ssm_param_name)
    if parameter_name == "":
        return

    # Standard SSM Parameter Store hard-limit is 4096 bytes.
    # Keep a guard margin to avoid edge overflows with unicode bytes.
    max_bytes = 3800

    compact_payload = copy.deepcopy(payload)
    result_section = compact_payload.get("result")
    if isinstance(result_section, dict):
        compact_payload["result"] = _compact_sync_status_section(result_section)
    progress_section = compact_payload.get("progress")
    if isinstance(progress_section, dict):
        compact_payload["progress"] = _compact_sync_status_section(
            progress_section
        )
    serialized_payload = json.dumps(compact_payload, ensure_ascii=False)

    if _json_size_bytes(compact_payload) > max_bytes:
        metadata_only_payload = copy.deepcopy(compact_payload)
        metadata_only_payload.pop("result", None)
        metadata_only_payload.pop("progress", None)
        serialized_payload = json.dumps(metadata_only_payload, ensure_ascii=False)

    if len(serialized_payload.encode("utf-8")) > max_bytes:
        fallback_payload = {
            "status": _clean(payload.get("status", "")),
            "mode": _clean(payload.get("mode", "")),
            "run_id": _clean(payload.get("run_id", "")),
            "sync_chain_depth": int(payload.get("sync_chain_depth", 0) or 0),
            "updated_at_unix": int(
                payload.get("updated_at_unix", int(time.time()))
                or int(time.time())
            ),
            "updated_at_iso": _clean(
                payload.get(
                    "updated_at_iso",
                    datetime.now(timezone.utc).isoformat(),
                )
            ),
            "message": _clean(payload.get("message", ""))[:512],
            "truncated_for_ssm": True,
        }
        # Preserve the counters needed by the admin panel even when the full
        # status no longer fits in a Standard SSM parameter.
        original_progress = payload.get("progress")
        if isinstance(original_progress, dict):
            fallback_payload.update(
                _compact_sync_status_section(original_progress)
            )
        original_result = payload.get("result")
        if isinstance(original_result, dict):
            compact_result = _compact_sync_status_section(original_result)
            for field_name, field_value in compact_result.items():
                if field_name not in fallback_payload:
                    fallback_payload[field_name] = field_value
        serialized_payload = json.dumps(fallback_payload, ensure_ascii=False)

    try:
        ssm.put_parameter(
            Name=parameter_name,
            Value=serialized_payload,
            Type="String",
            Overwrite=True,
            Tier="Standard",
        )
    except Exception as exc:
        print(
            f"[sync-status] failed to write parameter "
            f"'{parameter_name}': {exc}"
        )


def _safe_get_sync_status(ssm_param_name: str) -> Dict[str, Any]:
    parameter_name = _clean(ssm_param_name)
    if parameter_name == "":
        return {}
    try:
        response = ssm.get_parameter(Name=parameter_name)
        parameter_value = _clean(response.get("Parameter", {}).get("Value", ""))
        if parameter_value == "":
            return {}
        status_payload = json.loads(parameter_value)
        if isinstance(status_payload, dict):
            return status_payload
        return {}
    except Exception as exc:
        print(
            f"[sync-status] failed to read parameter "
            f"'{parameter_name}': {exc}"
        )
        return {}


def _refresh_budget_fx_rate_ssm(
    parameter_name: str,
    fallback_usd_to_pln_rate: float,
    nbp_url: str = DEFAULT_NBP_USD_PLN_URL,
) -> Dict[str, Any]:
    fallback_rate = max(0.0, float(fallback_usd_to_pln_rate or DEFAULT_BUDGET_USD_TO_PLN_RATE))
    payload: Dict[str, Any] = {
        "base_currency": "USD",
        "display_currency": "PLN",
        "rate": round(fallback_rate, 4),
        "source": "fallback_env",
        "effective_date": "",
        "fetched_at_unix": int(time.time()),
        "fetched_at_iso": datetime.now(timezone.utc).isoformat(),
        "error": "",
    }
    try:
        request = urllib.request.Request(
            _clean(nbp_url) or DEFAULT_NBP_USD_PLN_URL,
            headers={
                "Accept": "application/json",
                "User-Agent": "xml-baselinker-sync/1.0",
            },
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=8) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
        rates = (
            response_payload.get("rates", [])
            if isinstance(response_payload, dict)
            else []
        )
        latest_rate = rates[0] if rates and isinstance(rates[0], dict) else {}
        rate = _to_float(str(latest_rate.get("mid", "")), 0)
        if rate <= 0:
            raise ValueError("NBP response does not contain a positive USD mid rate.")
        payload.update(
            {
                "rate": round(float(rate), 4),
                "source": "nbp",
                "effective_date": _clean(
                    latest_rate.get("effectiveDate", "")
                ),
                "error": "",
            }
        )
    except Exception as exc:
        payload["error"] = f"{type(exc).__name__}: {exc}"

    name = _clean(parameter_name)
    if name != "":
        try:
            ssm.put_parameter(
                Name=name,
                Value=json.dumps(payload, ensure_ascii=False),
                Type="String",
                Overwrite=True,
                Tier="Standard",
            )
        except Exception as exc:
            payload["ssm_error"] = f"{type(exc).__name__}: {exc}"
            print(f"[budget-fx] failed to write parameter '{name}': {exc}")
    return payload


def _extract_status_updated_at_unix(payload: Dict[str, Any]) -> int:
    if not isinstance(payload, dict):
        return 0
    candidates = [
        payload.get("updated_at_unix"),
        payload.get("progress", {}).get("updated_at_unix")
        if isinstance(payload.get("progress"), dict)
        else None,
        payload.get("result", {}).get("updated_at_unix")
        if isinstance(payload.get("result"), dict)
        else None,
    ]
    for raw in candidates:
        try:
            value = int(raw or 0)
        except Exception:
            value = 0
        if value > 0:
            return value
    iso_candidates = [
        _clean(payload.get("updated_at_iso", "")),
        _clean(payload.get("progress", {}).get("updated_at_iso", ""))
        if isinstance(payload.get("progress"), dict)
        else "",
        _clean(payload.get("result", {}).get("updated_at_iso", ""))
        if isinstance(payload.get("result"), dict)
        else "",
    ]
    for raw_iso in iso_candidates:
        if raw_iso == "":
            continue
        text = raw_iso.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except Exception:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    return 0


def _maybe_reset_stale_sync_state(
    output_bucket: str,
    output_key: str,
    sync_status_param: str,
    stale_after_sec: int,
) -> Dict[str, Any]:
    safe_stale_after_sec = max(1, int(stale_after_sec or 0))
    status_payload = _safe_get_sync_status(sync_status_param)
    updated_at_unix = _extract_status_updated_at_unix(status_payload)
    status_name = _clean(status_payload.get("status", ""))
    status_run_id = _clean(status_payload.get("run_id", ""))
    out: Dict[str, Any] = {
        "enabled": True,
        "reset_performed": False,
        "stale_after_sec": safe_stale_after_sec,
        "status_updated_at_unix": int(updated_at_unix),
        "status": status_name,
        "status_run_id": status_run_id,
        "status_age_sec": 0,
        "deleted_keys": [],
        "delete_errors": [],
    }
    if updated_at_unix <= 0:
        return out

    now_unix = int(time.time())
    status_age_sec = max(0, now_unix - int(updated_at_unix))
    out["status_age_sec"] = int(status_age_sec)
    if status_age_sec <= safe_stale_after_sec:
        return out

    keys = [
        _state_key_for_api_sync(output_key),
        _source_snapshot_key_for_api_sync(output_key),
    ]
    deleted_keys: List[str] = []
    delete_errors: List[str] = []
    for key in keys:
        try:
            s3.delete_object(Bucket=output_bucket, Key=key)
            deleted_keys.append(key)
        except Exception as exc:
            delete_errors.append(f"{key}: {type(exc).__name__}: {exc}")
    out["deleted_keys"] = deleted_keys
    out["delete_errors"] = delete_errors
    out["reset_performed"] = True
    return out


def _get_ssm_parameter_string(parameter_name: str) -> str:
    name = _clean(parameter_name)
    if name == "":
        raise RuntimeError("SSM parameter name is empty.")
    response = ssm.get_parameter(Name=name, WithDecryption=True)
    parameter = response.get("Parameter", {})
    value = parameter.get("Value")
    if isinstance(value, str) and value != "":
        return value
    raise RuntimeError(f"SSM parameter '{name}' is missing or empty.")


def _source_xml_url_from_config(config: Dict[str, Any]) -> str:
    if not isinstance(config, dict):
        return ""
    for key in ("source_xml_url", "xml_url"):
        value = _clean(config.get(key))
        if value != "":
            return value
    return ""


def _load_sync_config_from_ssm(
    parameter_name: str,
    default_source_xml_url: str,
    default_inventory_id: int,
    default_warehouse_id: str,
    default_api_max_rpm: int,
) -> Dict[str, Any]:
    config = {
        "source_xml_url": _clean(default_source_xml_url),
        "bl_inventory_id": int(default_inventory_id or 0),
        "bl_inventory_name": "",
        "bl_warehouse_id": _clean(default_warehouse_id),
        "bl_warehouse_name": "",
        "bl_api_max_rpm": int(default_api_max_rpm or DEFAULT_BL_API_MAX_RPM),
        "source": "lambda_env_defaults",
    }
    name = _clean(parameter_name)
    if name == "":
        return config
    try:
        raw = _get_ssm_parameter_string(name)
        parsed = json.loads(raw)
    except Exception:
        return config
    if not isinstance(parsed, dict):
        return config

    source_xml_url = _source_xml_url_from_config(parsed)
    if source_xml_url != "":
        config["source_xml_url"] = source_xml_url
    try:
        inventory_id = int(parsed.get("bl_inventory_id", parsed.get("inventory_id", 0)) or 0)
    except Exception:
        inventory_id = 0
    if inventory_id > 0:
        config["bl_inventory_id"] = inventory_id
    warehouse_id = _clean(
        parsed.get("bl_warehouse_id", parsed.get("warehouse_id", ""))
    )
    if warehouse_id != "":
        config["bl_warehouse_id"] = warehouse_id
    try:
        api_max_rpm = int(parsed.get("bl_api_max_rpm", parsed.get("api_max_rpm", 0)) or 0)
    except Exception:
        api_max_rpm = 0
    if api_max_rpm > 0:
        config["bl_api_max_rpm"] = max(1, min(BL_API_HARD_MAX_RPM, api_max_rpm))
    config["bl_inventory_name"] = _clean(parsed.get("bl_inventory_name", ""))
    config["bl_warehouse_name"] = _clean(parsed.get("bl_warehouse_name", ""))
    config["source"] = "ssm_parameter"
    return config


def _sync_config_digest(config: Dict[str, Any]) -> str:
    comparable = {
        "source_xml_url": _source_xml_url_from_config(config),
        "bl_inventory_id": int(config.get("bl_inventory_id", 0) or 0),
        "bl_warehouse_id": _clean(config.get("bl_warehouse_id", "")),
        "bl_api_max_rpm": int(config.get("bl_api_max_rpm", DEFAULT_BL_API_MAX_RPM) or DEFAULT_BL_API_MAX_RPM),
    }
    serialized = json.dumps(
        comparable,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha1(serialized.encode("utf-8")).hexdigest()


def _delete_api_sync_state(
    output_bucket: str,
    output_key: str,
    extra_snapshot_key: str = "",
) -> Dict[str, Any]:
    keys = [
        _state_key_for_api_sync(output_key),
        _source_snapshot_key_for_api_sync(output_key),
    ]
    snapshot_key = _clean(extra_snapshot_key)
    if snapshot_key and snapshot_key not in keys:
        keys.append(snapshot_key)
    deleted_keys: List[str] = []
    delete_errors: List[str] = []
    for key in keys:
        try:
            s3.delete_object(Bucket=output_bucket, Key=key)
            deleted_keys.append(key)
        except Exception as exc:
            delete_errors.append(f"{key}: {type(exc).__name__}: {exc}")
    return {"deleted_keys": deleted_keys, "delete_errors": delete_errors}


def _maybe_reset_state_for_config_change(
    output_bucket: str,
    output_key: str,
    current_config_digest: str,
) -> Dict[str, Any]:
    state_key = _state_key_for_api_sync(output_key)
    state = _load_json_state(output_bucket, state_key)
    previous_digest = _clean(state.get("sync_config_digest", ""))
    out: Dict[str, Any] = {
        "enabled": True,
        "reset_performed": False,
        "previous_digest": previous_digest,
        "current_digest": _clean(current_config_digest),
        "deleted_keys": [],
        "delete_errors": [],
    }
    if not state:
        return out
    if previous_digest == _clean(current_config_digest) and previous_digest != "":
        return out
    snapshot_key = _clean(state.get("sync_source_snapshot_key", ""))
    deleted = _delete_api_sync_state(
        output_bucket=output_bucket,
        output_key=output_key,
        extra_snapshot_key=snapshot_key,
    )
    out.update(deleted)
    out["reset_performed"] = True
    return out


def _resolve_bl_api_token() -> str:
    return _clean(_get_ssm_parameter_string(BL_API_TOKEN_SSM_PARAM))


def _state_key_for_api_sync(output_key: str) -> str:
    if output_key.lower().endswith(".xml"):
        return f"{output_key[:-4]}.bl-sync-state.json"
    return f"{output_key}.bl-sync-state.json"


def _source_snapshot_key_for_api_sync(output_key: str) -> str:
    if output_key.lower().endswith(".xml"):
        return f"{output_key[:-4]}.bl-sync-source.xml"
    return f"{output_key}.bl-sync-source.xml"


def _load_json_state(output_bucket: str, key: str) -> Dict[str, Any]:
    try:
        obj = s3.get_object(Bucket=output_bucket, Key=key)
        payload = obj["Body"].read()
        data = json.loads(payload.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_json_state(output_bucket: str, key: str, data: Dict[str, Any]) -> None:
    s3.put_object(
        Bucket=output_bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
        CacheControl="private, max-age=0, no-store",
    )


def _load_source_snapshot(output_bucket: str, key: str) -> Optional[bytes]:
    try:
        obj = s3.get_object(Bucket=output_bucket, Key=key)
        payload = obj["Body"].read()
        return payload if isinstance(payload, (bytes, bytearray)) and len(payload) > 0 else None
    except Exception:
        return None


def _save_source_snapshot(
    output_bucket: str,
    key: str,
    source_xml: bytes,
    source_digest: str,
) -> None:
    s3.put_object(
        Bucket=output_bucket,
        Key=key,
        Body=source_xml,
        ContentType="application/xml; charset=utf-8",
        CacheControl="private, max-age=0, no-store",
        Metadata={
            "sha1": _clean(source_digest),
            "saved-at-unix": str(int(time.time())),
        },
    )


def _to_float(raw: str, default: float = 0.0) -> float:
    value = _clean(raw).replace(",", ".").replace("%", "")
    if value == "":
        return default
    try:
        return float(value)
    except Exception:
        return default


def _configure_bl_rate_limiter(max_rpm: int) -> int:
    safe_max = max(1, min(BL_API_HARD_MAX_RPM, int(max_rpm or DEFAULT_BL_API_MAX_RPM)))
    _BL_API_RATE_LIMITER["max_rpm"] = safe_max
    timestamps = _BL_API_RATE_LIMITER.get("timestamps")
    if isinstance(timestamps, deque):
        timestamps.clear()
    else:
        _BL_API_RATE_LIMITER["timestamps"] = deque()
    return safe_max


def _wait_for_bl_rate_limit_slot() -> None:
    max_rpm = int(_BL_API_RATE_LIMITER.get("max_rpm", DEFAULT_BL_API_MAX_RPM))
    window_sec = float(_BL_API_RATE_LIMITER.get("window_sec", BL_API_WINDOW_SECONDS))
    timestamps = _BL_API_RATE_LIMITER.get("timestamps")
    if not isinstance(timestamps, deque):
        timestamps = deque()
        _BL_API_RATE_LIMITER["timestamps"] = timestamps

    while True:
        now = time.monotonic()
        cutoff = now - window_sec
        while timestamps and timestamps[0] <= cutoff:
            timestamps.popleft()
        if len(timestamps) < max_rpm:
            timestamps.append(now)
            return
        sleep_for = (timestamps[0] + window_sec) - now
        if sleep_for > 0:
            time.sleep(sleep_for + 0.01)


def _bl_api_call(
    api_url: str,
    api_token: str,
    method: str,
    parameters: Dict[str, Any],
    timeout_sec: int,
    retries: int = 3,
) -> Dict[str, Any]:
    payload = urllib.parse.urlencode(
        {
            "method": method,
            "parameters": json.dumps(parameters, ensure_ascii=False),
        }
    ).encode("utf-8")
    headers = {
        "X-BLToken": api_token,
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "User-Agent": "xml-baselinker-sync/1.0",
    }

    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            _wait_for_bl_rate_limit_slot()
            request = urllib.request.Request(
                api_url,
                data=payload,
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=timeout_sec) as response:
                response_body = response.read()
            response_payload = json.loads(response_body.decode("utf-8"))
            if not isinstance(response_payload, dict):
                raise RuntimeError(f"{method}: malformed response")
            if response_payload.get("status") != "SUCCESS":
                error_message = response_payload.get("error_message", "unknown error")
                error_code = response_payload.get("error_code", "")
                raise RuntimeError(f"{method}: {error_message} ({error_code})")
            return response_payload
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(2 ** (attempt - 1))
    raise RuntimeError(f"BL API call failed for {method}: {last_error}")


def _split_category_path(path: str) -> List[str]:
    normalized_path = _clean(path)
    if normalized_path == "":
        return []

    path_parts: List[str] = []
    for path_part in re.split(r"\s*/\s*|\s*>\s*", normalized_path):
        normalized_part = path_part.strip()
        if normalized_part != "":
            path_parts.append(normalized_part)
    return path_parts


def _stable_signature(record: Dict[str, Any], parent_source_id: str) -> str:
    payload = {
        "parent_source_id": parent_source_id,
        "name": record.get("name", ""),
        "sku": record.get("sku", ""),
        "ean": record.get("ean", ""),
        "qty": int(record.get("quantity", 0)),
        "price": record.get("price", ""),
        "tax_rate": record.get("tax_rate", ""),
        "weight": record.get("weight", ""),
        "width": record.get("width", ""),
        "height": record.get("height", ""),
        "length": record.get("length", ""),
        "description": record.get("description", ""),
        "description_extra_1": record.get("description_extra_1", ""),
        "description_extra_2": record.get("description_extra_2", ""),
        "category_name": record.get("category_name", ""),
        "manufacturer_name": record.get("manufacturer_name", ""),
        "images": record.get("images", []),
        "attributes": record.get("attributes", []),
    }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(serialized.encode("utf-8")).hexdigest()


def _stable_shape_signature(record: Dict[str, Any], parent_source_id: str) -> str:
    payload = {
        "parent_source_id": parent_source_id,
        "name": record.get("name", ""),
        "sku": record.get("sku", ""),
        "ean": record.get("ean", ""),
        "tax_rate": record.get("tax_rate", ""),
        "weight": record.get("weight", ""),
        "width": record.get("width", ""),
        "height": record.get("height", ""),
        "length": record.get("length", ""),
        "description": record.get("description", ""),
        "description_extra_1": record.get("description_extra_1", ""),
        "description_extra_2": record.get("description_extra_2", ""),
        "category_name": record.get("category_name", ""),
        "manufacturer_name": record.get("manufacturer_name", ""),
        "images": record.get("images", []),
        "attributes": record.get("attributes", []),
    }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(serialized.encode("utf-8")).hexdigest()


def _sanitize_category_maps_from_state(
    raw_by_parent: Any,
    raw_path_cache: Any,
) -> Tuple[Dict[int, Dict[str, int]], Dict[str, int]]:
    by_parent: Dict[int, Dict[str, int]] = defaultdict(dict)
    if isinstance(raw_by_parent, dict):
        for parent_key, children in raw_by_parent.items():
            try:
                parent_id = int(parent_key)
            except Exception:
                continue
            if not isinstance(children, dict):
                continue
            for name, category_id_raw in children.items():
                normalized_name = _clean(name).lower()
                if normalized_name == "":
                    continue
                try:
                    category_id = int(category_id_raw or 0)
                except Exception:
                    category_id = 0
                if category_id > 0:
                    by_parent[parent_id][normalized_name] = category_id

    path_cache: Dict[str, int] = {}
    if isinstance(raw_path_cache, dict):
        for path, category_id_raw in raw_path_cache.items():
            normalized_path = _clean(path)
            if normalized_path == "":
                continue
            try:
                category_id = int(category_id_raw or 0)
            except Exception:
                category_id = 0
            if category_id > 0:
                path_cache[normalized_path] = category_id

    return dict(by_parent), path_cache


def _serialize_category_maps_for_state(
    by_parent: Dict[int, Dict[str, int]],
    path_cache: Dict[str, int],
) -> Tuple[Dict[str, Dict[str, int]], Dict[str, int]]:
    by_parent_out: Dict[str, Dict[str, int]] = {}
    for parent_id, children in by_parent.items():
        try:
            parent_key = str(int(parent_id))
        except Exception:
            continue
        if not isinstance(children, dict):
            continue
        normalized_children: Dict[str, int] = {}
        for name, category_id in children.items():
            normalized_name = _clean(name).lower()
            if normalized_name == "":
                continue
            try:
                cid = int(category_id or 0)
            except Exception:
                cid = 0
            if cid > 0:
                normalized_children[normalized_name] = cid
        if normalized_children:
            by_parent_out[parent_key] = normalized_children

    path_cache_out: Dict[str, int] = {}
    for path, category_id in path_cache.items():
        normalized_path = _clean(path)
        if normalized_path == "":
            continue
        try:
            cid = int(category_id or 0)
        except Exception:
            cid = 0
        if cid > 0:
            path_cache_out[normalized_path] = cid

    return by_parent_out, path_cache_out


def _sanitize_manufacturer_map_from_state(raw_map: Any) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if not isinstance(raw_map, dict):
        return out
    for name, manufacturer_id_raw in raw_map.items():
        normalized_name = _clean(name).lower()
        if normalized_name == "":
            continue
        try:
            manufacturer_id = int(manufacturer_id_raw or 0)
        except Exception:
            manufacturer_id = 0
        if manufacturer_id > 0:
            out[normalized_name] = manufacturer_id
    return out


def _sanitize_sku_map_from_state(raw_map: Any) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = {}
    if not isinstance(raw_map, dict):
        return out
    for sku, ids_raw in raw_map.items():
        sku_key = _clean(sku).lower()
        if sku_key == "":
            continue
        normalized_ids: List[int] = []
        if isinstance(ids_raw, list):
            candidates = ids_raw
        else:
            candidates = [ids_raw]
        for pid_raw in candidates:
            try:
                pid = int(pid_raw or 0)
            except Exception:
                pid = 0
            if pid > 0 and pid not in normalized_ids:
                normalized_ids.append(pid)
        if normalized_ids:
            out[sku_key] = normalized_ids
    return out


def _ensure_category_maps(
    api_url: str,
    api_token: str,
    timeout_sec: int,
    inventory_id: int,
) -> Tuple[Dict[int, Dict[str, int]], Dict[str, int]]:
    res = _bl_api_call(
        api_url=api_url,
        api_token=api_token,
        method="getInventoryCategories",
        parameters={"inventory_id": inventory_id},
        timeout_sec=timeout_sec,
    )
    categories = res.get("categories", [])
    items: Dict[int, Dict[str, Any]] = {}
    for row in categories if isinstance(categories, list) else []:
        if not isinstance(row, dict):
            continue
        cid = int(row.get("category_id", 0) or 0)
        if cid <= 0:
            continue
        items[cid] = {
            "name": _clean(row.get("name", "")),
            "parent_id": int(row.get("parent_id", 0) or 0),
        }

    by_parent: Dict[int, Dict[str, int]] = defaultdict(dict)
    for cid, item in items.items():
        by_parent[item["parent_id"]][item["name"].lower()] = cid

    path_cache: Dict[str, int] = {}
    memo: Dict[int, str] = {}

    def _path_for(cid: int) -> str:
        if cid in memo:
            return memo[cid]
        node = items.get(cid)
        if not node:
            memo[cid] = ""
            return ""
        parent = int(node["parent_id"])
        name = _clean(node["name"])
        if parent <= 0 or parent not in items:
            memo[cid] = name
            return name
        parent_path = _path_for(parent)
        memo[cid] = f"{parent_path} / {name}" if parent_path else name
        return memo[cid]

    for cid in items:
        path = _path_for(cid)
        if path:
            path_cache[path] = cid

    return dict(by_parent), path_cache


def _ensure_category_id(
    category_name: str,
    by_parent: Dict[int, Dict[str, int]],
    path_cache: Dict[str, int],
    api_url: str,
    api_token: str,
    timeout_sec: int,
    inventory_id: int,
) -> int:
    segments = _split_category_path(category_name)
    if not segments:
        return 0

    current_path = ""
    parent_id = 0
    for seg in segments:
        current_path = seg if current_path == "" else f"{current_path} / {seg}"
        cached = path_cache.get(current_path)
        if cached:
            parent_id = cached
            continue
        existing = by_parent.get(parent_id, {}).get(seg.lower())
        if existing:
            parent_id = existing
            path_cache[current_path] = existing
            continue

        created = _bl_api_call(
            api_url=api_url,
            api_token=api_token,
            method="addInventoryCategory",
            parameters={
                "inventory_id": inventory_id,
                "name": seg,
                "parent_id": parent_id,
            },
            timeout_sec=timeout_sec,
        )
        cid = int(created.get("category_id", 0) or 0)
        if cid <= 0:
            raise RuntimeError(f"Failed to create category segment: {seg}")
        by_parent.setdefault(parent_id, {})[seg.lower()] = cid
        path_cache[current_path] = cid
        parent_id = cid

    return parent_id


def _fetch_manufacturer_map(
    api_url: str,
    api_token: str,
    timeout_sec: int,
) -> Dict[str, int]:
    page = 1
    mapping: Dict[str, int] = {}
    while True:
        res = _bl_api_call(
            api_url=api_url,
            api_token=api_token,
            method="getInventoryManufacturers",
            parameters={"page": page},
            timeout_sec=timeout_sec,
        )
        rows = res.get("manufacturers", [])
        if not isinstance(rows, list) or len(rows) == 0:
            break
        for row in rows:
            if not isinstance(row, dict):
                continue
            mid = int(row.get("manufacturer_id", 0) or 0)
            name = _clean(row.get("manufacturer_name") or row.get("name") or "")
            if mid > 0 and name:
                mapping[name.lower()] = mid
        if len(rows) < 1000:
            break
        page += 1
        if page > 100:
            break
    return mapping


def _fetch_existing_product_ids_by_sku(
    api_url: str,
    api_token: str,
    timeout_sec: int,
    inventory_id: int,
) -> Dict[str, List[int]]:
    sku_map: Dict[str, List[int]] = defaultdict(list)
    page = 1
    while True:
        res = _bl_api_call(
            api_url=api_url,
            api_token=api_token,
            method="getInventoryProductsList",
            parameters={
                "inventory_id": inventory_id,
                "page": page,
                "include_variants": True,
            },
            timeout_sec=timeout_sec,
        )
        products = res.get("products", {})
        if not isinstance(products, dict) or not products:
            break

        row_count = 0
        for row in products.values():
            if not isinstance(row, dict):
                continue
            row_count += 1
            pid = int(row.get("id", 0) or 0)
            sku = _clean(row.get("sku", ""))
            if pid <= 0 or sku == "":
                continue
            sku_key = sku.lower()
            if pid not in sku_map[sku_key]:
                sku_map[sku_key].append(pid)

        if row_count < 1000:
            break
        page += 1
        if page > 10000:
            break

    return dict(sku_map)


def _fetch_existing_bl_list_rows(
    api_url: str,
    api_token: str,
    timeout_sec: int,
    inventory_id: int,
) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    page = 1
    while True:
        res = _bl_api_call(
            api_url=api_url,
            api_token=api_token,
            method="getInventoryProductsList",
            parameters={
                "inventory_id": inventory_id,
                "page": page,
                "include_variants": True,
            },
            timeout_sec=timeout_sec,
        )
        products = res.get("products", {})
        if not isinstance(products, dict) or not products:
            break
        row_count = 0
        for pid_key, row in products.items():
            if not isinstance(row, dict):
                continue
            pid = int(row.get("id", pid_key) or 0)
            if pid <= 0:
                continue
            out[pid] = row
            row_count += 1
        if row_count < 1000:
            break
        page += 1
        if page > 10000:
            break
    return out


def _fetch_existing_bl_details(
    api_url: str,
    api_token: str,
    timeout_sec: int,
    inventory_id: int,
    product_ids: List[int],
    batch_size: int = 200,
) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    if not product_ids:
        return out
    safe_batch = max(1, int(batch_size or 200))
    for start in range(0, len(product_ids), safe_batch):
        batch = product_ids[start : start + safe_batch]
        res = _bl_api_call(
            api_url=api_url,
            api_token=api_token,
            method="getInventoryProductsData",
            parameters={"inventory_id": inventory_id, "products": batch},
            timeout_sec=timeout_sec,
        )
        products = res.get("products", {})
        if not isinstance(products, dict):
            continue
        for pid_key, row in products.items():
            if not isinstance(row, dict):
                continue
            pid = int(pid_key or 0)
            if pid <= 0:
                continue
            out[pid] = row
    return out


def _fetch_bl_parent_id(
    api_url: str,
    api_token: str,
    timeout_sec: int,
    inventory_id: int,
    product_id: int,
) -> int:
    pid = int(product_id or 0)
    if pid <= 0:
        return 0
    res = _bl_api_call(
        api_url=api_url,
        api_token=api_token,
        method="getInventoryProductsData",
        parameters={"inventory_id": inventory_id, "products": [pid]},
        timeout_sec=timeout_sec,
    )
    products = res.get("products", {})
    if not isinstance(products, dict):
        return 0
    row = products.get(str(pid))
    if not isinstance(row, dict):
        row = products.get(pid)
    if not isinstance(row, dict):
        return 0
    return int(row.get("parent_id", 0) or 0)


def _audit_features_from_attributes(attributes: List[Tuple[str, str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for name, value in attributes:
        n = _clean(name)
        v = _clean(value)
        if n and v:
            out[n] = v
    return out


def _audit_flatten_bl_features(raw: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            key = _clean(k)
            if key == "":
                continue
            if isinstance(v, (dict, list)):
                out[key] = json.dumps(v, ensure_ascii=False, sort_keys=True)
            else:
                out[key] = _clean(v)
        return out
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                key = _clean(item[0])
                val = _clean(item[1])
                if key:
                    out[key] = val
    return out


def _audit_normalize_bl_images(raw: Any) -> List[str]:
    if isinstance(raw, dict):
        parsed: List[Tuple[int, str]] = []
        for k, v in raw.items():
            pos = _parse_int(str(k), 0)
            url = _clean(v)
            if url:
                parsed.append((pos, url))
        parsed.sort(key=lambda x: x[0])
        return _unique_nonempty([url for _pos, url in parsed])
    if isinstance(raw, list):
        return _unique_nonempty([_clean(v) for v in raw if _clean(v) != ""])
    return []


def _audit_resolve_category_id(
    category_name: str,
    by_parent: Dict[int, Dict[str, int]],
    path_cache: Dict[str, int],
) -> int:
    segments = _split_category_path(category_name)
    if not segments:
        return 0
    current_path = ""
    parent_id = 0
    for seg in segments:
        current_path = seg if current_path == "" else f"{current_path} / {seg}"
        cached = path_cache.get(current_path)
        if cached:
            parent_id = int(cached)
            continue
        existing = by_parent.get(parent_id, {}).get(seg.lower())
        if existing:
            parent_id = int(existing)
            continue
        return 0
    return int(parent_id or 0)


def _audit_build_expected_record(
    record: Dict[str, Any],
    source_to_bl_id: Dict[str, int],
    by_parent: Dict[int, Dict[str, int]],
    path_cache: Dict[str, int],
    manufacturer_map: Dict[str, int],
    parent_source_ids_with_variants: Set[str],
) -> Dict[str, Any]:
    source_id = _clean(record.get("id", ""))
    parent_source_id = _normalize_parent_id(record.get("parent_id", ""))
    parent_bl_id = 0
    if parent_source_id:
        parent_bl_id = int(source_to_bl_id.get(parent_source_id, 0) or 0)

    name = _clean(record.get("name", ""))
    sku = _clean(record.get("sku", ""))
    if name == "":
        name = sku or source_id

    category_name = _clean(record.get("category_name", ""))
    category_id = _audit_resolve_category_id(
        category_name=category_name,
        by_parent=by_parent,
        path_cache=path_cache,
    )
    manufacturer_name = _clean(record.get("manufacturer_name", ""))
    manufacturer_id = int(manufacturer_map.get(manufacturer_name.lower(), 0) or 0) if manufacturer_name else 0
    skip_quantity_compare = bool(parent_source_id == "" and source_id in parent_source_ids_with_variants)

    return {
        "source_id": source_id,
        "parent_source_id": parent_source_id,
        "parent_bl_id": parent_bl_id,
        "name": re.sub(r"\s+", " ", name).strip(),
        "sku": sku,
        "ean": _clean(record.get("ean", "")),
        "price": _to_float(record.get("price", ""), default=0.0),
        "quantity": int(record.get("quantity", 0) or 0),
        "tax_rate": _to_float(record.get("tax_rate", ""), default=23.0),
        "weight": _to_float(record.get("weight", ""), default=0.0),
        "width": _to_float(record.get("width", ""), default=0.0),
        "height": _to_float(record.get("height", ""), default=0.0),
        "length": _to_float(record.get("length", ""), default=0.0),
        "description": re.sub(r"\s+", " ", _clean(record.get("description", ""))).strip(),
        "description_extra_1": re.sub(r"\s+", " ", _clean(record.get("description_extra_1", ""))).strip(),
        "description_extra_2": re.sub(r"\s+", " ", _clean(record.get("description_extra_2", ""))).strip(),
        "category_name": category_name,
        "category_id": int(category_id or 0),
        "manufacturer_name": manufacturer_name,
        "manufacturer_id": int(manufacturer_id or 0),
        "images": list(record.get("images", [])),
        "attributes": _audit_features_from_attributes(record.get("attributes", [])),
        "skip_quantity_compare": skip_quantity_compare,
    }


def _audit_build_actual_record(
    bl_id: int,
    row: Dict[str, Any],
    default_price_group: int,
    default_warehouse: str,
    category_id_to_path: Dict[int, str],
    manufacturer_id_to_name: Dict[int, str],
) -> Dict[str, Any]:
    text_fields = row.get("text_fields", {})
    if not isinstance(text_fields, dict):
        text_fields = {}
    features_raw = text_fields.get("features", {})
    prices = row.get("prices", {})
    if not isinstance(prices, dict):
        prices = {}
    stock = row.get("stock", {})
    if not isinstance(stock, dict):
        stock = {}
    category_id = int(row.get("category_id", 0) or 0)
    manufacturer_id = int(row.get("manufacturer_id", 0) or 0)

    return {
        "bl_id": int(bl_id),
        "parent_bl_id": int(row.get("parent_id", 0) or 0),
        "name": re.sub(r"\s+", " ", _clean(text_fields.get("name", ""))).strip(),
        "sku": _clean(row.get("sku", "")),
        "ean": _clean(row.get("ean", "")),
        "price": _to_float(prices.get(str(default_price_group), 0.0), default=0.0),
        "quantity": _parse_int(str(stock.get(default_warehouse, 0)), 0),
        "tax_rate": _to_float(row.get("tax_rate", ""), default=23.0),
        "weight": _to_float(row.get("weight", ""), default=0.0),
        "width": _to_float(row.get("width", ""), default=0.0),
        "height": _to_float(row.get("height", ""), default=0.0),
        "length": _to_float(row.get("length", ""), default=0.0),
        "description": re.sub(r"\s+", " ", _clean(text_fields.get("description", ""))).strip(),
        "description_extra_1": re.sub(r"\s+", " ", _clean(text_fields.get("description_extra1", ""))).strip(),
        "description_extra_2": re.sub(r"\s+", " ", _clean(text_fields.get("description_extra2", ""))).strip(),
        "category_id": int(category_id or 0),
        "category_name": _clean(category_id_to_path.get(category_id, "")),
        "manufacturer_id": int(manufacturer_id or 0),
        "manufacturer_name": _clean(manufacturer_id_to_name.get(manufacturer_id, "")),
        "images": _audit_normalize_bl_images(row.get("images", {})),
        "attributes": _audit_flatten_bl_features(features_raw),
    }


def _audit_images_match(expected_images: List[str], actual_images: List[str]) -> bool:
    expected = _unique_nonempty([_clean(v) for v in expected_images if _clean(v) != ""])
    actual = _unique_nonempty([_clean(v) for v in actual_images if _clean(v) != ""])
    if expected == actual:
        return True
    if not expected:
        return True
    if len(actual) < len(expected):
        return False
    # BL often appends inherited parent images to variant records.
    if actual[: len(expected)] == expected:
        return True
    pos = 0
    for url in actual:
        if pos < len(expected) and url == expected[pos]:
            pos += 1
            if pos >= len(expected):
                return True
    return False


def _audit_compare_records(expected: Dict[str, Any], actual: Dict[str, Any]) -> List[Dict[str, Any]]:
    diffs: List[Dict[str, Any]] = []
    source_id = _clean(expected.get("source_id", ""))
    bl_id = int(actual.get("bl_id", 0) or 0)

    def _push(field: str, expected_value: Any, actual_value: Any, diff_type: str = "field_mismatch") -> None:
        diffs.append(
            {
                "type": diff_type,
                "field": field,
                "source_id": source_id,
                "bl_id": bl_id,
                "expected": expected_value,
                "actual": actual_value,
            }
        )

    if int(expected.get("parent_bl_id", 0)) != int(actual.get("parent_bl_id", 0)):
        diffs.append(
            {
                "type": "parent_relation_mismatch",
                "field": "parent_bl_id",
                "source_id": source_id,
                "bl_id": bl_id,
                "expected": int(expected.get("parent_bl_id", 0)),
                "actual": int(actual.get("parent_bl_id", 0)),
                "expected_parent_source_id": _clean(expected.get("parent_source_id", "")),
            }
        )

    for field in ["name", "sku", "ean", "description", "description_extra_1", "description_extra_2"]:
        if _clean(expected.get(field, "")) != _clean(actual.get(field, "")):
            _push(field, expected.get(field, ""), actual.get(field, ""))

    def _float_diff(field: str, expected_value: float, actual_value: float, tol: float) -> None:
        if abs(float(expected_value) - float(actual_value)) <= tol:
            return
        _push(field, expected_value, actual_value)

    _float_diff("price", float(expected.get("price", 0.0)), float(actual.get("price", 0.0)), AUDIT_PRICE_TOL)
    _float_diff("tax_rate", float(expected.get("tax_rate", 0.0)), float(actual.get("tax_rate", 0.0)), AUDIT_NUM_TOL)
    _float_diff("weight", float(expected.get("weight", 0.0)), float(actual.get("weight", 0.0)), AUDIT_NUM_TOL)
    _float_diff("width", float(expected.get("width", 0.0)), float(actual.get("width", 0.0)), AUDIT_NUM_TOL)
    _float_diff("height", float(expected.get("height", 0.0)), float(actual.get("height", 0.0)), AUDIT_NUM_TOL)
    _float_diff("length", float(expected.get("length", 0.0)), float(actual.get("length", 0.0)), AUDIT_NUM_TOL)

    if not bool(expected.get("skip_quantity_compare", False)) and int(expected.get("quantity", 0)) != int(actual.get("quantity", 0)):
        _push("quantity", int(expected.get("quantity", 0)), int(actual.get("quantity", 0)))

    if int(expected.get("category_id", 0)) != int(actual.get("category_id", 0)):
        diffs.append(
            {
                "type": "category_mismatch",
                "field": "category_id",
                "source_id": source_id,
                "bl_id": bl_id,
                "expected": int(expected.get("category_id", 0)),
                "actual": int(actual.get("category_id", 0)),
                "expected_category_name": _clean(expected.get("category_name", "")),
                "actual_category_name": _clean(actual.get("category_name", "")),
            }
        )

    if int(expected.get("manufacturer_id", 0)) != int(actual.get("manufacturer_id", 0)):
        diffs.append(
            {
                "type": "manufacturer_mismatch",
                "field": "manufacturer_id",
                "source_id": source_id,
                "bl_id": bl_id,
                "expected": int(expected.get("manufacturer_id", 0)),
                "actual": int(actual.get("manufacturer_id", 0)),
                "expected_manufacturer_name": _clean(expected.get("manufacturer_name", "")),
                "actual_manufacturer_name": _clean(actual.get("manufacturer_name", "")),
            }
        )

    if not _audit_images_match(
        list(expected.get("images", [])),
        list(actual.get("images", [])),
    ):
        diffs.append(
            {
                "type": "images_mismatch",
                "field": "images",
                "source_id": source_id,
                "bl_id": bl_id,
                "expected_count": len(expected.get("images", [])),
                "actual_count": len(actual.get("images", [])),
                "expected": expected.get("images", []),
                "actual": actual.get("images", []),
            }
        )

    if dict(expected.get("attributes", {})) != dict(actual.get("attributes", {})):
        diffs.append(
            {
                "type": "attributes_mismatch",
                "field": "attributes",
                "source_id": source_id,
                "bl_id": bl_id,
                "expected_count": len(expected.get("attributes", {})),
                "actual_count": len(actual.get("attributes", {})),
                "expected": expected.get("attributes", {}),
                "actual": actual.get("attributes", {}),
            }
        )
    return diffs


def _audit_key_prefix_for_api_sync(output_key: str) -> str:
    if output_key.lower().endswith(".xml"):
        return output_key[:-4]
    return output_key


def _run_full_consistency_audit(
    phase: str,
    output_bucket: str,
    output_key: str,
    inventory_id: int,
    api_url: str,
    api_token: str,
    timeout_sec: int,
    default_price_group: int,
    default_warehouse: str,
    records_for_sync: List[Dict[str, Any]],
    source_to_bl_id: Dict[str, int],
    by_parent: Dict[int, Dict[str, int]],
    path_cache: Dict[str, int],
    manufacturer_map: Dict[str, int],
    parent_source_ids_with_variants: Optional[Set[str]],
    details_limit_per_type: int,
    max_details_rows: int,
) -> Dict[str, Any]:
    started_unix = int(time.time())
    safe_limit_per_type = max(1, int(details_limit_per_type or 20))
    safe_max_details_rows = max(0, int(max_details_rows or 0))
    phase_name = _clean(phase).lower() or "unknown"
    record_source_ids = {_clean(rec.get("id", "")) for rec in records_for_sync if _clean(rec.get("id", "")) != ""}
    parent_ids_for_quantity_skip: Set[str] = set()
    if isinstance(parent_source_ids_with_variants, set):
        parent_ids_for_quantity_skip = {
            _clean(x)
            for x in parent_source_ids_with_variants
            if _clean(x) != ""
        }
    else:
        for rec in records_for_sync:
            source_id = _clean(rec.get("id", ""))
            parent_source_id = _normalize_parent_id(rec.get("parent_id", ""))
            if source_id == "" or parent_source_id == "":
                continue
            if parent_source_id == source_id:
                continue
            if parent_source_id in record_source_ids:
                parent_ids_for_quantity_skip.add(parent_source_id)

    bl_list_rows = _fetch_existing_bl_list_rows(
        api_url=api_url,
        api_token=api_token,
        timeout_sec=timeout_sec,
        inventory_id=inventory_id,
    )
    bl_ids = sorted(bl_list_rows.keys())
    bl_details = _fetch_existing_bl_details(
        api_url=api_url,
        api_token=api_token,
        timeout_sec=timeout_sec,
        inventory_id=inventory_id,
        product_ids=bl_ids,
        batch_size=200,
    )

    manufacturer_id_to_name: Dict[int, str] = {}
    for name_lc, manufacturer_id in manufacturer_map.items():
        mid = int(manufacturer_id or 0)
        if mid <= 0:
            continue
        if mid not in manufacturer_id_to_name:
            manufacturer_id_to_name[mid] = _clean(name_lc)

    category_id_to_path: Dict[int, str] = {}
    for path, category_id in path_cache.items():
        cid = int(category_id or 0)
        if cid <= 0:
            continue
        if cid not in category_id_to_path:
            category_id_to_path[cid] = _clean(path)

    sku_to_bl_ids: Dict[str, List[int]] = defaultdict(list)
    for bl_id, row in bl_list_rows.items():
        sku = _clean(row.get("sku", "")).lower()
        if sku == "":
            continue
        if bl_id not in sku_to_bl_ids[sku]:
            sku_to_bl_ids[sku].append(int(bl_id))

    matched_source_to_bl_id: Dict[str, int] = {}
    matched_bl_ids: Set[int] = set()
    unchanged_source_ids: List[str] = []
    changed_source_ids: List[str] = []
    missing_source_ids: List[str] = []
    diffs_count_by_type: Dict[str, int] = defaultdict(int)
    diffs_samples_by_type: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    details_rows: List[Dict[str, Any]] = []
    details_truncated = 0

    def _register_diff(diff: Dict[str, Any]) -> None:
        nonlocal details_truncated
        diff_type = _clean(diff.get("type", "other")) or "other"
        diffs_count_by_type[diff_type] += 1
        if len(diffs_samples_by_type[diff_type]) < safe_limit_per_type:
            diffs_samples_by_type[diff_type].append(diff)
        if safe_max_details_rows > 0 and len(details_rows) < safe_max_details_rows:
            details_rows.append(diff)
        else:
            details_truncated += 1

    for rec in records_for_sync:
        source_id = _clean(rec.get("id", ""))
        if source_id == "":
            continue
        candidate_bl_id = int(source_to_bl_id.get(source_id, 0) or 0)
        if candidate_bl_id <= 0 or candidate_bl_id not in bl_details:
            sku_key = _clean(rec.get("sku", "")).lower()
            if sku_key:
                candidates = sku_to_bl_ids.get(sku_key, [])
                if len(candidates) == 1:
                    candidate_bl_id = int(candidates[0])
                else:
                    candidate_bl_id = 0
            else:
                candidate_bl_id = 0

        if candidate_bl_id <= 0 or candidate_bl_id not in bl_details:
            missing_source_ids.append(source_id)
            _register_diff(
                {
                    "type": "missing_in_bl",
                    "field": "bl_id",
                    "source_id": source_id,
                    "expected_sku": _clean(rec.get("sku", "")),
                    "expected_name": _clean(rec.get("name", "")),
                }
            )
            changed_source_ids.append(source_id)
            continue

        matched_source_to_bl_id[source_id] = int(candidate_bl_id)
        matched_bl_ids.add(int(candidate_bl_id))
        expected = _audit_build_expected_record(
            record=rec,
            source_to_bl_id=matched_source_to_bl_id,
            by_parent=by_parent,
            path_cache=path_cache,
            manufacturer_map=manufacturer_map,
            parent_source_ids_with_variants=parent_ids_for_quantity_skip,
        )
        actual = _audit_build_actual_record(
            bl_id=int(candidate_bl_id),
            row=bl_details[int(candidate_bl_id)],
            default_price_group=default_price_group,
            default_warehouse=default_warehouse,
            category_id_to_path=category_id_to_path,
            manufacturer_id_to_name=manufacturer_id_to_name,
        )
        row_diffs = _audit_compare_records(expected=expected, actual=actual)
        if row_diffs:
            changed_source_ids.append(source_id)
            for diff in row_diffs:
                _register_diff(diff)
        else:
            unchanged_source_ids.append(source_id)

    extra_bl_ids = sorted([int(pid) for pid in bl_ids if int(pid) not in matched_bl_ids])
    for bl_id in extra_bl_ids:
        row = bl_list_rows.get(int(bl_id), {})
        _register_diff(
            {
                "type": "extra_in_bl",
                "bl_id": int(bl_id),
                "sku": _clean(row.get("sku", "")),
                "name": _clean(row.get("name", "")),
                "parent_bl_id": int(row.get("parent_id", 0) or 0),
            }
        )

    diff_total = int(sum(diffs_count_by_type.values()))
    summary_by_type: Dict[str, Dict[str, Any]] = {}
    for diff_type, count in sorted(diffs_count_by_type.items(), key=lambda item: (-item[1], item[0])):
        summary_by_type[diff_type] = {
            "count": int(count),
            "sample": diffs_samples_by_type.get(diff_type, []),
        }

    prefix = _audit_key_prefix_for_api_sync(output_key)
    summary_key = f"{prefix}.bl-audit-{phase_name}.summary.json"
    details_key = f"{prefix}.bl-audit-{phase_name}.details.ndjson"
    latest_summary_key = summary_key
    latest_details_key = details_key

    summary_payload: Dict[str, Any] = {
        "generated_at_unix": int(time.time()),
        "generated_at_iso": datetime.now(timezone.utc).isoformat(),
        "phase": phase_name,
        "inventory_id": int(inventory_id),
        "records_for_sync_total": len(records_for_sync),
        "bl_products_list_total": len(bl_list_rows),
        "bl_products_details_total": len(bl_details),
        "matched_records": len(matched_source_to_bl_id),
        "unchanged_records": len(unchanged_source_ids),
        "changed_records": len(changed_source_ids),
        "missing_in_bl": len(missing_source_ids),
        "extra_in_bl": len(extra_bl_ids),
        "diff_total": diff_total,
        "diff_breakdown": {k: v["count"] for k, v in summary_by_type.items()},
        "diff_samples": summary_by_type,
        "details_rows_written": len(details_rows),
        "details_rows_truncated": int(details_truncated),
        "duration_sec": max(0, int(time.time()) - started_unix),
    }

    details_body = b""
    if details_rows:
        details_body = ("\n".join(json.dumps(row, ensure_ascii=False) for row in details_rows) + "\n").encode("utf-8")

    s3.put_object(
        Bucket=output_bucket,
        Key=summary_key,
        Body=json.dumps(summary_payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
        CacheControl="private, max-age=0, no-store",
    )
    s3.put_object(
        Bucket=output_bucket,
        Key=details_key,
        Body=details_body,
        ContentType="application/x-ndjson; charset=utf-8",
        CacheControl="private, max-age=0, no-store",
    )

    return {
        "phase": phase_name,
        "summary": summary_payload,
        "summary_key": summary_key,
        "details_key": details_key,
        "latest_summary_key": latest_summary_key,
        "latest_details_key": latest_details_key,
        "matched_source_to_bl_id": matched_source_to_bl_id,
        "unchanged_source_ids": unchanged_source_ids,
        "changed_source_ids": changed_source_ids,
        "missing_source_ids": missing_source_ids,
        "extra_bl_ids": extra_bl_ids,
    }


def _ensure_manufacturer_id(
    manufacturer_name: str,
    manufacturer_map: Dict[str, int],
    api_url: str,
    api_token: str,
    timeout_sec: int,
) -> int:
    normalized = _clean(manufacturer_name)
    if normalized == "":
        return 0
    key = normalized.lower()
    existing = manufacturer_map.get(key)
    if existing:
        return existing
    created = _bl_api_call(
        api_url=api_url,
        api_token=api_token,
        method="addInventoryManufacturer",
        parameters={"manufacturer_name": normalized},
        timeout_sec=timeout_sec,
    )
    mid = int(created.get("manufacturer_id", 0) or 0)
    if mid > 0:
        manufacturer_map[key] = mid
    return mid


def _build_product_payload(
    record: Dict[str, Any],
    inventory_id: int,
    default_price_group: int,
    default_warehouse: str,
    parent_bl_id: int,
    category_id: int,
    manufacturer_id: int,
    existing_bl_id: int,
    include_stock: bool = True,
) -> Dict[str, Any]:
    features: Dict[str, str] = {}
    for name, value in record.get("attributes", []):
        n = _clean(name)
        v = _clean(value)
        if n and v:
            features[n] = v

    product_name = _clean(record.get("name", ""))
    if product_name == "":
        product_name = _clean(record.get("sku", "")) or _clean(record.get("id", ""))

    text_fields: Dict[str, Any] = {
        "name": product_name,
        "description": _clean(record.get("description", "")),
        "description_extra1": _clean(record.get("description_extra_1", "")),
        "description_extra2": _clean(record.get("description_extra_2", "")),
    }
    if features:
        text_fields["features"] = features

    # Always send a full 16-slot image map so BL can also clear stale slots.
    # Slot 0 is main image, 1..15 are extras.
    images: Dict[str, str] = {str(idx): "" for idx in range(16)}
    image_pos = 0
    seen_image_urls: Set[str] = set()
    for image_url in record.get("images", []):
        url = _clean(image_url)
        if not url:
            continue
        if url in seen_image_urls:
            continue
        seen_image_urls.add(url)
        images[str(image_pos)] = f"url:{url}"
        image_pos += 1
        if image_pos >= 16:
            break

    payload: Dict[str, Any] = {
        "inventory_id": inventory_id,
        "sku": _clean(record.get("sku", "")),
        "ean": _clean(record.get("ean", "")),
        "tax_rate": _to_float(record.get("tax_rate", ""), default=23.0),
        "weight": _to_float(record.get("weight", ""), default=0.0),
        "width": _to_float(record.get("width", ""), default=0.0),
        "height": _to_float(record.get("height", ""), default=0.0),
        "length": _to_float(record.get("length", ""), default=0.0),
        "prices": {str(default_price_group): _to_float(record.get("price", ""), default=0.0)},
        "text_fields": text_fields,
    }
    if include_stock:
        payload["stock"] = {default_warehouse: int(record.get("quantity", 0) or 0)}
    payload["images"] = images
    if existing_bl_id > 0:
        payload["product_id"] = existing_bl_id
    if parent_bl_id > 0:
        payload["parent_id"] = parent_bl_id
    if category_id > 0:
        payload["category_id"] = category_id
    if manufacturer_id > 0:
        payload["manufacturer_id"] = manufacturer_id
    return payload


def _sync_to_bl_api(
    source_xml: Optional[bytes],
    include_orphans_as_products: bool,
    output_bucket: str,
    output_key: str,
    sync_config_digest: str,
    active_sync_config: Optional[Dict[str, Any]],
    api_url: str,
    api_token: str,
    inventory_id: int,
    preferred_warehouse_id: str,
    timeout_sec: int,
    max_upserts_per_run: int,
    max_records_per_run: int,
    api_rate_limit_rpm: int,
    progress_update_every: int,
    remote_cache_ttl_sec: int = DEFAULT_BL_REMOTE_CACHE_TTL_SEC,
    bulk_update_enabled: bool = DEFAULT_BL_BULK_UPDATE_ENABLED,
    bulk_update_max_items: int = DEFAULT_BL_BULK_UPDATE_MAX_ITEMS,
    bulk_update_min_items: int = DEFAULT_BL_BULK_UPDATE_MIN_ITEMS,
    eta_moving_avg_enabled: bool = DEFAULT_BL_ETA_MOVING_AVG_ENABLED,
    eta_ma_alpha: float = DEFAULT_BL_ETA_MA_ALPHA,
    eta_ma_min_rpm: int = DEFAULT_BL_ETA_MA_MIN_RPM,
    eta_ma_bootstrap_sec: int = DEFAULT_BL_ETA_MA_BOOTSTRAP_SEC,
    full_audit_enabled: bool = DEFAULT_BL_FULL_AUDIT_ENABLED,
    full_audit_details_limit_per_type: int = DEFAULT_BL_FULL_AUDIT_DETAILS_LIMIT_PER_TYPE,
    full_audit_max_details_rows: int = DEFAULT_BL_FULL_AUDIT_MAX_DETAILS_ROWS,
    source_live_digest_hint: str = "",
    invocation_started_unix: int = 0,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    remaining_ms_fn: Optional[Callable[[], int]] = None,
    min_remaining_ms_for_continue: int = DEFAULT_BL_MIN_REMAINING_MS_FOR_CONTINUE,
) -> Dict[str, Any]:
    source_xml_bytes: Optional[bytes]
    if isinstance(source_xml, (bytes, bytearray)) and len(source_xml) > 0:
        source_xml_bytes = bytes(source_xml)
    else:
        source_xml_bytes = None

    live_source_digest = _clean(source_live_digest_hint)
    if live_source_digest == "" and source_xml_bytes is not None:
        live_source_digest = hashlib.sha1(source_xml_bytes).hexdigest()
    state_key = _state_key_for_api_sync(output_key)
    state = _load_json_state(output_bucket=output_bucket, key=state_key)

    snapshot_key = _clean(state.get("sync_source_snapshot_key", ""))
    if snapshot_key == "":
        snapshot_key = _source_snapshot_key_for_api_sync(output_key)
    snapshot_digest = _clean(state.get("sync_source_snapshot_digest", ""))

    raw_state_cursor_index = state.get("sync_cursor_index", 0)
    try:
        state_cursor_index = int(raw_state_cursor_index or 0)
    except Exception:
        state_cursor_index = 0
    if state_cursor_index < 0:
        state_cursor_index = 0

    source_snapshot_reused = False
    source_snapshot_refreshed = False
    source_snapshot_error = ""
    effective_source_xml = source_xml_bytes
    effective_source_digest = live_source_digest

    # Freeze source feed per sync cycle:
    # - cursor>0: resume from snapshot stored at cycle start
    # - cursor==0: start a new cycle and refresh snapshot from current live feed
    if state_cursor_index > 0:
        snapshot_bytes = _load_source_snapshot(output_bucket=output_bucket, key=snapshot_key)
        if snapshot_bytes is not None:
            effective_source_xml = snapshot_bytes
            effective_source_digest = (
                snapshot_digest if snapshot_digest != "" else hashlib.sha1(snapshot_bytes).hexdigest()
            )
            source_snapshot_reused = True
            if live_source_digest == "":
                live_source_digest = effective_source_digest
        elif source_xml_bytes is not None:
            effective_source_xml = source_xml_bytes
            effective_source_digest = (
                snapshot_digest
                if snapshot_digest != ""
                else hashlib.sha1(source_xml_bytes).hexdigest()
            )
            source_snapshot_reused = True
            source_snapshot_error = "snapshot_missing_on_resume_used_prefetched"
            if live_source_digest == "":
                live_source_digest = effective_source_digest
            try:
                _save_source_snapshot(
                    output_bucket=output_bucket,
                    key=snapshot_key,
                    source_xml=source_xml_bytes,
                    source_digest=(
                        live_source_digest
                        if live_source_digest != ""
                        else effective_source_digest
                    ),
                )
                source_snapshot_refreshed = True
            except Exception as snapshot_exc:
                source_snapshot_error = (
                    "snapshot_missing_on_resume_used_prefetched_and_save_failed:"
                    f"{type(snapshot_exc).__name__}"
                )
        else:
            source_snapshot_error = "snapshot_missing_on_resume"
            raise RuntimeError("Missing source snapshot while resuming sync cycle.")
    else:
        if source_xml_bytes is None:
            raise RuntimeError("Missing live source feed for new sync cycle start.")
        try:
            _save_source_snapshot(
                output_bucket=output_bucket,
                key=snapshot_key,
                source_xml=source_xml_bytes,
                source_digest=live_source_digest,
            )
            source_snapshot_refreshed = True
        except Exception as snapshot_exc:
            source_snapshot_error = f"snapshot_save_failed:{type(snapshot_exc).__name__}"

    if effective_source_xml is None:
        raise RuntimeError("Effective source XML is empty.")

    source_xml = effective_source_xml
    source_digest = effective_source_digest
    root = ET.fromstring(source_xml)
    records = _parse_records(root)
    ids = {r["id"] for r in records}
    base_records, variants_by_parent, base_stats = _build_relationships(
        records=records, include_orphans_as_products=include_orphans_as_products
    )
    parent_source_ids_with_variants: Set[str] = set(variants_by_parent.keys())

    source_to_bl_id = state.get("source_to_bl_id", {})
    hash_by_source_id = state.get("hash_by_source_id", {})
    shape_hash_by_source_id = state.get("shape_hash_by_source_id", {})
    price_qty_by_source_id = state.get("price_qty_by_source_id", {})
    parent_source_by_source_id = state.get("parent_source_by_source_id", {})
    raw_pre_audit_extra_bl_ids = state.get("sync_pre_audit_extra_bl_ids", [])
    raw_sync_target_source_ids = state.get("sync_target_source_ids", [])
    if not isinstance(source_to_bl_id, dict):
        source_to_bl_id = {}
    if not isinstance(hash_by_source_id, dict):
        hash_by_source_id = {}
    if not isinstance(shape_hash_by_source_id, dict):
        shape_hash_by_source_id = {}
    if not isinstance(price_qty_by_source_id, dict):
        price_qty_by_source_id = {}
    if not isinstance(parent_source_by_source_id, dict):
        parent_source_by_source_id = {}
    pre_audit_extra_bl_ids_state: List[int] = []
    if isinstance(raw_pre_audit_extra_bl_ids, list):
        for item in raw_pre_audit_extra_bl_ids:
            try:
                pid = int(item or 0)
            except Exception:
                pid = 0
            if pid > 0 and pid not in pre_audit_extra_bl_ids_state:
                pre_audit_extra_bl_ids_state.append(pid)
    sync_target_source_ids_state: List[str] = []
    if isinstance(raw_sync_target_source_ids, list):
        for item in raw_sync_target_source_ids:
            sid = _clean(item)
            if sid != "" and sid not in sync_target_source_ids_state:
                sync_target_source_ids_state.append(sid)
    shape_hash_by_source_id = {
        _clean(k): _clean(v)
        for k, v in shape_hash_by_source_id.items()
        if _clean(k) != "" and _clean(v) != ""
    }

    sanitized_price_qty: Dict[str, Dict[str, Any]] = {}
    for source_id_raw, row in price_qty_by_source_id.items():
        source_id = _clean(source_id_raw)
        if source_id == "" or not isinstance(row, dict):
            continue
        qty = _parse_int(str(row.get("quantity", 0)), 0)
        price = _normalize_decimal(str(row.get("price", "")), "0")
        sanitized_price_qty[source_id] = {"quantity": qty, "price": price}
    price_qty_by_source_id = sanitized_price_qty

    now_unix = int(time.time())
    remote_cache_raw = state.get("remote_cache", {})
    if not isinstance(remote_cache_raw, dict):
        remote_cache_raw = {}
    cache_ttl_sec = max(0, int(remote_cache_ttl_sec or 0))
    try:
        cache_inventory_id = int(remote_cache_raw.get("inventory_id", 0) or 0)
    except Exception:
        cache_inventory_id = 0
    try:
        cache_updated_unix = int(remote_cache_raw.get("updated_at_unix", 0) or 0)
    except Exception:
        cache_updated_unix = 0
    cache_fresh = (
        cache_ttl_sec > 0
        and cache_inventory_id == int(inventory_id)
        and cache_updated_unix > 0
        and (now_unix - cache_updated_unix) <= cache_ttl_sec
    )

    cache_used = 0
    cache_inventory_meta_used = 0
    cache_category_map_used = 0
    cache_manufacturer_map_used = 0
    cache_existing_sku_map_used = 0
    cache_inventory_meta_refreshed = 0
    cache_category_map_refreshed = 0
    cache_manufacturer_map_refreshed = 0
    cache_existing_sku_map_refreshed = 0

    default_price_group = 0
    default_warehouse = ""
    warehouse_ids = set()
    if cache_fresh:
        cache_used = 1
        default_price_group = _parse_int(str(remote_cache_raw.get("default_price_group", 0)), 0)
        default_warehouse = _clean(remote_cache_raw.get("default_warehouse", ""))
        raw_wh = remote_cache_raw.get("warehouse_ids", [])
        if isinstance(raw_wh, list):
            warehouse_ids = {_clean(w) for w in raw_wh if _clean(w) != ""}
        if default_price_group > 0 and default_warehouse != "":
            cache_inventory_meta_used = 1

    if default_price_group <= 0 or default_warehouse == "":
        inv = _bl_api_call(
            api_url=api_url,
            api_token=api_token,
            method="getInventories",
            parameters={},
            timeout_sec=timeout_sec,
        )
        inventories = inv.get("inventories", [])
        selected_inventory = None
        for row in inventories if isinstance(inventories, list) else []:
            if int(row.get("inventory_id", 0) or 0) == int(inventory_id):
                selected_inventory = row
                break
        if selected_inventory is None:
            raise RuntimeError(
                f"Inventory {inventory_id} not found in Base API getInventories."
            )
        default_price_group = int(selected_inventory.get("default_price_group", 0) or 0)
        default_warehouse = _clean(selected_inventory.get("default_warehouse", ""))
        inventory_warehouses = selected_inventory.get("warehouses", [])
        warehouse_ids = set()
        if isinstance(inventory_warehouses, list):
            warehouse_ids = {_clean(w) for w in inventory_warehouses if _clean(w) != ""}
        cache_inventory_meta_refreshed = 1

    env_warehouse = _clean(preferred_warehouse_id)
    if env_warehouse != "":
        if warehouse_ids and env_warehouse not in warehouse_ids:
            raise RuntimeError(
                f"BL_WAREHOUSE_ID='{env_warehouse}' is not assigned to inventory {inventory_id}."
            )
        default_warehouse = env_warehouse

    if default_price_group <= 0:
        raise RuntimeError("Inventory has no default price group configured.")
    if default_warehouse == "":
        raise RuntimeError("Inventory has no default warehouse configured.")

    raw_categories_by_parent = remote_cache_raw.get("categories_by_parent", {})
    raw_categories_path_cache = remote_cache_raw.get("categories_path_cache", {})
    if cache_fresh and isinstance(raw_categories_by_parent, dict) and isinstance(
        raw_categories_path_cache, dict
    ):
        by_parent, path_cache = _sanitize_category_maps_from_state(
            raw_by_parent=raw_categories_by_parent,
            raw_path_cache=raw_categories_path_cache,
        )
        cache_category_map_used = 1
    else:
        by_parent, path_cache = _ensure_category_maps(
            api_url=api_url,
            api_token=api_token,
            timeout_sec=timeout_sec,
            inventory_id=inventory_id,
        )
        cache_category_map_refreshed = 1

    raw_manufacturer_map = remote_cache_raw.get("manufacturer_map", {})
    if cache_fresh and isinstance(raw_manufacturer_map, dict):
        manufacturer_map = _sanitize_manufacturer_map_from_state(raw_manufacturer_map)
        cache_manufacturer_map_used = 1
    else:
        manufacturer_map = _fetch_manufacturer_map(
            api_url=api_url,
            api_token=api_token,
            timeout_sec=timeout_sec,
        )
        cache_manufacturer_map_refreshed = 1

    raw_sku_map = remote_cache_raw.get("existing_bl_ids_by_sku", {})
    if cache_fresh and isinstance(raw_sku_map, dict):
        existing_bl_ids_by_sku = _sanitize_sku_map_from_state(raw_sku_map)
        cache_existing_sku_map_used = 1
    else:
        existing_bl_ids_by_sku = _fetch_existing_product_ids_by_sku(
            api_url=api_url,
            api_token=api_token,
            timeout_sec=timeout_sec,
            inventory_id=inventory_id,
        )
        cache_existing_sku_map_refreshed = 1

    # Parents first, then variants.
    records_by_id = {r["id"]: r for r in records}

    def _depth_fast(rec: Dict[str, Any]) -> int:
        d = 0
        current_parent = rec.get("parent_id", "")
        seen = set()
        while current_parent and current_parent in ids and current_parent not in seen:
            d += 1
            seen.add(current_parent)
            parent_rec = records_by_id.get(current_parent)
            if parent_rec is None:
                break
            current_parent = parent_rec.get("parent_id", "")
        return d

    ordered_all = sorted(base_records, key=lambda r: (_depth_fast(r), r["id"]))
    ordered_all_by_id = {rec["id"]: rec for rec in ordered_all}
    desired_source_ids = {rec["id"] for rec in ordered_all}
    total_source_records = len(ordered_all)
    ordered: List[Dict[str, Any]] = list(ordered_all)
    total_records = len(ordered)

    raw_cursor_index = state.get("sync_cursor_index", 0)
    try:
        cursor_index = int(raw_cursor_index or 0)
    except Exception:
        cursor_index = 0
    if cursor_index < 0 or cursor_index >= total_source_records:
        cursor_index = 0

    state_source_digest = _clean(state.get("sync_source_digest", ""))
    if state_source_digest != "" and state_source_digest != source_digest:
        cursor_index = 0

    def _state_int(key: str, default: int = 0) -> int:
        try:
            return int(state.get(key, default) or 0)
        except Exception:
            return default

    global_cycle_id = _clean(state.get("sync_global_cycle_id", ""))
    global_started_unix = _state_int("sync_global_started_unix", 0)
    global_total_records = _state_int("sync_global_total_records", 0)
    global_processed = _state_int("sync_global_processed", 0)
    global_requested = _state_int("sync_global_requested", 0)
    global_updated = _state_int("sync_global_updated", 0)
    global_skipped_unchanged = _state_int("sync_global_skipped_unchanged", 0)
    global_skipped_missing_parent = _state_int("sync_global_skipped_missing_parent", 0)
    global_errors_count = _state_int("sync_global_errors_count", 0)
    global_delete_requested = _state_int("sync_global_delete_requested", 0)
    global_delete_deleted = _state_int("sync_global_delete_deleted", 0)
    global_delete_failed = _state_int("sync_global_delete_failed", 0)
    global_changed_target = _state_int("sync_global_changed_target", 0)
    global_delete_target = _state_int("sync_global_delete_target", 0)

    reset_global_counters = False
    if global_cycle_id == "":
        reset_global_counters = True
    if cursor_index == 0:
        reset_global_counters = True
    if state_source_digest != "" and state_source_digest != source_digest:
        reset_global_counters = True

    pre_audit_report: Dict[str, Any] = {}
    post_audit_report: Dict[str, Any] = {}
    pre_audit_diff_total = -1
    post_audit_skipped_no_changes = False
    sync_skipped_no_changes = False
    pre_audit_extra_bl_ids = list(pre_audit_extra_bl_ids_state)
    pre_audit_parent_relation_mismatch_source_ids: Set[str] = set()

    def _emit_stage_progress(
        sync_stage: str,
        phase: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if progress_callback is None:
            return
        payload: Dict[str, Any] = {
            "sync_stage": sync_stage,
            "phase": phase,
            "source_records_total": total_source_records,
            "source_live_digest_last_seen": live_source_digest,
            "source_snapshot_key": snapshot_key,
            "source_snapshot_digest": source_digest,
            "source_snapshot_reused": bool(source_snapshot_reused),
            "source_snapshot_refreshed": bool(source_snapshot_refreshed),
            "source_snapshot_error": source_snapshot_error,
            "global_cycle_id": global_cycle_id,
            "global_started_unix": global_started_unix,
            "global_total_records": global_total_records if global_total_records > 0 else total_source_records,
            "global_processed": global_processed,
            "global_requested": global_requested,
            "global_updated": global_updated,
            "global_skipped_unchanged": global_skipped_unchanged,
            "global_skipped_missing_parent": global_skipped_missing_parent,
            "global_errors_count": global_errors_count,
            "global_delete_requested": global_delete_requested,
            "global_delete_deleted": global_delete_deleted,
            "global_delete_failed": global_delete_failed,
            "global_changed_target": global_changed_target,
            "global_delete_target": global_delete_target,
        }
        if extra:
            payload.update(extra)
        try:
            progress_callback(payload)
        except Exception:
            return

    def _extract_pre_audit_source_ids_by_type(report: Dict[str, Any], diff_type: str) -> Set[str]:
        out: Set[str] = set()
        if not isinstance(report, dict):
            return out
        summary = report.get("summary", {})
        if not isinstance(summary, dict):
            return out
        diff_samples = summary.get("diff_samples", {})
        if not isinstance(diff_samples, dict):
            return out
        section = diff_samples.get(diff_type, {})
        if not isinstance(section, dict):
            return out
        rows = section.get("sample", [])
        if not isinstance(rows, list):
            return out
        for row in rows:
            if not isinstance(row, dict):
                continue
            source_id = _clean(row.get("source_id", ""))
            if source_id != "":
                out.add(source_id)
        return out

    # Hard reset per-cycle sync maps so pre-sync audit can rebuild state from scratch.
    # This avoids carrying stale signatures/mappings from a previous cycle.
    if reset_global_counters:
        source_to_bl_id = {}
        hash_by_source_id = {}
        shape_hash_by_source_id = {}
        price_qty_by_source_id = {}
        parent_source_by_source_id = {}
        pre_audit_extra_bl_ids = []

    if bool(full_audit_enabled) and reset_global_counters and total_source_records > 0:
        _emit_stage_progress(
            "pre_audit",
            "pre_audit_started",
            {
                "pre_audit_executed": False,
                "global_total_records": total_source_records,
                "global_processed": 0,
            },
        )
        try:
            pre_audit_report = _run_full_consistency_audit(
                phase="pre",
                output_bucket=output_bucket,
                output_key=output_key,
                inventory_id=inventory_id,
                api_url=api_url,
                api_token=api_token,
                timeout_sec=timeout_sec,
                default_price_group=default_price_group,
                default_warehouse=default_warehouse,
                records_for_sync=ordered_all,
                source_to_bl_id=source_to_bl_id,
                by_parent=by_parent,
                path_cache=path_cache,
                manufacturer_map=manufacturer_map,
                parent_source_ids_with_variants=parent_source_ids_with_variants,
                details_limit_per_type=full_audit_details_limit_per_type,
                max_details_rows=full_audit_max_details_rows,
            )
            matched_source_to_bl_id = pre_audit_report.get("matched_source_to_bl_id", {})
            if isinstance(matched_source_to_bl_id, dict):
                for sid, bl_id_raw in matched_source_to_bl_id.items():
                    source_id = _clean(sid)
                    bl_id = int(bl_id_raw or 0)
                    if source_id and bl_id > 0:
                        source_to_bl_id[source_id] = bl_id

            ordered_by_id = {rec["id"]: rec for rec in ordered_all}
            unchanged_source_ids = pre_audit_report.get("unchanged_source_ids", [])
            changed_source_ids = pre_audit_report.get("changed_source_ids", [])
            missing_source_ids = pre_audit_report.get("missing_source_ids", [])
            if not isinstance(unchanged_source_ids, list):
                unchanged_source_ids = []
            if not isinstance(changed_source_ids, list):
                changed_source_ids = []
            if not isinstance(missing_source_ids, list):
                missing_source_ids = []

            for sid_raw in unchanged_source_ids:
                sid = _clean(sid_raw)
                rec = ordered_by_id.get(sid)
                if rec is None:
                    continue
                parent_sid = _normalize_parent_id(rec.get("parent_id", ""))
                if parent_sid and parent_sid not in ids:
                    parent_sid = ""
                sig = _stable_signature(rec, parent_sid)
                shape_sig = _stable_shape_signature(rec, parent_sid)
                hash_by_source_id[sid] = sig
                shape_hash_by_source_id[sid] = shape_sig
                parent_source_by_source_id[sid] = parent_sid
                price_qty_by_source_id[sid] = {
                    "quantity": int(rec.get("quantity", 0) or 0),
                    "price": _normalize_decimal(rec.get("price", ""), "0"),
                }

            for sid_raw in changed_source_ids:
                sid = _clean(sid_raw)
                if sid == "":
                    continue
                hash_by_source_id.pop(sid, None)
                shape_hash_by_source_id.pop(sid, None)
                price_qty_by_source_id.pop(sid, None)

            for sid_raw in missing_source_ids:
                sid = _clean(sid_raw)
                if sid == "":
                    continue
                hash_by_source_id.pop(sid, None)
                shape_hash_by_source_id.pop(sid, None)
                price_qty_by_source_id.pop(sid, None)

            raw_extra_bl_ids = pre_audit_report.get("extra_bl_ids", [])
            next_extra_ids: List[int] = []
            if isinstance(raw_extra_bl_ids, list):
                for item in raw_extra_bl_ids:
                    try:
                        bl_id = int(item or 0)
                    except Exception:
                        bl_id = 0
                    if bl_id > 0 and bl_id not in next_extra_ids:
                        next_extra_ids.append(bl_id)
            pre_audit_extra_bl_ids = next_extra_ids
            pre_audit_parent_relation_mismatch_source_ids = _extract_pre_audit_source_ids_by_type(
                pre_audit_report,
                "parent_relation_mismatch",
            )
            pre_summary = pre_audit_report.get("summary", {})
            if isinstance(pre_summary, dict):
                pre_audit_diff_total = int(pre_summary.get("diff_total", 0) or 0)
        except Exception as audit_exc:
            pre_audit_report = {
                "phase": "pre",
                "error": f"{type(audit_exc).__name__}: {audit_exc}",
            }
        pre_summary_for_progress = pre_audit_report.get("summary", {})
        _emit_stage_progress(
            "pre_audit",
            "pre_audit_finished",
            {
                "pre_audit_executed": True,
                "pre_audit_summary": pre_summary_for_progress
                if isinstance(pre_summary_for_progress, dict)
                else {},
                "pre_audit_summary_key": _clean(pre_audit_report.get("summary_key", "")),
                "pre_audit_error": _clean(pre_audit_report.get("error", "")),
            },
        )

    if (
        bool(full_audit_enabled)
        and bool(reset_global_counters)
        and int(pre_audit_diff_total) == 0
    ):
        sync_skipped_no_changes = True

    sync_target_source_ids: List[str] = []

    if reset_global_counters:
        target_ids_set: Set[str] = set()
        raw_changed_ids = pre_audit_report.get("changed_source_ids", []) if isinstance(pre_audit_report, dict) else []
        raw_missing_ids = pre_audit_report.get("missing_source_ids", []) if isinstance(pre_audit_report, dict) else []
        if isinstance(raw_changed_ids, list):
            for item in raw_changed_ids:
                sid = _clean(item)
                if sid != "":
                    target_ids_set.add(sid)
        if isinstance(raw_missing_ids, list):
            for item in raw_missing_ids:
                sid = _clean(item)
                if sid != "":
                    target_ids_set.add(sid)
        if sync_skipped_no_changes:
            sync_target_source_ids = []
        elif target_ids_set:
            for rec in ordered_all:
                sid = rec["id"]
                if sid in target_ids_set:
                    sync_target_source_ids.append(sid)
        else:
            for rec in ordered_all:
                sid = rec["id"]
                parent_sid = _normalize_parent_id(rec.get("parent_id", ""))
                if parent_sid and parent_sid not in ids:
                    parent_sid = ""
                sig = _stable_signature(rec, parent_sid)
                if hash_by_source_id.get(sid) != sig:
                    sync_target_source_ids.append(sid)
    else:
        if sync_target_source_ids_state:
            for sid in sync_target_source_ids_state:
                if sid in ordered_all_by_id and sid not in sync_target_source_ids:
                    sync_target_source_ids.append(sid)
        if not sync_target_source_ids:
            for rec in ordered_all:
                sid = rec["id"]
                parent_sid = _normalize_parent_id(rec.get("parent_id", ""))
                if parent_sid and parent_sid not in ids:
                    parent_sid = ""
                sig = _stable_signature(rec, parent_sid)
                if hash_by_source_id.get(sid) != sig:
                    sync_target_source_ids.append(sid)

    ordered = [ordered_all_by_id[sid] for sid in sync_target_source_ids if sid in ordered_all_by_id]
    total_records = len(ordered)
    if cursor_index < 0 or cursor_index >= total_records:
        cursor_index = 0
    if state_source_digest != "" and state_source_digest != source_digest:
        cursor_index = 0

    if reset_global_counters:
        global_cycle_id = f"{source_digest[:12]}-{now_unix}"
        global_started_unix = now_unix
        global_total_records = total_records
        global_processed = 0
        global_requested = 0
        global_updated = 0
        global_skipped_unchanged = 0
        global_skipped_missing_parent = 0
        global_errors_count = 0
        global_delete_requested = 0
        global_delete_deleted = 0
        global_delete_failed = 0
        # Exact diff target for this cycle based on stored hashes.
        changed_target = 0
        for rec in ordered:
            sid = rec["id"]
            parent_sid = _normalize_parent_id(rec.get("parent_id", ""))
            if parent_sid and parent_sid not in ids:
                parent_sid = ""
            sig = _stable_signature(rec, parent_sid)
            if hash_by_source_id.get(sid) != sig:
                changed_target += 1
        global_changed_target = changed_target
        delete_target_bl_ids: Set[int] = set()
        for sid in list(source_to_bl_id.keys()):
            if sid in desired_source_ids:
                continue
            try:
                stale_bl_id = int(source_to_bl_id.get(sid, 0) or 0)
            except Exception:
                stale_bl_id = 0
            if stale_bl_id > 0:
                delete_target_bl_ids.add(stale_bl_id)
        for extra_bl_id in pre_audit_extra_bl_ids:
            if int(extra_bl_id or 0) > 0:
                delete_target_bl_ids.add(int(extra_bl_id))
        global_delete_target = len(delete_target_bl_ids)
    elif global_total_records <= 0:
        global_total_records = total_records

    pre_summary_after_diff = (
        pre_audit_report.get("summary", {}) if isinstance(pre_audit_report, dict) else {}
    )
    if sync_skipped_no_changes:
        _emit_stage_progress(
            "sync_skipped",
            "sync_skipped_no_changes",
            {
                "pre_audit_executed": bool(pre_audit_report),
                "sync_skipped_no_changes": True,
                "pre_audit_summary": pre_summary_after_diff
                if isinstance(pre_summary_after_diff, dict)
                else {},
                "pre_audit_summary_key": _clean(pre_audit_report.get("summary_key", ""))
                if isinstance(pre_audit_report, dict)
                else "",
                "global_total_records": 0,
                "global_processed": 0,
            },
        )
    else:
        _emit_stage_progress(
            "sync",
            "sync_started",
            {
                "pre_audit_executed": bool(pre_audit_report),
                "sync_skipped_no_changes": False,
                "pre_audit_summary": pre_summary_after_diff
                if isinstance(pre_summary_after_diff, dict)
                else {},
                "pre_audit_summary_key": _clean(pre_audit_report.get("summary_key", ""))
                if isinstance(pre_audit_report, dict)
                else "",
                "global_total_records": global_total_records,
                "global_processed": global_processed,
            },
        )

    # Backward-compatible bootstrap for cycles that started before ETA fields existed.
    if global_changed_target <= 0:
        remaining_changed_now = 0
        for rec in ordered:
            sid = rec["id"]
            parent_sid = _normalize_parent_id(rec.get("parent_id", ""))
            if parent_sid and parent_sid not in ids:
                parent_sid = ""
            sig = _stable_signature(rec, parent_sid)
            if hash_by_source_id.get(sid) != sig:
                remaining_changed_now += 1
        global_changed_target = int(global_requested) + int(remaining_changed_now)

    if global_delete_target <= 0:
        remaining_delete_bl_ids: Set[int] = set()
        for sid in list(source_to_bl_id.keys()):
            if sid in desired_source_ids:
                continue
            try:
                stale_bl_id = int(source_to_bl_id.get(sid, 0) or 0)
            except Exception:
                stale_bl_id = 0
            if stale_bl_id > 0:
                remaining_delete_bl_ids.add(stale_bl_id)
        for extra_bl_id in pre_audit_extra_bl_ids:
            if int(extra_bl_id or 0) > 0:
                remaining_delete_bl_ids.add(int(extra_bl_id))
        remaining_delete_now = len(remaining_delete_bl_ids)
        global_delete_target = int(global_delete_deleted) + int(remaining_delete_now)

    effective_rpm = max(1, int(api_rate_limit_rpm or 1))
    eta_ma_min_rpm_safe = max(1, int(eta_ma_min_rpm or 1))
    eta_ma_bootstrap_sec_safe = max(0, int(eta_ma_bootstrap_sec or 0))
    eta_ma_alpha_safe = float(eta_ma_alpha)
    if eta_ma_alpha_safe < 0.01:
        eta_ma_alpha_safe = 0.01
    if eta_ma_alpha_safe > 0.90:
        eta_ma_alpha_safe = 0.90
    eta_moving_avg_enabled_safe = bool(eta_moving_avg_enabled)
    try:
        eta_ma_rpm_prev = float(state.get("sync_eta_ma_rpm", 0.0) or 0.0)
    except Exception:
        eta_ma_rpm_prev = 0.0
    if eta_ma_rpm_prev <= 0:
        eta_ma_rpm_prev = float(effective_rpm)
    eta_ma_rpm_prev = max(float(eta_ma_min_rpm_safe), min(float(effective_rpm), eta_ma_rpm_prev))
    eta_ma_samples_prev = _state_int("sync_eta_ma_samples", 0)
    invocation_started_unix_safe = int(invocation_started_unix or 0)

    def _mutations_per_min_for_invocation(
        requested_now: int,
        delete_deleted_now: int,
    ) -> float:
        if invocation_started_unix_safe <= 0:
            return 0.0
        elapsed_sec = int(time.time()) - invocation_started_unix_safe
        if elapsed_sec <= 0:
            return 0.0
        mutations_now = max(0, int(requested_now) + int(delete_deleted_now))
        if mutations_now <= 0:
            return 0.0
        return float(mutations_now) * 60.0 / float(elapsed_sec)

    def _build_eta_fields(
        requested_now: int,
        delete_deleted_now: int,
    ) -> Dict[str, Any]:
        cycle_total_mutations = max(
            0, int(global_changed_target) + int(global_delete_target)
        )
        completed_mutations = max(
            0, int(global_requested + requested_now) + int(global_delete_deleted + delete_deleted_now)
        )
        remaining_mutations = max(0, cycle_total_mutations - completed_mutations)
        eta_instant_rpm = _mutations_per_min_for_invocation(
            requested_now=requested_now,
            delete_deleted_now=delete_deleted_now,
        )
        eta_rpm_used = float(effective_rpm)
        eta_rpm_mode = "static_limit"
        if eta_moving_avg_enabled_safe:
            eta_rpm_mode = "moving_average"
            eta_rpm_used = eta_ma_rpm_prev
            if (
                eta_instant_rpm > 0
                and invocation_started_unix_safe > 0
                and (int(time.time()) - invocation_started_unix_safe) >= eta_ma_bootstrap_sec_safe
            ):
                eta_rpm_used = (
                    (1.0 - eta_ma_alpha_safe) * eta_ma_rpm_prev
                    + eta_ma_alpha_safe * eta_instant_rpm
                )
            eta_rpm_used = max(
                float(eta_ma_min_rpm_safe),
                min(float(effective_rpm), float(eta_rpm_used)),
            )
        eta_seconds = int(math.ceil((float(remaining_mutations) * 60.0) / max(1.0, eta_rpm_used)))
        eta_finish_unix = int(time.time()) + int(eta_seconds)
        return {
            "eta_req_limit_rpm": effective_rpm,
            "eta_rpm_mode": eta_rpm_mode,
            "eta_rpm_used": round(float(eta_rpm_used), 2),
            "eta_rpm_ma_prev": round(float(eta_ma_rpm_prev), 2),
            "eta_rpm_instant": round(float(eta_instant_rpm), 2),
            "eta_ma_alpha": round(float(eta_ma_alpha_safe), 4),
            "eta_cycle_changed_target": int(global_changed_target),
            "eta_cycle_delete_target": int(global_delete_target),
            "eta_cycle_total_mutations": int(cycle_total_mutations),
            "eta_completed_mutations": int(completed_mutations),
            "eta_remaining_mutations": int(remaining_mutations),
            "eta_seconds_remaining": int(eta_seconds),
            "eta_finish_unix": int(eta_finish_unix),
            "eta_finish_iso": datetime.fromtimestamp(
                eta_finish_unix, tz=POLAND_TZ
            ).isoformat(),
        }

    if max_records_per_run > 0:
        batch_end_candidate = min(total_records, cursor_index + max_records_per_run)
    else:
        batch_end_candidate = total_records
    batch_records = ordered[cursor_index:batch_end_candidate]

    requested = 0
    updated = 0
    skipped_unchanged = 0
    skipped_no_parent = 0
    matched_existing_by_sku = 0
    matched_existing_by_sku_ambiguous = 0
    created_categories = 0
    created_manufacturers = 0
    bulk_queue_records = 0
    bulk_stock_calls = 0
    bulk_price_calls = 0
    bulk_stock_records = 0
    bulk_price_records = 0
    bulk_skipped_by_threshold = 0
    delete_requested = 0
    delete_deleted = 0
    delete_failed = 0
    processed_records = 0
    upsert_limit_reached = False
    soft_timeout_reached = False
    token_temporarily_blocked = False
    blocked_token_error_message = ""
    blocked_token_until_unix = 0
    blocked_token_until_iso = ""
    delete_phase_executed = False
    delete_phase_deferred = False
    errors: List[str] = []
    bulk_queue: List[Dict[str, Any]] = []
    queue_budget_used = 0

    def _mark_token_temporarily_blocked(exc: Exception) -> None:
        nonlocal token_temporarily_blocked
        nonlocal blocked_token_error_message
        nonlocal blocked_token_until_unix
        nonlocal blocked_token_until_iso
        token_temporarily_blocked = True
        raw_err = _clean(str(exc))
        if raw_err != "":
            blocked_token_error_message = raw_err[:600]
        parsed_unix = _extract_blocked_token_until_unix(raw_err)
        if parsed_unix > blocked_token_until_unix:
            blocked_token_until_unix = int(parsed_unix)
            blocked_token_until_iso = datetime.fromtimestamp(
                blocked_token_until_unix, tz=POLAND_TZ
            ).isoformat()

    def _drop_state_for_source(source_id: str) -> None:
        source_to_bl_id.pop(source_id, None)
        hash_by_source_id.pop(source_id, None)
        shape_hash_by_source_id.pop(source_id, None)
        price_qty_by_source_id.pop(source_id, None)
        parent_source_by_source_id.pop(source_id, None)

    safe_progress_every = max(1, int(progress_update_every or 1))

    def _emit_progress(extra: Optional[Dict[str, Any]] = None) -> None:
        if progress_callback is None:
            return
        payload: Dict[str, Any] = {
            "sync_stage": "sync",
            "source_records_total": total_records,
            "source_live_digest_last_seen": live_source_digest,
            "source_snapshot_key": snapshot_key,
            "source_snapshot_digest": source_digest,
            "source_snapshot_reused": bool(source_snapshot_reused),
            "source_snapshot_refreshed": bool(source_snapshot_refreshed),
            "source_snapshot_error": source_snapshot_error,
            "batch_start_index": cursor_index,
            "batch_target_end_index": batch_end_candidate,
            "batch_records_target": len(batch_records),
            "batch_records_scanned": processed_records,
            "sync_requested": requested,
            "sync_updated": updated,
            "sync_skipped_unchanged": skipped_unchanged,
            "sync_skipped_missing_parent": skipped_no_parent,
            "upsert_limit_reached": upsert_limit_reached,
            "soft_timeout_reached": soft_timeout_reached,
            "global_cycle_id": global_cycle_id,
            "global_started_unix": global_started_unix,
            "global_total_records": global_total_records,
            "global_processed": global_processed + processed_records,
            "global_requested": global_requested + requested,
            "global_updated": global_updated + updated,
            "global_skipped_unchanged": global_skipped_unchanged + skipped_unchanged,
            "global_skipped_missing_parent": (
                global_skipped_missing_parent + skipped_no_parent
            ),
            "global_errors_count": global_errors_count + len(errors),
            "global_delete_requested": global_delete_requested + delete_requested,
            "global_delete_deleted": global_delete_deleted + delete_deleted,
            "global_delete_failed": global_delete_failed + delete_failed,
        }
        payload.update(
            _build_eta_fields(
                requested_now=requested,
                delete_deleted_now=delete_deleted,
            )
        )
        if extra:
            payload.update(extra)
        try:
            progress_callback(payload)
        except Exception:
            # Progress reporting must never break the sync flow.
            return

    _emit_progress({"phase": "batch_started"})

    for rec in batch_records:
        if remaining_ms_fn is not None:
            try:
                remaining_ms = int(remaining_ms_fn() or 0)
            except Exception:
                remaining_ms = 0
            if remaining_ms > 0 and remaining_ms <= max(10_000, int(min_remaining_ms_for_continue)):
                soft_timeout_reached = True
                break

        if max_upserts_per_run > 0 and (requested + queue_budget_used) >= max_upserts_per_run:
            upsert_limit_reached = True
            break
        processed_records += 1

        source_id = rec["id"]
        parent_source_id = _normalize_parent_id(rec.get("parent_id", ""))
        is_parent_with_variants = bool(
            parent_source_id == "" and source_id in parent_source_ids_with_variants
        )
        if parent_source_id and parent_source_id not in ids:
            parent_source_id = ""

        if parent_source_id:
            parent_bl_id = int(source_to_bl_id.get(parent_source_id, 0) or 0)
            if parent_bl_id <= 0:
                skipped_no_parent += 1
                continue
        else:
            parent_bl_id = 0

        signature = _stable_signature(rec, parent_source_id)
        shape_signature = _stable_shape_signature(rec, parent_source_id)
        if hash_by_source_id.get(source_id) == signature:
            parent_source_by_source_id[source_id] = parent_source_id
            shape_hash_by_source_id[source_id] = shape_signature
            skipped_unchanged += 1
            continue

        try:
            existing_bl_id_from_state = int(source_to_bl_id.get(source_id, 0) or 0)
            existing_bl_id = existing_bl_id_from_state
            if existing_bl_id <= 0:
                sku_key = _clean(rec.get("sku", "")).lower()
                if sku_key:
                    candidates = existing_bl_ids_by_sku.get(sku_key, [])
                    if len(candidates) == 1:
                        existing_bl_id = int(candidates[0])
                        source_to_bl_id[source_id] = existing_bl_id
                        matched_existing_by_sku += 1
                    elif len(candidates) > 1:
                        matched_existing_by_sku_ambiguous += 1

            previous_shape = _clean(shape_hash_by_source_id.get(source_id, ""))
            previous_price_qty = price_qty_by_source_id.get(source_id, {})
            old_qty = _parse_int(str(previous_price_qty.get("quantity", 0)), 0)
            old_price = _normalize_decimal(str(previous_price_qty.get("price", "")), "0")
            new_qty = int(rec.get("quantity", 0) or 0)
            new_price_norm = _normalize_decimal(rec.get("price", ""), "0")
            qty_changed = old_qty != new_qty
            if is_parent_with_variants:
                # BL derives parent stock from variants, do not try to enforce it directly.
                qty_changed = False
            price_changed = old_price != new_price_norm
            bulk_path_eligible = (
                bool(bulk_update_enabled)
                and int(bulk_update_max_items or 0) > 0
                and existing_bl_id_from_state > 0
                and existing_bl_id > 0
                and previous_shape != ""
                and previous_shape == shape_signature
                and (qty_changed or price_changed)
            )
            if bulk_path_eligible:
                bulk_queue.append(
                    {
                        "source_id": source_id,
                        "parent_source_id": parent_source_id,
                        "bl_id": existing_bl_id,
                        "sku_key": _clean(rec.get("sku", "")).lower(),
                        "signature": signature,
                        "shape_signature": shape_signature,
                        "qty": new_qty,
                        "price": _to_float(rec.get("price", ""), default=0.0),
                        "price_norm": new_price_norm,
                        "qty_changed": qty_changed,
                        "price_changed": price_changed,
                        "include_stock": not is_parent_with_variants,
                    }
                )
                queue_budget_used += 1
                bulk_queue_records += 1
                continue

            category_id = 0
            if _clean(rec.get("category_name", "")):
                before = len(path_cache)
                category_id = _ensure_category_id(
                    category_name=rec.get("category_name", ""),
                    by_parent=by_parent,
                    path_cache=path_cache,
                    api_url=api_url,
                    api_token=api_token,
                    timeout_sec=timeout_sec,
                    inventory_id=inventory_id,
                )
                if len(path_cache) > before:
                    created_categories += len(path_cache) - before

            manufacturer_id = 0
            if _clean(rec.get("manufacturer_name", "")):
                before_m = len(manufacturer_map)
                manufacturer_id = _ensure_manufacturer_id(
                    manufacturer_name=rec.get("manufacturer_name", ""),
                    manufacturer_map=manufacturer_map,
                    api_url=api_url,
                    api_token=api_token,
                    timeout_sec=timeout_sec,
                )
                if len(manufacturer_map) > before_m:
                    created_manufacturers += 1

            payload = _build_product_payload(
                record=rec,
                inventory_id=inventory_id,
                default_price_group=default_price_group,
                default_warehouse=default_warehouse,
                parent_bl_id=parent_bl_id,
                category_id=category_id,
                manufacturer_id=manufacturer_id,
                existing_bl_id=existing_bl_id,
                include_stock=(not is_parent_with_variants),
            )
            try:
                out = _bl_api_call(
                    api_url=api_url,
                    api_token=api_token,
                    method="addInventoryProduct",
                    parameters=payload,
                    timeout_sec=timeout_sec,
                )
            except Exception as first_exc:
                msg = str(first_exc).lower()
                if "error_blocked_token" in msg or "token blocked until" in msg:
                    _mark_token_temporarily_blocked(first_exc)
                    raise
                # Stored BL product id became stale (e.g. item removed manually in BL).
                if existing_bl_id > 0 and (
                    "error_product_id" in msg or "no product with id" in msg
                ):
                    source_to_bl_id.pop(source_id, None)
                    payload = _build_product_payload(
                        record=rec,
                        inventory_id=inventory_id,
                        default_price_group=default_price_group,
                        default_warehouse=default_warehouse,
                        parent_bl_id=parent_bl_id,
                        category_id=category_id,
                        manufacturer_id=manufacturer_id,
                        existing_bl_id=0,
                        include_stock=(not is_parent_with_variants),
                    )
                    out = _bl_api_call(
                        api_url=api_url,
                        api_token=api_token,
                        method="addInventoryProduct",
                        parameters=payload,
                        timeout_sec=timeout_sec,
                    )
                else:
                    raise
            bl_id = int(out.get("product_id", 0) or 0)
            if bl_id <= 0:
                raise RuntimeError(f"addInventoryProduct did not return product_id for {source_id}")

            needs_parent_relation_repair = bool(
                parent_bl_id > 0 and source_id in pre_audit_parent_relation_mismatch_source_ids
            )
            if needs_parent_relation_repair:
                actual_parent_bl_id = _fetch_bl_parent_id(
                    api_url=api_url,
                    api_token=api_token,
                    timeout_sec=timeout_sec,
                    inventory_id=inventory_id,
                    product_id=bl_id,
                )
                if int(actual_parent_bl_id) != int(parent_bl_id):
                    old_bl_id = int(bl_id)
                    try:
                        _bl_api_call(
                            api_url=api_url,
                            api_token=api_token,
                            method="deleteInventoryProduct",
                            parameters={"product_id": old_bl_id},
                            timeout_sec=timeout_sec,
                        )
                    except Exception as delete_exc:
                        delete_msg = str(delete_exc).lower()
                        if "not exist" not in delete_msg and "not found" not in delete_msg:
                            raise RuntimeError(
                                f"parent relation fix delete failed for {source_id}: {delete_exc}"
                            )

                    recreate_payload = _build_product_payload(
                        record=rec,
                        inventory_id=inventory_id,
                        default_price_group=default_price_group,
                        default_warehouse=default_warehouse,
                        parent_bl_id=parent_bl_id,
                        category_id=category_id,
                        manufacturer_id=manufacturer_id,
                        existing_bl_id=0,
                        include_stock=(not is_parent_with_variants),
                    )
                    recreate_out = _bl_api_call(
                        api_url=api_url,
                        api_token=api_token,
                        method="addInventoryProduct",
                        parameters=recreate_payload,
                        timeout_sec=timeout_sec,
                    )
                    recreated_bl_id = int(recreate_out.get("product_id", 0) or 0)
                    if recreated_bl_id <= 0:
                        raise RuntimeError(
                            f"parent relation fix recreate failed for {source_id}"
                        )
                    bl_id = recreated_bl_id
                    sku_key = _clean(rec.get("sku", "")).lower()
                    if sku_key:
                        candidates = existing_bl_ids_by_sku.get(sku_key, [])
                        existing_bl_ids_by_sku[sku_key] = [
                            int(pid) for pid in candidates if int(pid or 0) != int(old_bl_id)
                        ]

                    final_parent_bl_id = _fetch_bl_parent_id(
                        api_url=api_url,
                        api_token=api_token,
                        timeout_sec=timeout_sec,
                        inventory_id=inventory_id,
                        product_id=bl_id,
                    )
                    if int(final_parent_bl_id) != int(parent_bl_id):
                        raise RuntimeError(
                            f"parent relation still mismatched after recreate for {source_id}:"
                            f" expected={parent_bl_id} actual={final_parent_bl_id}"
                        )

            source_to_bl_id[source_id] = bl_id
            hash_by_source_id[source_id] = signature
            shape_hash_by_source_id[source_id] = shape_signature
            price_qty_by_source_id[source_id] = {
                "quantity": int(rec.get("quantity", 0) or 0),
                "price": _normalize_decimal(rec.get("price", ""), "0"),
            }
            parent_source_by_source_id[source_id] = parent_source_id
            sku_key = _clean(rec.get("sku", "")).lower()
            if sku_key:
                candidates = existing_bl_ids_by_sku.setdefault(sku_key, [])
                if bl_id not in candidates:
                    candidates.append(bl_id)
            requested += 1
            updated += 1
        except Exception as exc:
            errors.append(f"{source_id}: {exc}")
            if token_temporarily_blocked:
                break
            if len(errors) >= 50:
                break

        if processed_records % safe_progress_every == 0:
            _emit_progress({"phase": "batch_in_progress"})

    if bulk_queue and (bulk_update_enabled and int(bulk_update_max_items or 0) > 0):
        if len(bulk_queue) < max(1, int(bulk_update_min_items or 1)):
            bulk_skipped_by_threshold = len(bulk_queue)
            for item in bulk_queue:
                if max_upserts_per_run > 0 and requested >= max_upserts_per_run:
                    upsert_limit_reached = True
                    break
                source_id = item["source_id"]
                rec = records_by_id.get(source_id)
                if rec is None:
                    continue
                try:
                    parent_source_id = _normalize_parent_id(item.get("parent_source_id", ""))
                    parent_bl_id = 0
                    if parent_source_id:
                        parent_bl_id = int(source_to_bl_id.get(parent_source_id, 0) or 0)
                        if parent_bl_id <= 0:
                            skipped_no_parent += 1
                            continue

                    category_id = 0
                    if _clean(rec.get("category_name", "")):
                        before = len(path_cache)
                        category_id = _ensure_category_id(
                            category_name=rec.get("category_name", ""),
                            by_parent=by_parent,
                            path_cache=path_cache,
                            api_url=api_url,
                            api_token=api_token,
                            timeout_sec=timeout_sec,
                            inventory_id=inventory_id,
                        )
                        if len(path_cache) > before:
                            created_categories += len(path_cache) - before

                    manufacturer_id = 0
                    if _clean(rec.get("manufacturer_name", "")):
                        before_m = len(manufacturer_map)
                        manufacturer_id = _ensure_manufacturer_id(
                            manufacturer_name=rec.get("manufacturer_name", ""),
                            manufacturer_map=manufacturer_map,
                            api_url=api_url,
                            api_token=api_token,
                            timeout_sec=timeout_sec,
                        )
                        if len(manufacturer_map) > before_m:
                            created_manufacturers += 1

                    payload = _build_product_payload(
                        record=rec,
                        inventory_id=inventory_id,
                        default_price_group=default_price_group,
                        default_warehouse=default_warehouse,
                        parent_bl_id=parent_bl_id,
                        category_id=category_id,
                        manufacturer_id=manufacturer_id,
                        existing_bl_id=int(item["bl_id"]),
                        include_stock=bool(item.get("include_stock", True)),
                    )
                    out = _bl_api_call(
                        api_url=api_url,
                        api_token=api_token,
                        method="addInventoryProduct",
                        parameters=payload,
                        timeout_sec=timeout_sec,
                    )
                    bl_id = int(out.get("product_id", 0) or 0)
                    if bl_id <= 0:
                        raise RuntimeError(
                            f"addInventoryProduct did not return product_id for {source_id}"
                        )
                    source_to_bl_id[source_id] = bl_id
                    hash_by_source_id[source_id] = item["signature"]
                    shape_hash_by_source_id[source_id] = item["shape_signature"]
                    parent_source_by_source_id[source_id] = parent_source_id
                    price_qty_by_source_id[source_id] = {
                        "quantity": int(item["qty"]),
                        "price": item["price_norm"],
                    }
                    sku_key = _clean(item.get("sku_key", ""))
                    if sku_key:
                        sku_candidates = existing_bl_ids_by_sku.setdefault(sku_key, [])
                        if bl_id not in sku_candidates:
                            sku_candidates.append(bl_id)
                    requested += 1
                    updated += 1
                except Exception as exc:
                    msg = str(exc).lower()
                    if "error_blocked_token" in msg or "token blocked until" in msg:
                        _mark_token_temporarily_blocked(exc)
                    errors.append(f"{source_id}: bulk-threshold fallback failed: {exc}")
                    if token_temporarily_blocked or len(errors) >= 50:
                        break
        else:
            chunk_size = max(1, min(1000, int(bulk_update_max_items or 1000)))

            def _warn_ids(raw_warning: Any) -> set:
                out = set()
                if isinstance(raw_warning, dict):
                    keys = raw_warning.keys()
                else:
                    keys = []
                for key in keys:
                    try:
                        out.add(int(key))
                    except Exception:
                        continue
                return out

            for idx in range(0, len(bulk_queue), chunk_size):
                chunk = bulk_queue[idx : idx + chunk_size]
                failed_ids: set = set()

                stock_payload_products: Dict[str, Dict[str, int]] = {}
                for item in chunk:
                    if not item.get("qty_changed", False):
                        continue
                    stock_payload_products[str(item["bl_id"])] = {
                        default_warehouse: int(item["qty"])
                    }
                if stock_payload_products:
                    try:
                        out = _bl_api_call(
                            api_url=api_url,
                            api_token=api_token,
                            method="updateInventoryProductsStock",
                            parameters={
                                "inventory_id": inventory_id,
                                "products": stock_payload_products,
                            },
                            timeout_sec=timeout_sec,
                        )
                        bulk_stock_calls += 1
                        bulk_stock_records += len(stock_payload_products)
                        failed_ids |= _warn_ids(out.get("warnings"))
                    except Exception as exc:
                        msg = str(exc).lower()
                        if "error_blocked_token" in msg or "token blocked until" in msg:
                            _mark_token_temporarily_blocked(exc)
                        for item in chunk:
                            failed_ids.add(int(item["bl_id"]))
                        errors.append(f"bulk stock chunk starting at {idx}: {exc}")
                        if token_temporarily_blocked:
                            break

                price_payload_products: Dict[str, Dict[str, float]] = {}
                for item in chunk:
                    if int(item["bl_id"]) in failed_ids:
                        continue
                    if not item.get("price_changed", False):
                        continue
                    price_payload_products[str(item["bl_id"])] = {
                        str(default_price_group): float(item["price"])
                    }
                if price_payload_products:
                    try:
                        out = _bl_api_call(
                            api_url=api_url,
                            api_token=api_token,
                            method="updateInventoryProductsPrices",
                            parameters={
                                "inventory_id": inventory_id,
                                "products": price_payload_products,
                            },
                            timeout_sec=timeout_sec,
                        )
                        bulk_price_calls += 1
                        bulk_price_records += len(price_payload_products)
                        failed_ids |= _warn_ids(out.get("warnings"))
                    except Exception as exc:
                        msg = str(exc).lower()
                        if "error_blocked_token" in msg or "token blocked until" in msg:
                            _mark_token_temporarily_blocked(exc)
                        for item in chunk:
                            failed_ids.add(int(item["bl_id"]))
                        errors.append(f"bulk price chunk starting at {idx}: {exc}")
                        if token_temporarily_blocked:
                            break

                for item in chunk:
                    bl_id = int(item["bl_id"])
                    source_id = item["source_id"]
                    if bl_id in failed_ids:
                        errors.append(
                            f"{source_id}: bulk update warning/failure for product_id={bl_id}"
                        )
                        continue
                    source_to_bl_id[source_id] = bl_id
                    hash_by_source_id[source_id] = item["signature"]
                    shape_hash_by_source_id[source_id] = item["shape_signature"]
                    parent_source_by_source_id[source_id] = item["parent_source_id"]
                    price_qty_by_source_id[source_id] = {
                        "quantity": int(item["qty"]),
                        "price": item["price_norm"],
                    }
                    sku_key = _clean(item.get("sku_key", ""))
                    if sku_key:
                        sku_candidates = existing_bl_ids_by_sku.setdefault(sku_key, [])
                        if bl_id not in sku_candidates:
                            sku_candidates.append(bl_id)
                    requested += 1
                    updated += 1

                queue_budget_used = max(0, queue_budget_used - len(chunk))
                if token_temporarily_blocked or len(errors) >= 50:
                    break

    next_cursor_index = cursor_index + processed_records
    if next_cursor_index < 0:
        next_cursor_index = 0
    if next_cursor_index > total_records:
        next_cursor_index = total_records
    has_more_batches = next_cursor_index < total_records
    remaining_pre_audit_extra_bl_ids = list(pre_audit_extra_bl_ids)

    # Delete phase only after complete pass over all records.
    if not has_more_batches:
        delete_phase_executed = True
        stale_source_ids = [sid for sid in list(source_to_bl_id.keys()) if sid not in desired_source_ids]
        if stale_source_ids:
            stale_set = set(stale_source_ids)
            stale_depth_cache: Dict[str, int] = {}

            def _stale_depth(source_id: str, visiting: set) -> int:
                if source_id in stale_depth_cache:
                    return stale_depth_cache[source_id]
                if source_id in visiting:
                    return 0
                visiting.add(source_id)
                depth = 0
                parent_source_id = _normalize_parent_id(parent_source_by_source_id.get(source_id, ""))
                if parent_source_id and parent_source_id in stale_set:
                    depth = 1 + _stale_depth(parent_source_id, visiting)
                visiting.remove(source_id)
                stale_depth_cache[source_id] = depth
                return depth

            stale_source_ids.sort(key=lambda sid: (-_stale_depth(sid, set()), sid))
            for source_id in stale_source_ids:
                if remaining_ms_fn is not None:
                    try:
                        remaining_ms = int(remaining_ms_fn() or 0)
                    except Exception:
                        remaining_ms = 0
                    if remaining_ms > 0 and remaining_ms <= max(10_000, int(min_remaining_ms_for_continue)):
                        soft_timeout_reached = True
                        delete_phase_deferred = True
                        break
                delete_requested += 1
                bl_id = int(source_to_bl_id.get(source_id, 0) or 0)
                if bl_id <= 0:
                    _drop_state_for_source(source_id)
                    delete_deleted += 1
                    continue
                try:
                    _bl_api_call(
                        api_url=api_url,
                        api_token=api_token,
                        method="deleteInventoryProduct",
                        parameters={"product_id": bl_id},
                        timeout_sec=timeout_sec,
                    )
                    _drop_state_for_source(source_id)
                    delete_deleted += 1
                except Exception as exc:
                    msg = str(exc).lower()
                    if "not exist" in msg or "not found" in msg:
                        _drop_state_for_source(source_id)
                        delete_deleted += 1
                        continue
                    delete_failed += 1
                    errors.append(f"{source_id} delete: {exc}")
                    if len(errors) >= 50:
                        break

        if (not delete_phase_deferred) and remaining_pre_audit_extra_bl_ids and len(errors) < 50:
            extra_ids_to_delete = sorted({int(x) for x in remaining_pre_audit_extra_bl_ids if int(x or 0) > 0})
            for bl_id in extra_ids_to_delete:
                if remaining_ms_fn is not None:
                    try:
                        remaining_ms = int(remaining_ms_fn() or 0)
                    except Exception:
                        remaining_ms = 0
                    if remaining_ms > 0 and remaining_ms <= max(10_000, int(min_remaining_ms_for_continue)):
                        soft_timeout_reached = True
                        delete_phase_deferred = True
                        break
                delete_requested += 1
                try:
                    _bl_api_call(
                        api_url=api_url,
                        api_token=api_token,
                        method="deleteInventoryProduct",
                        parameters={"product_id": int(bl_id)},
                        timeout_sec=timeout_sec,
                    )
                    delete_deleted += 1
                    remaining_pre_audit_extra_bl_ids = [
                        pid for pid in remaining_pre_audit_extra_bl_ids if int(pid or 0) != int(bl_id)
                    ]
                except Exception as exc:
                    msg = str(exc).lower()
                    if "not exist" in msg or "not found" in msg:
                        delete_deleted += 1
                        remaining_pre_audit_extra_bl_ids = [
                            pid for pid in remaining_pre_audit_extra_bl_ids if int(pid or 0) != int(bl_id)
                        ]
                        continue
                    delete_failed += 1
                    errors.append(f"extra_bl_id={bl_id} delete: {exc}")
                    if len(errors) >= 50:
                        break
        if delete_phase_deferred:
            has_more_batches = True
            next_cursor_index = total_records
        else:
            next_cursor_index = 0

    global_processed += processed_records
    global_requested += requested
    global_updated += updated
    global_skipped_unchanged += skipped_unchanged
    global_skipped_missing_parent += skipped_no_parent
    global_errors_count += len(errors)
    global_delete_requested += delete_requested
    global_delete_deleted += delete_deleted
    global_delete_failed += delete_failed
    batch_mutations = max(0, int(requested) + int(delete_deleted))
    batch_duration_sec = 0
    if invocation_started_unix_safe > 0:
        batch_duration_sec = max(1, int(time.time()) - invocation_started_unix_safe)
    batch_actual_rpm = 0.0
    if batch_mutations > 0 and batch_duration_sec > 0:
        batch_actual_rpm = float(batch_mutations) * 60.0 / float(batch_duration_sec)

    eta_ma_rpm_next = float(eta_ma_rpm_prev)
    eta_ma_samples_next = int(eta_ma_samples_prev)
    if eta_moving_avg_enabled_safe and batch_actual_rpm > 0.0:
        eta_ma_rpm_next = (
            (1.0 - eta_ma_alpha_safe) * eta_ma_rpm_prev + eta_ma_alpha_safe * batch_actual_rpm
        )
        eta_ma_rpm_next = max(
            float(eta_ma_min_rpm_safe),
            min(float(effective_rpm), float(eta_ma_rpm_next)),
        )
        eta_ma_samples_next += 1

    serialized_categories_by_parent, serialized_categories_path_cache = (
        _serialize_category_maps_for_state(
            by_parent=by_parent,
            path_cache=path_cache,
        )
    )

    state_out = {
        "sync_config_digest": _clean(sync_config_digest),
        "sync_config": active_sync_config or {},
        "source_to_bl_id": source_to_bl_id,
        "hash_by_source_id": hash_by_source_id,
        "shape_hash_by_source_id": shape_hash_by_source_id,
        "price_qty_by_source_id": price_qty_by_source_id,
        "parent_source_by_source_id": parent_source_by_source_id,
        "sync_pre_audit_extra_bl_ids": remaining_pre_audit_extra_bl_ids,
        "sync_target_source_ids": sync_target_source_ids,
        "sync_cursor_index": next_cursor_index,
        "sync_source_digest": source_digest,
        "sync_source_live_digest_last_seen": live_source_digest,
        "sync_source_snapshot_key": snapshot_key,
        "sync_source_snapshot_digest": source_digest,
        "sync_source_snapshot_reused": bool(source_snapshot_reused),
        "sync_source_snapshot_refreshed": bool(source_snapshot_refreshed),
        "sync_skipped_no_changes": bool(sync_skipped_no_changes),
        "updated_at_unix": int(time.time()),
        "updated_at_iso": datetime.now(timezone.utc).isoformat(),
        "inventory_id": inventory_id,
        "sync_global_cycle_id": global_cycle_id,
        "sync_global_started_unix": global_started_unix,
        "sync_global_total_records": global_total_records,
        "sync_global_processed": global_processed,
        "sync_global_requested": global_requested,
        "sync_global_updated": global_updated,
        "sync_global_skipped_unchanged": global_skipped_unchanged,
        "sync_global_skipped_missing_parent": global_skipped_missing_parent,
        "sync_global_errors_count": global_errors_count,
        "sync_global_delete_requested": global_delete_requested,
        "sync_global_delete_deleted": global_delete_deleted,
        "sync_global_delete_failed": global_delete_failed,
        "sync_global_changed_target": global_changed_target,
        "sync_global_delete_target": global_delete_target,
        "sync_global_completed_unix": int(time.time()) if not has_more_batches else 0,
        "sync_blocked_token": bool(token_temporarily_blocked),
        "sync_blocked_until_unix": int(blocked_token_until_unix or 0),
        "sync_blocked_until_iso": blocked_token_until_iso,
        "sync_eta_ma_enabled": bool(eta_moving_avg_enabled_safe),
        "sync_eta_ma_alpha": float(eta_ma_alpha_safe),
        "sync_eta_ma_min_rpm": int(eta_ma_min_rpm_safe),
        "sync_eta_ma_bootstrap_sec": int(eta_ma_bootstrap_sec_safe),
        "sync_eta_ma_rpm": round(float(eta_ma_rpm_next), 4),
        "sync_eta_ma_samples": int(eta_ma_samples_next),
        "sync_eta_last_batch_rpm": round(float(batch_actual_rpm), 4),
        "sync_eta_last_batch_mutations": int(batch_mutations),
        "sync_eta_last_batch_duration_sec": int(batch_duration_sec),
        "sync_eta_last_updated_unix": int(time.time()),
        "remote_cache": {
            "inventory_id": int(inventory_id),
            "default_price_group": int(default_price_group),
            "default_warehouse": default_warehouse,
            "warehouse_ids": sorted(list(warehouse_ids)),
            "categories_by_parent": serialized_categories_by_parent,
            "categories_path_cache": serialized_categories_path_cache,
            "manufacturer_map": manufacturer_map,
            "existing_bl_ids_by_sku": existing_bl_ids_by_sku,
            "updated_at_unix": int(time.time()),
            "updated_at_iso": datetime.now(timezone.utc).isoformat(),
            "source_digest_hint": source_digest,
        },
    }

    if isinstance(pre_audit_report, dict) and pre_audit_report:
        state_out["sync_pre_audit_phase"] = _clean(pre_audit_report.get("phase", ""))
        state_out["sync_pre_audit_summary_key"] = _clean(pre_audit_report.get("summary_key", ""))
        state_out["sync_pre_audit_details_key"] = _clean(pre_audit_report.get("details_key", ""))
        pre_summary = pre_audit_report.get("summary", {})
        if isinstance(pre_summary, dict):
            state_out["sync_pre_audit_summary"] = {
                "diff_total": int(pre_summary.get("diff_total", 0) or 0),
                "missing_in_bl": int(pre_summary.get("missing_in_bl", 0) or 0),
                "extra_in_bl": int(pre_summary.get("extra_in_bl", 0) or 0),
                "changed_records": int(pre_summary.get("changed_records", 0) or 0),
                "unchanged_records": int(pre_summary.get("unchanged_records", 0) or 0),
                "details_rows_truncated": int(pre_summary.get("details_rows_truncated", 0) or 0),
            }
        if _clean(pre_audit_report.get("error", "")) != "":
            state_out["sync_pre_audit_error"] = _clean(pre_audit_report.get("error", ""))
    if source_snapshot_error != "":
        state_out["sync_source_snapshot_error"] = source_snapshot_error

    # Persist checkpoint before post-audit so the sync never loses progress
    # if post-audit hits timeout.
    _save_json_state(output_bucket=output_bucket, key=state_key, data=state_out)

    post_audit_executed = False
    if bool(full_audit_enabled) and not has_more_batches and total_source_records > 0:
        _emit_progress(
            {
                "sync_stage": "post_audit",
                "phase": "post_audit_started",
                "pre_audit_summary": pre_audit_report.get("summary", {})
                if isinstance(pre_audit_report, dict)
                else {},
                "pre_audit_summary_key": _clean(pre_audit_report.get("summary_key", ""))
                if isinstance(pre_audit_report, dict)
                else "",
            }
        )
        total_mutations_done = int(global_requested) + int(global_delete_deleted)
        if (
            bool(reset_global_counters)
            and int(pre_audit_diff_total) == 0
            and total_mutations_done == 0
        ):
            post_audit_skipped_no_changes = True
            post_audit_report = {
                "phase": "post",
                "skipped": True,
                "reason": "pre_audit_diff_total_zero_and_no_mutations",
            }
        else:
            try:
                post_audit_executed = True
                post_audit_report = _run_full_consistency_audit(
                    phase="post",
                    output_bucket=output_bucket,
                    output_key=output_key,
                    inventory_id=inventory_id,
                    api_url=api_url,
                    api_token=api_token,
                    timeout_sec=timeout_sec,
                    default_price_group=default_price_group,
                    default_warehouse=default_warehouse,
                    records_for_sync=ordered_all,
                    source_to_bl_id=source_to_bl_id,
                    by_parent=by_parent,
                    path_cache=path_cache,
                    manufacturer_map=manufacturer_map,
                    parent_source_ids_with_variants=parent_source_ids_with_variants,
                    details_limit_per_type=full_audit_details_limit_per_type,
                    max_details_rows=full_audit_max_details_rows,
                )
            except Exception as audit_exc:
                post_audit_report = {
                    "phase": "post",
                    "error": f"{type(audit_exc).__name__}: {audit_exc}",
                }

    if isinstance(post_audit_report, dict) and post_audit_report:
        state_out["sync_post_audit_phase"] = _clean(post_audit_report.get("phase", ""))
        state_out["sync_post_audit_summary_key"] = _clean(post_audit_report.get("summary_key", ""))
        state_out["sync_post_audit_details_key"] = _clean(post_audit_report.get("details_key", ""))
        state_out["sync_post_audit_skipped_no_changes"] = bool(post_audit_skipped_no_changes)
        post_summary = post_audit_report.get("summary", {})
        if isinstance(post_summary, dict):
            state_out["sync_post_audit_summary"] = {
                "diff_total": int(post_summary.get("diff_total", 0) or 0),
                "missing_in_bl": int(post_summary.get("missing_in_bl", 0) or 0),
                "extra_in_bl": int(post_summary.get("extra_in_bl", 0) or 0),
                "changed_records": int(post_summary.get("changed_records", 0) or 0),
                "unchanged_records": int(post_summary.get("unchanged_records", 0) or 0),
                "details_rows_truncated": int(post_summary.get("details_rows_truncated", 0) or 0),
            }
        if _clean(post_audit_report.get("error", "")) != "":
            state_out["sync_post_audit_error"] = _clean(post_audit_report.get("error", ""))
    _save_json_state(output_bucket=output_bucket, key=state_key, data=state_out)

    _emit_progress(
        {
            "sync_stage": "finished" if not has_more_batches else "sync",
            "phase": "batch_finished",
            "has_more_batches": has_more_batches,
            "delete_phase_executed": delete_phase_executed,
            "next_cursor_index": next_cursor_index,
            "errors_count": len(errors),
            "pre_audit_executed": bool(pre_audit_report),
            "pre_audit_summary": pre_audit_report.get("summary", {}) if isinstance(pre_audit_report, dict) else {},
            "pre_audit_summary_key": _clean(pre_audit_report.get("summary_key", "")) if isinstance(pre_audit_report, dict) else "",
            "sync_skipped_no_changes": bool(sync_skipped_no_changes),
            "post_audit_executed": bool(post_audit_executed),
            "post_audit_summary": post_audit_report.get("summary", {}) if isinstance(post_audit_report, dict) else {},
            "post_audit_summary_key": _clean(post_audit_report.get("summary_key", "")) if isinstance(post_audit_report, dict) else "",
            "post_audit_skipped_no_changes": bool(post_audit_skipped_no_changes),
            "post_audit_skipped_reason": _clean(post_audit_report.get("reason", "")) if isinstance(post_audit_report, dict) else "",
        }
    )

    return {
        "ok": True,
        "mode": "push_to_bl_api",
        "inventory_id": inventory_id,
        "default_price_group": default_price_group,
        "default_warehouse": default_warehouse,
        "state_key": state_key,
        "source_records_total": total_records,
        "source_live_digest_last_seen": live_source_digest,
        "source_snapshot_key": snapshot_key,
        "source_snapshot_digest": source_digest,
        "source_snapshot_reused": bool(source_snapshot_reused),
        "source_snapshot_refreshed": bool(source_snapshot_refreshed),
        "source_snapshot_error": source_snapshot_error,
        "sync_skipped_no_changes": bool(sync_skipped_no_changes),
        "batch_start_index": cursor_index,
        "batch_end_index": next_cursor_index,
        "batch_records_scanned": processed_records,
        "batch_records_target": len(batch_records),
        "has_more_batches": has_more_batches,
        "upsert_limit_reached": upsert_limit_reached,
        "soft_timeout_reached": soft_timeout_reached,
        "token_temporarily_blocked": token_temporarily_blocked,
        "blocked_token_error_message": blocked_token_error_message,
        "blocked_token_until_unix": int(blocked_token_until_unix or 0),
        "blocked_token_until_iso": blocked_token_until_iso,
        "delete_phase_executed": delete_phase_executed,
        "sync_cursor_index": next_cursor_index,
        "sync_requested": requested,
        "sync_updated": updated,
        "sync_skipped_unchanged": skipped_unchanged,
        "sync_skipped_missing_parent": skipped_no_parent,
        "sync_matched_existing_by_sku": matched_existing_by_sku,
        "sync_matched_existing_by_sku_ambiguous": matched_existing_by_sku_ambiguous,
        "existing_sku_keys": len(existing_bl_ids_by_sku),
        "bulk_queue_records": bulk_queue_records,
        "bulk_stock_calls": bulk_stock_calls,
        "bulk_price_calls": bulk_price_calls,
        "bulk_stock_records": bulk_stock_records,
        "bulk_price_records": bulk_price_records,
        "bulk_skipped_by_threshold": bulk_skipped_by_threshold,
        "sync_delete_requested": delete_requested,
        "sync_delete_deleted": delete_deleted,
        "sync_delete_failed": delete_failed,
        "created_categories": created_categories,
        "created_manufacturers": created_manufacturers,
        "cache_used": cache_used,
        "cache_ttl_sec": cache_ttl_sec,
        "cache_inventory_meta_used": cache_inventory_meta_used,
        "cache_category_map_used": cache_category_map_used,
        "cache_manufacturer_map_used": cache_manufacturer_map_used,
        "cache_existing_sku_map_used": cache_existing_sku_map_used,
        "cache_inventory_meta_refreshed": cache_inventory_meta_refreshed,
        "cache_category_map_refreshed": cache_category_map_refreshed,
        "cache_manufacturer_map_refreshed": cache_manufacturer_map_refreshed,
        "cache_existing_sku_map_refreshed": cache_existing_sku_map_refreshed,
        "eta_ma_enabled": bool(eta_moving_avg_enabled_safe),
        "eta_ma_alpha": round(float(eta_ma_alpha_safe), 4),
        "eta_ma_rpm_prev": round(float(eta_ma_rpm_prev), 2),
        "eta_ma_rpm_next": round(float(eta_ma_rpm_next), 2),
        "eta_last_batch_rpm": round(float(batch_actual_rpm), 2),
        "eta_last_batch_duration_sec": int(batch_duration_sec),
        "eta_last_batch_mutations": int(batch_mutations),
        "global_cycle_id": global_cycle_id,
        "global_started_unix": global_started_unix,
        "global_total_records": global_total_records,
        "global_processed": global_processed,
        "global_requested": global_requested,
        "global_updated": global_updated,
        "global_skipped_unchanged": global_skipped_unchanged,
        "global_skipped_missing_parent": global_skipped_missing_parent,
        "global_errors_count": global_errors_count,
        "global_delete_requested": global_delete_requested,
        "global_delete_deleted": global_delete_deleted,
        "global_delete_failed": global_delete_failed,
        "global_changed_target": global_changed_target,
        "global_delete_target": global_delete_target,
        "sync_pre_audit_extra_bl_ids_remaining": len(remaining_pre_audit_extra_bl_ids),
        "sync_stage": "finished" if not has_more_batches else "sync",
        "pre_audit_executed": bool(pre_audit_report),
        "pre_audit_phase": _clean(pre_audit_report.get("phase", "")) if isinstance(pre_audit_report, dict) else "",
        "pre_audit_summary_key": _clean(pre_audit_report.get("summary_key", "")) if isinstance(pre_audit_report, dict) else "",
        "pre_audit_details_key": _clean(pre_audit_report.get("details_key", "")) if isinstance(pre_audit_report, dict) else "",
        "pre_audit_summary": pre_audit_report.get("summary", {}) if isinstance(pre_audit_report, dict) else {},
        "pre_audit_error": _clean(pre_audit_report.get("error", "")) if isinstance(pre_audit_report, dict) else "",
        "post_audit_executed": bool(post_audit_executed),
        "post_audit_phase": _clean(post_audit_report.get("phase", "")) if isinstance(post_audit_report, dict) else "",
        "post_audit_summary_key": _clean(post_audit_report.get("summary_key", "")) if isinstance(post_audit_report, dict) else "",
        "post_audit_details_key": _clean(post_audit_report.get("details_key", "")) if isinstance(post_audit_report, dict) else "",
        "post_audit_summary": post_audit_report.get("summary", {}) if isinstance(post_audit_report, dict) else {},
        "post_audit_error": _clean(post_audit_report.get("error", "")) if isinstance(post_audit_report, dict) else "",
        "post_audit_skipped_no_changes": bool(post_audit_skipped_no_changes),
        "post_audit_skipped_reason": _clean(post_audit_report.get("reason", "")) if isinstance(post_audit_report, dict) else "",
        "errors_count": len(errors),
        "errors": errors[:20],
        **_build_eta_fields(requested_now=0, delete_deleted_now=0),
        **base_stats,
    }


def _enqueue_self_sync(
    function_arn: str,
    next_chain_depth: int,
    continue_not_before_unix: int = 0,
    chain_reason: str = "",
) -> int:
    payload = {
        "sync_chain": True,
        "sync_chain_depth": int(next_chain_depth),
        "invoked_at_unix": int(time.time()),
    }
    if int(continue_not_before_unix or 0) > 0:
        payload["continue_not_before_unix"] = int(continue_not_before_unix)
    reason_text = _clean(chain_reason)
    if reason_text != "":
        payload["sync_chain_reason"] = reason_text
    response = lambda_api.invoke(
        FunctionName=function_arn,
        InvocationType="Event",
        Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )
    return int(response.get("StatusCode", 0) or 0)


def _enqueue_sync_continuation(
    function_arn: str,
    next_chain_depth: int,
    continuation_sqs_url: str = "",
    continue_not_before_unix: int = 0,
    chain_reason: str = "",
    blocked_min_delay_sec: int = DEFAULT_BL_CONTINUATION_BLOCKED_MIN_DELAY_SEC,
) -> Dict[str, Any]:
    payload = {
        "sync_chain": True,
        "sync_chain_depth": int(next_chain_depth),
        "invoked_at_unix": int(time.time()),
    }
    if int(continue_not_before_unix or 0) > 0:
        payload["continue_not_before_unix"] = int(continue_not_before_unix)
    reason_text = _clean(chain_reason)
    if reason_text != "":
        payload["sync_chain_reason"] = reason_text

    queue_url = _clean(continuation_sqs_url)
    if queue_url != "":
        now_unix = int(time.time())
        delay_sec = max(0, int(continue_not_before_unix or 0) - now_unix)
        if reason_text in {"blocked_token_resume", "blocked_token_resume_wait"}:
            delay_sec = max(delay_sec, max(0, int(blocked_min_delay_sec or 0)))
        delay_sec = min(900, max(0, int(delay_sec)))
        response = sqs_api.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(payload, ensure_ascii=False),
            DelaySeconds=delay_sec,
        )
        metadata = response.get("ResponseMetadata", {})
        status_code = int(metadata.get("HTTPStatusCode", 0) or 0)
        return {
            "ok": status_code in {200, 201, 202},
            "status_code": status_code,
            "mode": "sqs_delay",
            "delay_seconds": int(delay_sec),
            "message_id": _clean(response.get("MessageId", "")),
        }

    status_code = _enqueue_self_sync(
        function_arn=function_arn,
        next_chain_depth=next_chain_depth,
        continue_not_before_unix=continue_not_before_unix,
        chain_reason=chain_reason,
    )
    return {
        "ok": status_code == 202,
        "status_code": int(status_code),
        "mode": "lambda_async",
        "delay_seconds": 0,
        "message_id": "",
    }


def _extract_event_payload(event: Any) -> Tuple[Dict[str, Any], str]:
    if not isinstance(event, dict):
        return {}, "unknown"

    records = event.get("Records")
    if isinstance(records, list) and records:
        first = records[0]
        if isinstance(first, dict) and _clean(first.get("eventSource", "")) == "aws:sqs":
            body_raw = first.get("body", "")
            if isinstance(body_raw, (bytes, bytearray)):
                body_raw = body_raw.decode("utf-8", errors="ignore")
            payload: Dict[str, Any] = {}
            if isinstance(body_raw, str) and _clean(body_raw) != "":
                try:
                    parsed = json.loads(body_raw)
                    if isinstance(parsed, dict):
                        payload = parsed
                except Exception:
                    payload = {}
            payload["_trigger"] = "sqs"
            payload["_sqs_message_id"] = _clean(first.get("messageId", ""))
            return payload, "sqs"

    return event, "direct"


def lambda_handler(event, context):
    default_source_xml_url = _env_str("XML_URL", "")
    if default_source_xml_url == "":
        raise RuntimeError("XML_URL must be configured.")
    output_bucket = os.environ["OUTPUT_BUCKET"]
    output_key = os.getenv("OUTPUT_KEY", "feeds/baselinker/products.xml")
    timeout_sec = _env_int("REQUEST_TIMEOUT_SEC", 180)
    event_obj, event_trigger_source = _extract_event_payload(event)
    is_chain_invoke = bool(event_obj.get("sync_chain", False))
    current_chain_depth = _parse_int(str(event_obj.get("sync_chain_depth", 0)), 0)
    if current_chain_depth < 0:
        current_chain_depth = 0

    include_orphans_as_products = _env_bool("INCLUDE_ORPHANS_AS_PRODUCTS", False)

    sync_config_param = _env_str("BL_SYNC_CONFIG_SSM_PARAM", "")
    default_bl_inventory_id = _env_int("BL_INVENTORY_ID", 0)
    default_bl_warehouse_id = _env_str("BL_WAREHOUSE_ID", "")
    default_bl_api_max_rpm = _env_int("BL_API_MAX_RPM", DEFAULT_BL_API_MAX_RPM)
    active_sync_config = _load_sync_config_from_ssm(
        parameter_name=sync_config_param,
        default_source_xml_url=default_source_xml_url,
        default_inventory_id=default_bl_inventory_id,
        default_warehouse_id=default_bl_warehouse_id,
        default_api_max_rpm=default_bl_api_max_rpm,
    )
    active_sync_config_digest = _sync_config_digest(active_sync_config)
    source_xml_url = _source_xml_url_from_config(active_sync_config)
    if source_xml_url == "":
        source_xml_url = default_source_xml_url
    bl_inventory_id = int(active_sync_config.get("bl_inventory_id", 0) or 0)
    bl_warehouse_id = _clean(active_sync_config.get("bl_warehouse_id", ""))
    bl_api_max_rpm = int(active_sync_config.get("bl_api_max_rpm", default_bl_api_max_rpm) or default_bl_api_max_rpm)
    configured_bl_api_max_rpm = _configure_bl_rate_limiter(bl_api_max_rpm)
    progress_update_every = _env_int(
        "BL_PROGRESS_UPDATE_EVERY", DEFAULT_BL_PROGRESS_UPDATE_EVERY
    )
    min_remaining_ms_for_continue = _env_int(
        "BL_MIN_REMAINING_MS_FOR_CONTINUE", DEFAULT_BL_MIN_REMAINING_MS_FOR_CONTINUE
    )
    remote_cache_ttl_sec = _env_int(
        "BL_REMOTE_CACHE_TTL_SEC", DEFAULT_BL_REMOTE_CACHE_TTL_SEC
    )
    bulk_update_enabled = _env_bool(
        "BL_BULK_UPDATE_ENABLED", DEFAULT_BL_BULK_UPDATE_ENABLED
    )
    bulk_update_max_items = _env_int(
        "BL_BULK_UPDATE_MAX_ITEMS", DEFAULT_BL_BULK_UPDATE_MAX_ITEMS
    )
    bulk_update_min_items = _env_int(
        "BL_BULK_UPDATE_MIN_ITEMS", DEFAULT_BL_BULK_UPDATE_MIN_ITEMS
    )
    eta_moving_avg_enabled = _env_bool(
        "BL_ETA_MOVING_AVG_ENABLED", DEFAULT_BL_ETA_MOVING_AVG_ENABLED
    )
    eta_ma_alpha = _env_float("BL_ETA_MA_ALPHA", DEFAULT_BL_ETA_MA_ALPHA)
    eta_ma_min_rpm = _env_int("BL_ETA_MA_MIN_RPM", DEFAULT_BL_ETA_MA_MIN_RPM)
    eta_ma_bootstrap_sec = _env_int(
        "BL_ETA_MA_BOOTSTRAP_SEC", DEFAULT_BL_ETA_MA_BOOTSTRAP_SEC
    )
    full_audit_enabled = _env_bool(
        "BL_FULL_AUDIT_ENABLED", DEFAULT_BL_FULL_AUDIT_ENABLED
    )
    full_audit_details_limit_per_type = _env_int(
        "BL_FULL_AUDIT_DETAILS_LIMIT_PER_TYPE",
        DEFAULT_BL_FULL_AUDIT_DETAILS_LIMIT_PER_TYPE,
    )
    full_audit_max_details_rows = _env_int(
        "BL_FULL_AUDIT_MAX_DETAILS_ROWS", DEFAULT_BL_FULL_AUDIT_MAX_DETAILS_ROWS
    )
    reset_state_if_status_stale_enabled = _env_bool(
        "BL_RESET_STATE_IF_STATUS_STALE_ENABLED",
        DEFAULT_BL_RESET_STATE_IF_STATUS_STALE_ENABLED,
    )
    reset_state_if_status_stale_sec = _env_int(
        "BL_RESET_STATE_IF_STATUS_STALE_SEC",
        DEFAULT_BL_RESET_STATE_IF_STATUS_STALE_SEC,
    )
    sync_status_param = _env_str(
        "BL_SYNC_STATUS_SSM_PARAM", DEFAULT_BL_SYNC_STATUS_SSM_PARAM
    )
    budget_fx_rate_ssm_param = _env_str(
        "BUDGET_FX_RATE_SSM_PARAM", DEFAULT_BUDGET_FX_RATE_SSM_PARAM
    )
    budget_usd_to_pln_rate = _env_float(
        "BUDGET_USD_TO_PLN_RATE", DEFAULT_BUDGET_USD_TO_PLN_RATE
    )
    nbp_usd_pln_url = _env_str("NBP_USD_PLN_URL", DEFAULT_NBP_USD_PLN_URL)
    enable_self_chain = _env_bool("BL_ENABLE_SELF_CHAIN", DEFAULT_BL_ENABLE_SELF_CHAIN)
    max_chain_depth = _env_int("BL_MAX_CHAIN_DEPTH", DEFAULT_BL_MAX_CHAIN_DEPTH)
    continuation_sqs_url = _env_str("BL_CONTINUATION_SQS_URL", "")
    continuation_blocked_min_delay_sec = _env_int(
        "BL_CONTINUATION_BLOCKED_MIN_DELAY_SEC",
        DEFAULT_BL_CONTINUATION_BLOCKED_MIN_DELAY_SEC,
    )
    blocked_token_resume_buffer_sec = _env_int(
        "BL_BLOCKED_TOKEN_RESUME_BUFFER_SEC",
        DEFAULT_BL_BLOCKED_TOKEN_RESUME_BUFFER_SEC,
    )
    blocked_token_resume_jitter_sec = _env_int(
        "BL_BLOCKED_TOKEN_RESUME_JITTER_SEC",
        DEFAULT_BL_BLOCKED_TOKEN_RESUME_JITTER_SEC,
    )
    blocked_token_fallback_delay_sec = _env_int(
        "BL_BLOCKED_TOKEN_FALLBACK_DELAY_SEC",
        DEFAULT_BL_BLOCKED_TOKEN_FALLBACK_DELAY_SEC,
    )
    blocked_token_max_inline_wait_sec = _env_int(
        "BL_BLOCKED_TOKEN_MAX_INLINE_WAIT_SEC",
        DEFAULT_BL_BLOCKED_TOKEN_MAX_INLINE_WAIT_SEC,
    )
    run_started_unix = int(time.time())
    request_id = getattr(context, "aws_request_id", "")
    run_id = f"{run_started_unix}-{request_id if request_id else 'local'}"
    budget_fx_rate: Dict[str, Any] = {}
    if not is_chain_invoke:
        budget_fx_rate = _refresh_budget_fx_rate_ssm(
            parameter_name=budget_fx_rate_ssm_param,
            fallback_usd_to_pln_rate=budget_usd_to_pln_rate,
            nbp_url=nbp_usd_pln_url,
        )

    def _write_sync_status(status: str, extra: Optional[Dict[str, Any]] = None) -> None:
        payload: Dict[str, Any] = {
            "status": status,
            "mode": "push_to_bl_api",
            "run_id": run_id,
            "sync_chain_depth": current_chain_depth,
            "updated_at_unix": int(time.time()),
            "updated_at_iso": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            payload.update(extra)
        _safe_put_sync_status(sync_status_param, payload)

    stale_reset_info: Dict[str, Any] = {
        "enabled": bool(reset_state_if_status_stale_enabled),
        "reset_performed": False,
        "stale_after_sec": max(1, int(reset_state_if_status_stale_sec or 1)),
        "status_updated_at_unix": 0,
        "status_age_sec": 0,
        "status": "",
        "status_run_id": "",
        "deleted_keys": [],
        "delete_errors": [],
    }
    # Only the root/scheduled invoke may reset stale state.
    # Chained invocations should continue the in-flight cycle.
    if (not is_chain_invoke) and bool(reset_state_if_status_stale_enabled):
        stale_reset_info = _maybe_reset_stale_sync_state(
            output_bucket=output_bucket,
            output_key=output_key,
            sync_status_param=sync_status_param,
            stale_after_sec=max(1, int(reset_state_if_status_stale_sec or 1)),
        )
    config_reset_info: Dict[str, Any] = {
        "enabled": True,
        "reset_performed": False,
        "previous_digest": "",
        "current_digest": active_sync_config_digest,
        "deleted_keys": [],
        "delete_errors": [],
    }
    if not is_chain_invoke:
        config_reset_info = _maybe_reset_state_for_config_change(
            output_bucket=output_bucket,
            output_key=output_key,
            current_config_digest=active_sync_config_digest,
        )

    continue_not_before_unix = _parse_int(
        str(event_obj.get("continue_not_before_unix", 0)),
        0,
    )
    if is_chain_invoke and continue_not_before_unix > int(time.time()):
        chain_reason = _clean(event_obj.get("sync_chain_reason", ""))
        wait_remaining_sec = continue_not_before_unix - int(time.time())
        if wait_remaining_sec > 0:
            if enable_self_chain and current_chain_depth < max_chain_depth:
                function_arn_for_self = (
                    getattr(context, "invoked_function_arn", "")
                    if context is not None
                    else _env_str("AWS_LAMBDA_FUNCTION_NAME", "")
                )
                continuation_result = _enqueue_sync_continuation(
                    function_arn=function_arn_for_self,
                    next_chain_depth=current_chain_depth,
                    continuation_sqs_url=continuation_sqs_url,
                    continue_not_before_unix=continue_not_before_unix,
                    chain_reason=chain_reason if chain_reason else "blocked_token_resume_wait",
                    blocked_min_delay_sec=continuation_blocked_min_delay_sec,
                )
                _write_sync_status(
                    "running",
                    {
                        "message": "Continuation re-enqueued while waiting for token unblock (no inline sleep).",
                        "wait_for_unblock": True,
                        "wait_seconds_remaining": int(wait_remaining_sec),
                        "continue_not_before_unix": int(continue_not_before_unix),
                        "continue_not_before_iso": datetime.fromtimestamp(
                            continue_not_before_unix, tz=POLAND_TZ
                        ).isoformat(),
                        "continuation_enqueued": bool(continuation_result.get("ok", False)),
                        "continuation_status_code": int(continuation_result.get("status_code", 0) or 0),
                        "continuation_mode": _clean(continuation_result.get("mode", "")),
                        "continuation_delay_seconds": int(continuation_result.get("delay_seconds", 0) or 0),
                        "continuation_message_id": _clean(continuation_result.get("message_id", "")),
                        "sync_chain_depth": current_chain_depth,
                        "chain_reason": chain_reason,
                    },
                )
                return {
                    "ok": True,
                    "mode": "push_to_bl_api",
                    "waiting_for_blocked_token_unblock": True,
                    "continuation_enqueued": bool(continuation_result.get("ok", False)),
                    "continuation_status_code": int(continuation_result.get("status_code", 0) or 0),
                    "continuation_mode": _clean(continuation_result.get("mode", "")),
                    "continuation_delay_seconds": int(continuation_result.get("delay_seconds", 0) or 0),
                    "continuation_message_id": _clean(continuation_result.get("message_id", "")),
                    "continue_not_before_unix": int(continue_not_before_unix),
                    "continue_not_before_iso": datetime.fromtimestamp(
                        continue_not_before_unix, tz=POLAND_TZ
                    ).isoformat(),
                    "wait_seconds_remaining": int(wait_remaining_sec),
                    "sync_chain_depth": current_chain_depth,
                }
            _write_sync_status(
                "error",
                {
                    "message": "Continuation wait window active but self-chain is unavailable.",
                    "wait_for_unblock": True,
                    "wait_seconds_remaining": int(wait_remaining_sec),
                    "continue_not_before_unix": int(continue_not_before_unix),
                    "continue_not_before_iso": datetime.fromtimestamp(
                        continue_not_before_unix, tz=POLAND_TZ
                    ).isoformat(),
                    "sync_chain_depth": current_chain_depth,
                },
            )
            return {
                "ok": False,
                "mode": "push_to_bl_api",
                "error": "Continuation wait window active but self-chain is unavailable.",
                "continue_not_before_unix": int(continue_not_before_unix),
                "wait_seconds_remaining": int(wait_remaining_sec),
                "sync_chain_depth": current_chain_depth,
            }

    _write_sync_status(
        "running",
        {
            "started_at_unix": run_started_unix,
            "started_at_iso": datetime.now(timezone.utc).isoformat(),
            "message": "Sync started.",
            "sync_config": active_sync_config,
            "sync_config_digest": active_sync_config_digest,
            "sync_config_param": sync_config_param,
            "budget_fx_rate": budget_fx_rate,
            "budget_fx_rate_ssm_param": budget_fx_rate_ssm_param,
            "stale_state_reset": stale_reset_info,
            "config_state_reset": config_reset_info,
        },
    )
    try:
        sync_state_preview = _load_json_state(
            output_bucket=output_bucket,
            key=_state_key_for_api_sync(output_key),
        )
        preview_cursor = _parse_int(
            str(sync_state_preview.get("sync_cursor_index", 0)),
            0,
        )
        if preview_cursor < 0:
            preview_cursor = 0
        preview_snapshot_key = _clean(
            sync_state_preview.get("sync_source_snapshot_key", "")
        )
        if preview_snapshot_key == "":
            preview_snapshot_key = _source_snapshot_key_for_api_sync(output_key)

        source_xml: Optional[bytes] = None
        source_live_digest_hint = ""
        source_fetch_mode = "live_download"
        if preview_cursor > 0:
            snapshot_prefetch = _load_source_snapshot(
                output_bucket=output_bucket,
                key=preview_snapshot_key,
            )
            if snapshot_prefetch is not None:
                source_xml = snapshot_prefetch
                source_live_digest_hint = _clean(
                    sync_state_preview.get("sync_source_live_digest_last_seen", "")
                )
                if source_live_digest_hint == "":
                    source_live_digest_hint = _clean(
                        sync_state_preview.get("sync_source_digest", "")
                    )
                source_fetch_mode = "snapshot_resume"
            else:
                source_fetch_mode = "snapshot_missing_fallback_live_download"

        if source_xml is None:
            source_xml = _download(source_xml_url, timeout_sec=timeout_sec)
            if source_live_digest_hint == "":
                source_live_digest_hint = hashlib.sha1(source_xml).hexdigest()

        bl_api_token = _resolve_bl_api_token()
        if bl_inventory_id <= 0:
            raise RuntimeError("BL_INVENTORY_ID must be set to a valid inventory ID.")
        bl_api_url = _clean(os.getenv("BL_API_URL", DEFAULT_BL_API_URL))
        bl_api_timeout_sec = _env_int("BL_API_TIMEOUT_SEC", DEFAULT_BL_API_TIMEOUT_SEC)
        max_upserts_per_run = _env_int(
            "BL_MAX_UPSERTS_PER_RUN", DEFAULT_BL_MAX_UPSERTS_PER_RUN
        )
        max_records_per_run = _env_int(
            "BL_MAX_RECORDS_PER_RUN", DEFAULT_BL_MAX_RECORDS_PER_RUN
        )

        def _remaining_ms() -> int:
            if context is None:
                return 0
            try:
                return int(context.get_remaining_time_in_millis())
            except Exception:
                return 0

        def _progress_callback(progress: Dict[str, Any]) -> None:
            _write_sync_status(
                "running",
                {
                    "inventory_id": bl_inventory_id,
                    "warehouse_id": bl_warehouse_id,
                    "progress": progress,
                    "message": "Sync in progress.",
                },
            )

        result = _sync_to_bl_api(
            source_xml=source_xml,
            include_orphans_as_products=include_orphans_as_products,
            output_bucket=output_bucket,
            output_key=output_key,
            sync_config_digest=active_sync_config_digest,
            active_sync_config=active_sync_config,
            api_url=bl_api_url,
            api_token=bl_api_token,
            inventory_id=bl_inventory_id,
            preferred_warehouse_id=bl_warehouse_id,
            timeout_sec=bl_api_timeout_sec,
            max_upserts_per_run=max_upserts_per_run,
            max_records_per_run=max_records_per_run,
            api_rate_limit_rpm=configured_bl_api_max_rpm,
            progress_update_every=progress_update_every,
            remote_cache_ttl_sec=remote_cache_ttl_sec,
            bulk_update_enabled=bulk_update_enabled,
            bulk_update_max_items=bulk_update_max_items,
            bulk_update_min_items=bulk_update_min_items,
            eta_moving_avg_enabled=eta_moving_avg_enabled,
            eta_ma_alpha=eta_ma_alpha,
            eta_ma_min_rpm=eta_ma_min_rpm,
            eta_ma_bootstrap_sec=eta_ma_bootstrap_sec,
            full_audit_enabled=full_audit_enabled,
            full_audit_details_limit_per_type=full_audit_details_limit_per_type,
            full_audit_max_details_rows=full_audit_max_details_rows,
            source_live_digest_hint=source_live_digest_hint,
            invocation_started_unix=run_started_unix,
            progress_callback=_progress_callback,
            remaining_ms_fn=_remaining_ms,
            min_remaining_ms_for_continue=min_remaining_ms_for_continue,
        )
        result["source_fetch_mode"] = source_fetch_mode
        result["sync_config"] = active_sync_config
        result["sync_config_digest"] = active_sync_config_digest
        result["sync_config_param"] = sync_config_param
        result["resume_cursor_hint"] = int(preview_cursor)
        result["resume_snapshot_key"] = preview_snapshot_key
        result["bl_api_max_rpm"] = configured_bl_api_max_rpm
        result["self_chain_enabled"] = bool(enable_self_chain)
        result["sync_chain_depth"] = current_chain_depth
        result["max_chain_depth"] = max_chain_depth
        result["max_upserts_per_run"] = max_upserts_per_run
        result["max_records_per_run"] = max_records_per_run
        result["progress_update_every"] = progress_update_every
        result["min_remaining_ms_for_continue"] = min_remaining_ms_for_continue
        result["remote_cache_ttl_sec"] = remote_cache_ttl_sec
        result["bulk_update_enabled"] = bool(bulk_update_enabled)
        result["bulk_update_max_items"] = bulk_update_max_items
        result["bulk_update_min_items"] = bulk_update_min_items
        result["eta_moving_avg_enabled"] = bool(eta_moving_avg_enabled)
        result["eta_ma_alpha"] = float(eta_ma_alpha)
        result["eta_ma_min_rpm"] = int(eta_ma_min_rpm)
        result["eta_ma_bootstrap_sec"] = int(eta_ma_bootstrap_sec)
        result["full_audit_enabled"] = bool(full_audit_enabled)
        result["full_audit_details_limit_per_type"] = int(full_audit_details_limit_per_type)
        result["full_audit_max_details_rows"] = int(full_audit_max_details_rows)
        result["blocked_token_resume_buffer_sec"] = int(blocked_token_resume_buffer_sec)
        result["blocked_token_resume_jitter_sec"] = int(blocked_token_resume_jitter_sec)
        result["blocked_token_fallback_delay_sec"] = int(blocked_token_fallback_delay_sec)
        result["blocked_token_max_inline_wait_sec"] = int(blocked_token_max_inline_wait_sec)
        result["continuation_sqs_url_configured"] = bool(_clean(continuation_sqs_url) != "")
        result["continuation_blocked_min_delay_sec"] = int(continuation_blocked_min_delay_sec)
        result["event_trigger_source"] = event_trigger_source
        result["sync_status_param"] = sync_status_param
        result["stale_state_reset"] = stale_reset_info
        result["config_state_reset"] = config_reset_info
        result["continuation_enqueued"] = False
        result["continuation_status_code"] = 0
        if result.get("has_more_batches") and enable_self_chain:
            if current_chain_depth >= max_chain_depth:
                result["continuation_error"] = (
                    "Max chain depth reached; next batch was not enqueued."
                )
            else:
                next_depth = current_chain_depth + 1
                continue_not_before_unix = 0
                chain_reason = ""
                if result.get("token_temporarily_blocked"):
                    continue_not_before_unix = _compute_blocked_token_resume_unix(
                        blocked_until_unix=int(result.get("blocked_token_until_unix", 0) or 0),
                        fallback_delay_sec=blocked_token_fallback_delay_sec,
                        buffer_sec=blocked_token_resume_buffer_sec,
                        jitter_sec=blocked_token_resume_jitter_sec,
                    )
                    chain_reason = "blocked_token_resume"
                    result["continuation_not_before_unix"] = int(continue_not_before_unix)
                    result["continuation_not_before_iso"] = datetime.fromtimestamp(
                        continue_not_before_unix, tz=POLAND_TZ
                    ).isoformat()
                    result["continuation_delay_sec"] = max(
                        0, int(continue_not_before_unix) - int(time.time())
                    )
                continuation_result = _enqueue_sync_continuation(
                    function_arn=context.invoked_function_arn,
                    next_chain_depth=next_depth,
                    continuation_sqs_url=continuation_sqs_url,
                    continue_not_before_unix=continue_not_before_unix,
                    chain_reason=chain_reason,
                    blocked_min_delay_sec=continuation_blocked_min_delay_sec,
                )
                result["continuation_enqueued"] = bool(continuation_result.get("ok", False))
                result["continuation_status_code"] = int(continuation_result.get("status_code", 0) or 0)
                result["continuation_mode"] = _clean(continuation_result.get("mode", ""))
                result["continuation_delay_seconds"] = int(continuation_result.get("delay_seconds", 0) or 0)
                result["continuation_message_id"] = _clean(continuation_result.get("message_id", ""))
                result["continuation_next_depth"] = next_depth

        post_audit_alert = _publish_post_audit_alert(
            sync_result=result,
            run_id=run_id,
            topic_arn=_env_str("POST_SYNC_ALERT_TOPIC_ARN", ""),
            output_bucket=output_bucket,
            admin_portal_url=_env_str("ADMIN_PORTAL_URL", ""),
        )
        result["post_audit_alert_required"] = bool(
            post_audit_alert.get("required", False)
        )
        result["post_audit_alert_published"] = bool(
            post_audit_alert.get("published", False)
        )
        result["post_audit_alert_message_id"] = _clean(
            post_audit_alert.get("message_id", "")
        )
        result["post_audit_alert_error"] = _clean(
            post_audit_alert.get("error", "")
        )

        if result.get("has_more_batches"):
            if result.get("continuation_enqueued"):
                status_name = "running"
                status_msg = "Batch finished; continuation enqueued."
            else:
                status_name = "error"
                status_msg = "Batch finished but continuation was not enqueued."
        else:
            status_name = "success"
            if result.get("sync_skipped_no_changes"):
                status_msg = "Sync skipped; pre-audit diff_total=0."
            else:
                status_msg = "Sync finished."

        _write_sync_status(
            status_name,
            {
                "inventory_id": bl_inventory_id,
                "warehouse_id": bl_warehouse_id,
                "message": status_msg,
                "result": result,
            },
        )
        return result
    except Exception as exc:
        _write_sync_status(
            "error",
            {
                "message": "Sync failed.",
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        raise
