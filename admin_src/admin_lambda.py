from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import boto3


ssm = boto3.client("ssm")
lambda_api = boto3.client("lambda")
scheduler = boto3.client("scheduler")
budgets = boto3.client("budgets", region_name="us-east-1")

POLAND_TZ = ZoneInfo("Europe/Warsaw")
STATUS_PARAM = os.getenv("SYNC_STATUS_PARAM", "/comarch-baselinker-sync/push-sync-status")
SYNC_FUNCTION_NAME = os.getenv("SYNC_FUNCTION_NAME", "comarch-baselinker-sync")
SYNC_CONFIG_PARAM = os.getenv("SYNC_CONFIG_PARAM", "/comarch-baselinker-sync/sync-config")
DEFAULT_COMARCH_XML_URL = os.getenv("DEFAULT_COMARCH_XML_URL", "")
DEFAULT_BL_INVENTORY_ID = os.getenv("DEFAULT_BL_INVENTORY_ID", "")
DEFAULT_BL_WAREHOUSE_ID = os.getenv("DEFAULT_BL_WAREHOUSE_ID", "")
DEFAULT_BL_API_MAX_RPM = os.getenv("DEFAULT_BL_API_MAX_RPM", "90")
BL_API_URL = os.getenv("BL_API_URL", "https://api.baselinker.com/connector.php")
BL_API_TOKEN_SSM_PARAM = os.getenv(
    "BL_API_TOKEN_SSM_PARAM",
    "/comarch-baselinker-sync/api-token",
)
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD_SHA256 = os.getenv("ADMIN_PASSWORD_SHA256", "")
SCHEDULE_NAME = os.getenv("SCHEDULE_NAME", "comarch-baselinker-sync-midnight")
SCHEDULE_GROUP = os.getenv("SCHEDULE_GROUP", "default")
BUDGET_NAME = os.getenv("BUDGET_NAME", "comarch-baselinker-sync-monthly-budget")
BUDGET_LIMIT_USD = os.getenv("BUDGET_LIMIT_USD", "30")
BUDGET_USD_TO_PLN_RATE = os.getenv("BUDGET_USD_TO_PLN_RATE", "4.00")
BUDGET_FX_RATE_SSM_PARAM = os.getenv("BUDGET_FX_RATE_SSM_PARAM", "/comarch-baselinker-sync/usd-pln-rate")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "")


def _json_response(status_code: int, payload: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json; charset=utf-8",
            "Cache-Control": "no-store",
        },
        "body": json.dumps(payload, ensure_ascii=False),
    }


def _html_response(status_code: int, body: str) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-store",
        },
        "body": body,
    }


def _favicon_response() -> dict:
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
  <rect width="64" height="64" rx="12" fill="#eaf3ff"/>
  <text x="32" y="42" text-anchor="middle"
        font-family="Trebuchet MS, Arial, sans-serif"
        font-size="31" font-weight="800" fill="#165d9c">CB</text>
</svg>"""
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "image/svg+xml; charset=utf-8",
            "Cache-Control": "private, max-age=86400",
        },
        "body": svg,
    }


def _unauthorized() -> dict:
    return {
        "statusCode": 401,
        "headers": {
            "WWW-Authenticate": 'Basic realm="Comarch BaseLinker Sync Admin"',
            "Content-Type": "text/plain; charset=utf-8",
            "Cache-Control": "no-store",
        },
        "body": "Unauthorized",
    }


def _headers(event: dict) -> dict:
    return {str(k).lower(): str(v) for k, v in (event.get("headers") or {}).items()}


def _is_authorized(event: dict) -> bool:
    expected_hash = ADMIN_PASSWORD_SHA256.strip().lower()
    if expected_hash == "":
        return False

    auth = _headers(event).get("authorization", "")
    if not auth.lower().startswith("basic "):
        return False
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
        username, password = decoded.split(":", 1)
    except Exception:
        return False

    if not hmac.compare_digest(username, ADMIN_USERNAME):
        return False
    actual_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
    return hmac.compare_digest(actual_hash, expected_hash)


def _request_info(event: dict) -> tuple[str, str]:
    http = (event.get("requestContext") or {}).get("http") or {}
    method = str(http.get("method") or event.get("httpMethod") or "GET").upper()
    path = str(event.get("rawPath") or event.get("path") or "/")
    return method, path.rstrip("/") or "/"


def _load_status() -> dict:
    try:
        raw = ssm.get_parameter(Name=STATUS_PARAM, WithDecryption=False)["Parameter"]["Value"]
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception as exc:
        return {"status": "unknown", "error": f"{type(exc).__name__}: {exc}"}
    return {"status": "unknown"}


def _next_midnight_pl() -> str:
    now = datetime.now(POLAND_TZ)
    candidate = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate.isoformat()


def _load_schedule() -> dict:
    out = {
        "name": SCHEDULE_NAME,
        "group": SCHEDULE_GROUP,
        "next_run_iso": _next_midnight_pl(),
    }
    try:
        data = scheduler.get_schedule(Name=SCHEDULE_NAME, GroupName=SCHEDULE_GROUP)
        out.update(
            {
                "state": data.get("State"),
                "expression": data.get("ScheduleExpression"),
                "timezone": data.get("ScheduleExpressionTimezone"),
            }
        )
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _parse_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _parse_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip().replace(",", "."))
    except Exception:
        return default


def _first_dict(*items: object) -> dict:
    for item in items:
        if isinstance(item, dict):
            return item
    return {}


def _load_usd_to_pln_rate() -> dict:
    fallback_rate = max(0.0, _parse_float(BUDGET_USD_TO_PLN_RATE, 4.0))
    fallback = {
        "rate": fallback_rate,
        "source": "fallback_env",
        "effective_date": "",
        "fetched_at_iso": "",
        "error": "",
    }
    if BUDGET_FX_RATE_SSM_PARAM.strip() == "":
        fallback["error"] = "Brak nazwy parametru SSM z kursem USD/PLN."
        return fallback
    try:
        raw = ssm.get_parameter(Name=BUDGET_FX_RATE_SSM_PARAM, WithDecryption=False)[
            "Parameter"
        ]["Value"]
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("SSM FX parameter is not a JSON object.")
        rate = _parse_float(payload.get("rate"), 0.0)
        if rate <= 0:
            raise ValueError("SSM FX parameter does not contain a positive rate.")
        return {
            "rate": rate,
            "source": _clean(payload.get("source")) or "ssm",
            "effective_date": _clean(payload.get("effective_date")),
            "fetched_at_iso": _clean(payload.get("fetched_at_iso")),
            "error": _clean(payload.get("error")),
        }
    except Exception as exc:
        fallback["error"] = f"{type(exc).__name__}: {exc}"
        return fallback


def _load_budget_status() -> dict:
    limit_default = max(0.0, _parse_float(BUDGET_LIMIT_USD, 30.0))
    fx = _load_usd_to_pln_rate()
    usd_to_pln_rate = max(0.0, _parse_float(fx.get("rate"), 4.0))
    out = {
        "name": BUDGET_NAME,
        "limit_usd": round(limit_default, 2),
        "spent_usd": 0.0,
        "remaining_usd": round(limit_default, 2),
        "limit_pln": round(limit_default * usd_to_pln_rate, 2),
        "spent_pln": 0.0,
        "remaining_pln": round(limit_default * usd_to_pln_rate, 2),
        "percent_used": 0.0,
        "currency": "PLN",
        "source_currency": "USD",
        "display_currency": "PLN",
        "usd_to_pln_rate": round(usd_to_pln_rate, 4),
        "usd_to_pln_source": fx.get("source", ""),
        "usd_to_pln_effective_date": fx.get("effective_date", ""),
        "usd_to_pln_fetched_at_iso": fx.get("fetched_at_iso", ""),
        "usd_to_pln_error": fx.get("error", ""),
        "source": "aws_budgets",
        "error": "",
    }
    if AWS_ACCOUNT_ID.strip() == "":
        out["error"] = "Brak AWS_ACCOUNT_ID w konfiguracji panelu."
        return out
    try:
        data = budgets.describe_budget(AccountId=AWS_ACCOUNT_ID, BudgetName=BUDGET_NAME)
        budget = data.get("Budget", {}) if isinstance(data, dict) else {}
        limit = budget.get("BudgetLimit", {}) if isinstance(budget.get("BudgetLimit"), dict) else {}
        spend = (
            budget.get("CalculatedSpend", {}).get("ActualSpend", {})
            if isinstance(budget.get("CalculatedSpend"), dict)
            else {}
        )
        limit_usd = _parse_float(limit.get("Amount"), limit_default)
        spent_usd = _parse_float(spend.get("Amount"), 0.0)
        remaining_usd = max(0.0, limit_usd - spent_usd)
        source_currency = _clean(spend.get("Unit") or limit.get("Unit") or "USD") or "USD"
        out.update(
            {
                "limit_usd": round(limit_usd, 2),
                "spent_usd": round(spent_usd, 4),
                "remaining_usd": round(remaining_usd, 4),
                "limit_pln": round(limit_usd * usd_to_pln_rate, 2),
                "spent_pln": round(spent_usd * usd_to_pln_rate, 2),
                "remaining_pln": round(remaining_usd * usd_to_pln_rate, 2),
                "percent_used": round((spent_usd * 100.0 / limit_usd), 2)
                if limit_usd > 0
                else 0.0,
                "currency": "PLN",
                "source_currency": source_currency,
                "display_currency": "PLN",
                "usd_to_pln_rate": round(usd_to_pln_rate, 4),
                "usd_to_pln_source": fx.get("source", ""),
                "usd_to_pln_effective_date": fx.get("effective_date", ""),
                "usd_to_pln_fetched_at_iso": fx.get("fetched_at_iso", ""),
                "usd_to_pln_error": fx.get("error", ""),
                "updated_at_iso": datetime.now(POLAND_TZ).isoformat(),
            }
        )
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _default_sync_config() -> dict:
    return {
        "comarch_xml_url": DEFAULT_COMARCH_XML_URL,
        "bl_inventory_id": _parse_int(DEFAULT_BL_INVENTORY_ID, 0),
        "bl_inventory_name": "",
        "bl_warehouse_id": DEFAULT_BL_WAREHOUSE_ID,
        "bl_warehouse_name": "",
        "bl_api_max_rpm": max(1, min(100, _parse_int(DEFAULT_BL_API_MAX_RPM, 90))),
        "source": "lambda_env_defaults",
    }


def _load_sync_config() -> dict:
    config = _default_sync_config()
    if SYNC_CONFIG_PARAM == "":
        return config
    try:
        raw = ssm.get_parameter(Name=SYNC_CONFIG_PARAM, WithDecryption=True)["Parameter"]["Value"]
        parsed = json.loads(raw)
    except Exception:
        return config
    if not isinstance(parsed, dict):
        return config
    if _clean(parsed.get("comarch_xml_url")):
        config["comarch_xml_url"] = _clean(parsed.get("comarch_xml_url"))
    inv_id = _parse_int(parsed.get("bl_inventory_id"), 0)
    if inv_id > 0:
        config["bl_inventory_id"] = inv_id
    if _clean(parsed.get("bl_inventory_name")):
        config["bl_inventory_name"] = _clean(parsed.get("bl_inventory_name"))
    if _clean(parsed.get("bl_warehouse_id")):
        config["bl_warehouse_id"] = _clean(parsed.get("bl_warehouse_id"))
    if _clean(parsed.get("bl_warehouse_name")):
        config["bl_warehouse_name"] = _clean(parsed.get("bl_warehouse_name"))
    rpm = _parse_int(parsed.get("bl_api_max_rpm"), 0)
    if rpm > 0:
        config["bl_api_max_rpm"] = max(1, min(100, rpm))
    config["source"] = "ssm_parameter"
    return config


def _save_sync_config(config: dict) -> None:
    payload = {
        "comarch_xml_url": _clean(config.get("comarch_xml_url")),
        "bl_inventory_id": int(config.get("bl_inventory_id") or 0),
        "bl_inventory_name": _clean(config.get("bl_inventory_name")),
        "bl_warehouse_id": _clean(config.get("bl_warehouse_id")),
        "bl_warehouse_name": _clean(config.get("bl_warehouse_name")),
        "bl_api_max_rpm": int(config.get("bl_api_max_rpm") or 90),
        "updated_at_iso": datetime.now(timezone.utc).isoformat(),
        "updated_by": "admin_panel",
    }
    ssm.put_parameter(
        Name=SYNC_CONFIG_PARAM,
        Value=json.dumps(payload, ensure_ascii=False),
        Type="String",
        Overwrite=True,
        Tier="Standard",
    )


def _get_bl_api_token() -> str:
    raw = ssm.get_parameter(Name=BL_API_TOKEN_SSM_PARAM, WithDecryption=True)["Parameter"]["Value"]
    token = _clean(raw)
    if token == "":
        raise RuntimeError("BL API token is empty.")
    return token


def _bl_api_call(method: str, parameters: Optional[dict] = None, timeout_sec: int = 30) -> dict:
    payload = urllib.parse.urlencode(
        {
            "method": method,
            "parameters": json.dumps(parameters or {}, ensure_ascii=False),
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        BL_API_URL,
        data=payload,
        headers={
            "X-BLToken": _get_bl_api_token(),
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "User-Agent": "comarch-baselinker-sync-admin/1.0",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"{method}: malformed response")
    if data.get("status") != "SUCCESS":
        raise RuntimeError(f"{method}: {data.get('error_message', 'unknown error')}")
    return data


def _name_from_row(row: dict, fallback: str) -> str:
    for key in ("name", "inventory_name", "warehouse_name", "description"):
        value = _clean(row.get(key))
        if value:
            return value
    return fallback


def _load_bl_options() -> dict:
    inventories_data = _bl_api_call("getInventories")
    warehouses_data = _bl_api_call("getInventoryWarehouses")

    inventories = []
    for row in inventories_data.get("inventories", []):
        if not isinstance(row, dict):
            continue
        inv_id = _parse_int(row.get("inventory_id"), 0)
        if inv_id <= 0:
            continue
        raw_warehouses = row.get("warehouses", [])
        warehouse_ids = []
        if isinstance(raw_warehouses, list):
            warehouse_ids = [_clean(item) for item in raw_warehouses if _clean(item)]
        inventories.append(
            {
                "id": inv_id,
                "name": _name_from_row(row, f"Katalog {inv_id}"),
                "warehouse_ids": warehouse_ids,
            }
        )

    referenced_warehouse_ids = []
    for inventory in inventories:
        for warehouse_id in inventory.get("warehouse_ids", []):
            if warehouse_id and warehouse_id not in referenced_warehouse_ids:
                referenced_warehouse_ids.append(warehouse_id)

    warehouse_name_by_raw_id = {}
    raw_warehouses = warehouses_data.get("warehouses", [])
    rows = []
    if isinstance(raw_warehouses, dict):
        rows = [
            {**value, "warehouse_id": key} if isinstance(value, dict) else {"warehouse_id": key, "name": value}
            for key, value in raw_warehouses.items()
        ]
    elif isinstance(raw_warehouses, list):
        rows = raw_warehouses
    for row in rows:
        if not isinstance(row, dict):
            continue
        warehouse_id = _clean(row.get("warehouse_id") or row.get("id"))
        if warehouse_id == "":
            continue
        warehouse_name_by_raw_id[warehouse_id] = _name_from_row(row, f"Magazyn {warehouse_id}")

    warehouses = []
    used_warehouse_ids = set()
    for warehouse_id in referenced_warehouse_ids:
        raw_id = warehouse_id.split("_", 1)[1] if "_" in warehouse_id else warehouse_id
        name = (
            warehouse_name_by_raw_id.get(warehouse_id)
            or warehouse_name_by_raw_id.get(raw_id)
            or f"Magazyn {warehouse_id}"
        )
        warehouses.append({"id": warehouse_id, "name": name})
        used_warehouse_ids.add(warehouse_id)

    for raw_id, name in warehouse_name_by_raw_id.items():
        prefixed_candidates = {raw_id, f"bl_{raw_id}", f"shop_{raw_id}"}
        if prefixed_candidates & used_warehouse_ids:
            continue
        warehouses.append({"id": raw_id, "name": name})

    inventories.sort(key=lambda item: item["name"].lower())
    warehouses.sort(key=lambda item: item["name"].lower())
    return {"inventories": inventories, "warehouses": warehouses}


def _config_payload() -> dict:
    config = _load_sync_config()
    options = _load_bl_options()
    return {
        "config": config,
        "defaults": _default_sync_config(),
        "options": options,
    }


def _request_json(event: dict) -> dict:
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    if not isinstance(raw, str) or raw.strip() == "":
        return {}
    parsed = json.loads(raw)
    return parsed if isinstance(parsed, dict) else {}


def _validate_sync_config(config_raw: dict, options: dict) -> dict:
    comarch_url = _clean(config_raw.get("comarch_xml_url"))
    if not comarch_url.startswith("https://"):
        raise ValueError("Adres XML musi zaczynać się od https://")

    inventory_id = _parse_int(config_raw.get("bl_inventory_id"), 0)
    inventory_by_id = {int(item["id"]): item for item in options.get("inventories", [])}
    if inventory_id <= 0 or inventory_id not in inventory_by_id:
        raise ValueError("Wybierz poprawny katalog Baselinker.")
    inventory = inventory_by_id[inventory_id]

    warehouse_id = _clean(config_raw.get("bl_warehouse_id"))
    warehouse_by_id = {str(item["id"]): item for item in options.get("warehouses", [])}
    if warehouse_id == "" or warehouse_id not in warehouse_by_id:
        raise ValueError("Wybierz poprawny magazyn Baselinker.")
    allowed_warehouses = set(inventory.get("warehouse_ids") or [])
    if allowed_warehouses and warehouse_id not in allowed_warehouses:
        raise ValueError("Wybrany magazyn nie jest przypisany do wybranego katalogu.")

    rpm = _parse_int(config_raw.get("bl_api_max_rpm"), 90)
    if rpm < 1 or rpm > 100:
        raise ValueError("Limit zapytań musi być liczbą od 1 do 100.")

    return {
        "comarch_xml_url": comarch_url,
        "bl_inventory_id": inventory_id,
        "bl_inventory_name": inventory["name"],
        "bl_warehouse_id": warehouse_id,
        "bl_warehouse_name": warehouse_by_id[warehouse_id]["name"],
        "bl_api_max_rpm": rpm,
    }


def _summarize_status(status: dict) -> dict:
    current_status = _clean(status.get("status", ""))
    progress = status.get("progress") if isinstance(status.get("progress"), dict) else {}
    result = status.get("result") if isinstance(status.get("result"), dict) else {}
    running_without_progress = current_status == "running" and not progress
    usable_result = {} if running_without_progress else result
    usable_status = {} if running_without_progress else status
    source = progress if progress else usable_result
    pre_audit_summary = _first_dict(
        source.get("pre_audit_summary"),
        usable_result.get("pre_audit_summary"),
        progress.get("pre_audit_summary"),
        usable_status.get("pre_audit_summary"),
    )
    post_audit_summary = _first_dict(
        source.get("post_audit_summary"),
        usable_result.get("post_audit_summary"),
        progress.get("post_audit_summary"),
        usable_status.get("post_audit_summary"),
    )

    total = int(source.get("global_total_records") or 0)
    processed = int(source.get("global_processed") or 0)
    requested = int(source.get("global_requested") or 0)
    updated = int(source.get("global_updated") or 0)
    delete_target_from_audit = int(pre_audit_summary.get("extra_in_bl") or 0)
    delete_target = int(source.get("global_delete_target") or 0) or delete_target_from_audit
    delete_requested = int(source.get("global_delete_requested") or 0)
    delete_deleted = int(source.get("global_delete_deleted") or 0)
    delete_failed = int(source.get("global_delete_failed") or 0)
    if (
        delete_deleted <= 0
        and delete_target > 0
        and _clean(status.get("status")) == "success"
        and post_audit_summary
        and int(post_audit_summary.get("diff_total") or 0) == 0
    ):
        delete_deleted = delete_target
    changed_target = int(source.get("global_changed_target") or 0)
    write_target = changed_target if changed_target > 0 else total
    mutation_total = max(0, write_target + delete_target)
    mutation_done = max(0, requested + delete_deleted)
    if mutation_total > 0:
        percent = round((min(mutation_done, mutation_total) * 100.0 / mutation_total), 2)
    else:
        percent = round((processed * 100.0 / total), 2) if total > 0 else 0.0
    audit_unchanged = int(
        pre_audit_summary.get("unchanged_records")
        or post_audit_summary.get("unchanged_records")
        or 0
    )
    skipped_unchanged = int(source.get("global_skipped_unchanged") or 0)
    sync_skipped_no_changes = bool(
        source.get("sync_skipped_no_changes")
        or usable_result.get("sync_skipped_no_changes")
        or usable_status.get("sync_skipped_no_changes")
    )
    post_audit_skipped_no_changes = bool(
        source.get("post_audit_skipped_no_changes")
        or usable_result.get("post_audit_skipped_no_changes")
        or usable_status.get("post_audit_skipped_no_changes")
    )
    pre_audit_executed = bool(
        source.get("pre_audit_executed")
        or usable_result.get("pre_audit_executed")
        or pre_audit_summary
    )
    post_audit_executed = bool(
        source.get("post_audit_executed")
        or usable_result.get("post_audit_executed")
        or post_audit_summary
    )
    phase = source.get("phase") or usable_result.get("phase") or usable_status.get("phase") or ""
    sync_stage = source.get("sync_stage") or usable_result.get("sync_stage") or usable_status.get("sync_stage") or ""
    if sync_stage == "":
        if str(phase).startswith("pre_audit"):
            sync_stage = "pre_audit"
        elif str(phase).startswith("post_audit"):
            sync_stage = "post_audit"
        elif current_status == "running":
            has_sync_activity = bool(
                pre_audit_executed
                or pre_audit_summary
                or requested > 0
                or updated > 0
                or delete_requested > 0
                or delete_deleted > 0
                or sync_skipped_no_changes
                or post_audit_executed
                or post_audit_summary
            )
            sync_stage = "sync" if has_sync_activity else "pre_audit"
        elif current_status == "success":
            sync_stage = "finished"
        else:
            sync_stage = "unknown"

    pre_diff = int(pre_audit_summary.get("diff_total") or 0)
    pre_missing = int(pre_audit_summary.get("missing_in_bl") or 0)
    pre_changed = int(pre_audit_summary.get("changed_records") or 0)
    pre_extra = int(pre_audit_summary.get("extra_in_bl") or 0)
    post_diff = int(post_audit_summary.get("diff_total") or 0)
    post_error = _clean(
        source.get("post_audit_error")
        or usable_result.get("post_audit_error")
        or usable_status.get("post_audit_error")
    )
    post_audit_has_inconsistencies = bool(post_audit_summary and post_diff > 0)
    needs_admin_attention = bool(post_error or post_audit_has_inconsistencies)
    admin_attention_message = ""
    if post_error:
        admin_attention_message = (
            "Kontrola po aktualizacji zakończyła się błędem. Powiadom administratora systemu, "
            "żeby sprawdził szczegóły, albo spróbuj uruchomić aktualizację jeszcze raz."
        )
    elif post_audit_has_inconsistencies:
        admin_attention_message = (
            f"Po aktualizacji wykryto {post_diff} niespójności. Powiadom administratora systemu, "
            "żeby sprawdził szczegóły, albo spróbuj uruchomić aktualizację jeszcze raz."
        )

    def _step_status(step: str) -> str:
        if step == "pre_audit":
            if _clean(source.get("pre_audit_error") or usable_result.get("pre_audit_error")):
                return "error"
            if sync_stage == "pre_audit" and current_status == "running":
                return "running"
            return "done" if pre_audit_executed else "waiting"
        if step == "sync":
            if sync_skipped_no_changes:
                return "skipped"
            if sync_stage == "sync" and current_status == "running":
                return "running"
            if sync_stage == "post_audit" and current_status == "running":
                return "done"
            if current_status in {"success", "error"} or post_audit_executed or post_audit_skipped_no_changes:
                return "done"
            return "waiting" if not pre_audit_executed else "ready"
        if step == "post_audit":
            if needs_admin_attention:
                return "error"
            if post_audit_skipped_no_changes:
                return "skipped"
            if sync_stage == "post_audit" and current_status == "running":
                return "running"
            return "done" if post_audit_executed else "waiting"
        return "waiting"

    pre_audit_step_status = _step_status("pre_audit")
    sync_step_status = _step_status("sync")
    post_audit_step_status = _step_status("post_audit")

    def _pre_audit_summary_text() -> str:
        if pre_audit_summary:
            return f"{pre_diff} różnic, {audit_unchanged} bez zmian"
        if pre_audit_step_status == "running":
            return "Sprawdzamy, co trzeba zmienić."
        return "Czeka na uruchomienie aktualizacji."

    def _sync_summary_text() -> str:
        if sync_skipped_no_changes:
            return "Pominięto, bo nie znaleziono różnic."
        delete_text = ""
        if delete_target > 0 or delete_deleted > 0:
            delete_text = f"; usunięto {delete_deleted} z {delete_target} zbędnych rekordów"
        if sync_step_status == "running":
            return f"Zapisano {updated} z {write_target} rekordów{delete_text}"
        if sync_step_status == "done":
            return f"Zapisano {updated} rekordów{delete_text}"
        if sync_step_status == "ready":
            return "Gotowe do rozpoczęcia po policzeniu różnic."
        return "Czeka na wynik liczenia różnic."

    def _post_audit_summary_text() -> str:
        if needs_admin_attention:
            return admin_attention_message
        if post_audit_skipped_no_changes:
            return "Pominięto, bo nie było żadnych zmian do wykonania."
        if post_audit_summary:
            return "Kontrola zakończona: brak niespójności."
        if post_audit_step_status == "running":
            return "Sprawdzamy, czy dane po aktualizacji są zgodne."
        return "Czeka na zakończenie aktualizacji."

    steps = [
        {
            "key": "pre_audit",
            "label": "Liczenie różnic przed aktualizacją",
            "status": pre_audit_step_status,
            "summary": _pre_audit_summary_text(),
        },
        {
            "key": "sync",
            "label": "Wprowadzanie zmian w Baselinkerze",
            "status": sync_step_status,
            "summary": _sync_summary_text(),
        },
        {
            "key": "post_audit",
            "label": "Kontrola po aktualizacji",
            "status": post_audit_step_status,
            "summary": _post_audit_summary_text(),
        },
    ]

    summary_lines = []
    if pre_audit_summary:
        summary_lines.append(f"{pre_diff} = liczba wykrytych problemów/rozjazdów w danych.")
        summary_lines.append(
            f"{pre_changed} = liczba rekordów, które trzeba utworzyć albo zaktualizować w BL."
        )
        summary_lines.append(
            f"{pre_missing} = część z tych {pre_changed}, które są nowe/brakujące w BL."
        )
        summary_lines.append(
            f"{pre_extra} = rekordy istniejące w BL, których nie powinno już być i trzeba je usunąć."
        )
        summary_lines.append(f"{audit_unchanged} = rekordy bez zmian.")
    if sync_skipped_no_changes:
        summary_lines.append("Nie wykonano synca, bo audyt przed aktualizacją nie wykazał różnic.")
    elif requested > 0 or updated > 0 or delete_deleted > 0:
        summary_lines.append(
            f"Sync: zapisano {updated} rekordów, usunięto {delete_deleted} zbędnych."
        )
    if post_audit_skipped_no_changes:
        summary_lines.append("Kontrola po aktualizacji została pominięta, bo nie było zmian do wykonania.")
    elif needs_admin_attention:
        summary_lines.append(admin_attention_message)
    elif post_audit_summary:
        summary_lines.append("Kontrola po aktualizacji: brak niespójności.")

    updated_unix = int(status.get("updated_at_unix") or 0)
    updated_age_sec = 0
    if updated_unix > 0:
        updated_age_sec = max(0, int(datetime.now(timezone.utc).timestamp()) - updated_unix)

    return {
        "status": status.get("status", "unknown"),
        "run_id": status.get("run_id", ""),
        "updated_at_iso": status.get("updated_at_iso", ""),
        "updated_age_sec": updated_age_sec,
        "phase": phase,
        "sync_stage": sync_stage,
        "global_total_records": total,
        "global_processed": processed,
        "progress_percent": percent,
        "global_requested": requested,
        "global_updated": updated,
        "global_skipped_unchanged": skipped_unchanged,
        "global_changed_target": changed_target,
        "global_write_target": write_target,
        "global_delete_target": delete_target,
        "global_delete_requested": delete_requested,
        "global_delete_deleted": delete_deleted,
        "global_delete_failed": delete_failed,
        "mutation_total": mutation_total,
        "mutation_done": mutation_done,
        "unchanged_records": audit_unchanged or skipped_unchanged,
        "unchanged_records_source": "pre_audit" if audit_unchanged else "runtime",
        "pre_audit_summary": pre_audit_summary,
        "post_audit_summary": post_audit_summary,
        "post_audit_diff_total": post_diff,
        "post_audit_error": post_error,
        "pre_audit_executed": pre_audit_executed,
        "sync_skipped_no_changes": sync_skipped_no_changes,
        "post_audit_executed": post_audit_executed,
        "post_audit_skipped_no_changes": post_audit_skipped_no_changes,
        "post_audit_has_inconsistencies": post_audit_has_inconsistencies,
        "needs_admin_attention": needs_admin_attention,
        "admin_attention_message": admin_attention_message,
        "steps": steps,
        "summary_lines": summary_lines,
        "global_errors_count": int(source.get("global_errors_count") or 0),
        "eta_finish_iso": source.get("eta_finish_iso") or usable_result.get("eta_finish_iso"),
        "message": status.get("message") or "",
    }


def _status_payload() -> dict:
    raw_status = _load_status()
    return {
        "sync": _summarize_status(raw_status),
        "raw_status": raw_status,
        "schedule": _load_schedule(),
        "budget": _load_budget_status(),
        "server_time_iso": datetime.now(POLAND_TZ).isoformat(),
    }


def _trigger_sync(event: dict) -> dict:
    current = _summarize_status(_load_status())
    if current.get("status") == "running" and int(current.get("updated_age_sec") or 0) < 3600:
        return _json_response(
            409,
            {
                "ok": False,
                "message": "Sync is already running.",
                "sync": current,
            },
        )

    try:
        body = _request_json(event)
        options = _load_bl_options()
        next_config = _validate_sync_config(
            body.get("config") if isinstance(body.get("config"), dict) else _load_sync_config(),
            options,
        )
        _save_sync_config(next_config)
    except ValueError as exc:
        return _json_response(400, {"ok": False, "message": str(exc)})
    except Exception as exc:
        return _json_response(
            500,
            {"ok": False, "message": f"Failed to save sync config: {type(exc).__name__}: {exc}"},
        )

    response = lambda_api.invoke(
        FunctionName=SYNC_FUNCTION_NAME,
        InvocationType="Event",
        Payload=json.dumps(
            {
                "source": "admin_panel",
                "reason": "manual_on_demand_sync",
                "sync_config_saved": True,
            },
            ensure_ascii=False,
        ).encode("utf-8"),
    )
    return _json_response(
        202,
        {
            "ok": True,
            "status_code": response.get("StatusCode"),
            "request_id": (response.get("ResponseMetadata") or {}).get("RequestId"),
            "message": "Sync queued.",
            "config": next_config,
        },
    )


def _page() -> str:
    return """<!doctype html>
<html lang="pl">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Comarch → BaseLinker Sync</title>
  <link rel="icon" href="/assets/favicon.svg" type="image/svg+xml">
  <style>
    :root {
      --orange: #1673b8;
      --orange-dark: #0f5d96;
      --navy: #183c5c;
      --danger: #b3261e;
      --ink: #263652;
      --muted: #6a6a6a;
      --line: #e8e2df;
      --panel: #ffffff;
      --soft: #f7f4f2;
      --good: #0d7a4f;
      --warn: #b66a00;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background: linear-gradient(180deg, #fff 0%, var(--soft) 100%);
      font-family: "Trebuchet MS", "Segoe UI", sans-serif;
    }
    .topbar { height: 5px; background: var(--orange); }
    main { max-width: 1120px; margin: 0 auto; padding: 28px 18px 42px; }
    header {
      display: flex;
      gap: 18px;
      align-items: center;
      justify-content: space-between;
      padding: 18px 0 24px;
      border-bottom: 1px solid var(--line);
    }
    .brand { display: flex; align-items: center; gap: 18px; }
    .brand-mark {
      display: grid;
      width: 52px;
      height: 52px;
      flex: 0 0 52px;
      place-items: center;
      border-radius: 14px;
      color: #fff;
      background: linear-gradient(145deg, #2698d8, var(--orange));
      box-shadow: 0 12px 28px rgba(22, 115, 184, .24);
      font-weight: 900;
      letter-spacing: -.04em;
    }
    h1 { margin: 0; font-size: 25px; line-height: 1.1; color: var(--navy); }
    .subtitle { margin-top: 5px; color: var(--muted); font-size: 14px; }
    .actions { display: flex; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
    button {
      appearance: none;
      border: 1px solid var(--ink);
      background: #fff;
      color: var(--ink);
      min-height: 42px;
      padding: 0 16px;
      font-weight: 700;
      cursor: pointer;
      border-radius: 2px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 9px;
    }
    button.primary { border-color: var(--orange); background: var(--orange); color: #fff; }
    button.primary:hover { background: var(--orange-dark); }
    button:disabled { opacity: .45; cursor: not-allowed; }
    .spinner {
      width: 16px;
      height: 16px;
      border: 2px solid currentColor;
      border-right-color: transparent;
      border-radius: 50%;
      display: none;
      animation: spin .8s linear infinite;
    }
    button.loading .spinner { display: inline-block; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 22px 0;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 4px;
      padding: 16px;
      min-height: 118px;
    }
    .panel.wide { grid-column: span 2; }
    .panel.config { grid-column: 1 / -1; }
    .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .08em;
      margin-bottom: 10px;
    }
    .value { font-size: 24px; font-weight: 800; overflow-wrap: anywhere; }
    .small { color: var(--muted); font-size: 13px; margin-top: 8px; overflow-wrap: anywhere; }
    .status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 24px;
      font-weight: 800;
    }
    .dot { width: 11px; height: 11px; border-radius: 999px; background: var(--muted); }
    .dot.running { background: var(--warn); }
    .dot.success { background: var(--good); }
    .dot.error { background: var(--danger); }
    .bar {
      width: 100%;
      height: 10px;
      background: #ece8e5;
      border-radius: 999px;
      overflow: hidden;
      margin-top: 14px;
    }
    .bar span { display: block; height: 100%; width: 0%; background: var(--orange); }
    .steps {
      display: grid;
      gap: 10px;
      margin-top: 8px;
    }
    .step {
      display: grid;
      grid-template-columns: 20px 1fr;
      gap: 10px;
      align-items: start;
      padding: 10px;
      border: 1px solid var(--line);
      background: #fffaf6;
      border-radius: 4px;
    }
    .step-mark {
      width: 20px;
      height: 20px;
      border-radius: 50%;
      border: 2px solid var(--muted);
      margin-top: 1px;
    }
    .step.running .step-mark {
      border-color: var(--orange);
      border-right-color: transparent;
      animation: spin .8s linear infinite;
    }
    .step.done .step-mark { border-color: var(--good); background: var(--good); }
    .step.skipped .step-mark { border-color: var(--muted); background: #f0ece8; }
    .step.error .step-mark { border-color: var(--danger); background: var(--danger); }
    .step.error {
      border-color: rgba(179, 38, 30, .45);
      background: #fff1ef;
      box-shadow: inset 3px 0 0 var(--danger);
    }
    .step.error .step-title { color: var(--danger); }
    .step.error .step-summary {
      color: #7d1b16;
      font-weight: 700;
    }
    .step-title { font-weight: 800; color: var(--navy); }
    .step-summary { margin-top: 3px; color: var(--muted); font-size: 13px; }
    .attention-alert {
      margin-top: 12px;
      padding: 11px 12px;
      border: 1px solid rgba(179, 38, 30, .45);
      background: #fff1ef;
      color: #7d1b16;
      border-radius: 4px;
      font-size: 13px;
      font-weight: 700;
      line-height: 1.4;
    }
    .attention-alert.hidden { display: none; }
    .summary-list {
      margin-top: 12px;
      padding-top: 10px;
      border-top: 1px solid var(--line);
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    .budget-ok { color: var(--good); }
    .budget-warn { color: var(--warn); }
    .budget-danger { color: var(--danger); }
    .config-grid {
      display: grid;
      grid-template-columns: 2fr 1fr 1fr 140px;
      gap: 12px;
      align-items: end;
    }
    .field label {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
      font-weight: 700;
    }
    input, select {
      width: 100%;
      min-height: 42px;
      border: 1px solid var(--line);
      border-radius: 2px;
      background: #fff;
      color: var(--ink);
      padding: 0 11px;
      font: inherit;
    }
    input:focus, select:focus {
      outline: 2px solid rgba(216, 111, 32, .22);
      border-color: var(--orange);
    }
    .config-note { margin-top: 12px; color: var(--muted); font-size: 13px; }
    .config-note strong { color: var(--ink); }
    .foot { margin-top: 18px; color: var(--muted); font-size: 12px; }
    @media (max-width: 760px) {
      header { align-items: flex-start; flex-direction: column; }
      .actions { justify-content: flex-start; width: 100%; }
      button { width: 100%; }
      .grid { grid-template-columns: 1fr; }
      .panel.wide { grid-column: span 1; }
      .panel.config { grid-column: span 1; }
      .config-grid { grid-template-columns: 1fr; }
      h1 { font-size: 22px; }
      .brand-mark { width: 46px; height: 46px; flex-basis: 46px; }
    }
  </style>
</head>
<body>
  <div class="topbar"></div>
  <main>
    <header>
      <div class="brand">
        <div class="brand-mark" aria-hidden="true">CB</div>
        <div>
          <h1>Comarch → BaseLinker Sync</h1>
          <div class="subtitle">Podgląd i ręczne uruchamianie synchronizacji produktów</div>
        </div>
      </div>
      <div class="actions">
        <button id="refreshBtn"><span class="spinner"></span><span class="btnText">Odśwież dane</span></button>
        <button id="syncBtn" class="primary"><span class="spinner"></span><span class="btnText">Uruchom aktualizację</span></button>
      </div>
    </header>

    <section class="grid">
      <div class="panel">
        <div class="label">Stan</div>
        <div class="status"><span id="statusDot" class="dot"></span><span id="statusText">...</span></div>
        <div id="message" class="small"></div>
        <div id="attentionAlert" class="attention-alert hidden"></div>
      </div>
      <div class="panel">
        <div class="label">Postęp</div>
        <div id="progressText" class="value">0%</div>
        <div class="bar"><span id="progressBar"></span></div>
        <div id="recordsText" class="small"></div>
      </div>
      <div class="panel">
        <div class="label">Przewidywany koniec</div>
        <div id="etaText" class="value">-</div>
        <div id="updatedText" class="small"></div>
      </div>
      <div class="panel">
        <div class="label">Następne uruchomienie</div>
        <div id="nextRunText" class="value">-</div>
        <div id="scheduleText" class="small"></div>
      </div>
      <div class="panel">
        <div class="label">Koszt w tym miesiącu</div>
        <div id="budgetText" class="value">-</div>
        <div id="budgetSmall" class="small"></div>
      </div>
      <div class="panel wide">
        <div class="label">Co zostało zrobione</div>
        <div id="countersText" class="value">-</div>
        <div id="runText" class="small"></div>
      </div>
      <div class="panel wide">
        <div class="label">Etapy aktualizacji</div>
        <div id="stepsList" class="steps"></div>
        <div id="summaryText" class="summary-list"></div>
      </div>
      <div class="panel config">
        <div class="label">Ustawienia aktualizacji</div>
        <div class="config-grid">
          <div class="field">
            <label for="comarchUrlInput">Link do XML z Comarch e-Sklep</label>
            <input id="comarchUrlInput" type="url" placeholder="https://...">
          </div>
          <div class="field">
            <label for="inventorySelect">Katalog Baselinker</label>
            <select id="inventorySelect"></select>
          </div>
          <div class="field">
            <label for="warehouseSelect">Magazyn Baselinker</label>
            <select id="warehouseSelect"></select>
          </div>
          <div class="field">
            <label for="rpmInput">Zapytań / min</label>
            <input id="rpmInput" type="number" min="1" max="100" step="1">
          </div>
        </div>
        <div id="configNote" class="config-note">
          Zmiany nie zapisują się podczas edycji. <strong>Zostaną użyte i zapamiętane dopiero po kliknięciu „Uruchom aktualizację”.</strong>
        </div>
      </div>
    </section>
    <div class="foot">Strona odświeża dane co minutę, gdy aktualizacja jest w toku.</div>
  </main>
  <script>
    const statusText = document.getElementById('statusText');
    const statusDot = document.getElementById('statusDot');
    const progressText = document.getElementById('progressText');
    const progressBar = document.getElementById('progressBar');
    const recordsText = document.getElementById('recordsText');
    const etaText = document.getElementById('etaText');
    const updatedText = document.getElementById('updatedText');
    const nextRunText = document.getElementById('nextRunText');
    const scheduleText = document.getElementById('scheduleText');
    const budgetText = document.getElementById('budgetText');
    const budgetSmall = document.getElementById('budgetSmall');
    const countersText = document.getElementById('countersText');
    const runText = document.getElementById('runText');
    const stepsList = document.getElementById('stepsList');
    const summaryText = document.getElementById('summaryText');
    const comarchUrlInput = document.getElementById('comarchUrlInput');
    const inventorySelect = document.getElementById('inventorySelect');
    const warehouseSelect = document.getElementById('warehouseSelect');
    const rpmInput = document.getElementById('rpmInput');
    const configNote = document.getElementById('configNote');
    const message = document.getElementById('message');
    const attentionAlert = document.getElementById('attentionAlert');
    const syncBtn = document.getElementById('syncBtn');
    const refreshBtn = document.getElementById('refreshBtn');
    let timer = null;
    let isTriggering = false;
    let isRefreshing = false;
    let isConfigLoading = true;
    let triggerLockUntil = 0;
    let forcePollingUntil = 0;
    let configOptions = { inventories: [], warehouses: [] };
    let lastSync = {};

    function fmt(value) {
      if (!value) return '-';
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString('pl-PL');
    }

    function statusLabel(status) {
      const map = {
        running: 'W toku',
        success: 'Zakończona',
        error: 'Błąd',
        unknown: 'Nieznany'
      };
      return map[status] || 'Nieznany';
    }

    function messageLabel(text) {
      const raw = String(text || '').trim();
      if (!raw) return '';
      const map = {
        'Sync started.': 'Aktualizacja została uruchomiona.',
        'Sync in progress.': 'Aktualizacja produktów jest w toku.',
        'Sync finished.': 'Aktualizacja zakończona.',
        'Sync queued.': 'Aktualizacja została dodana do kolejki.',
        'Sync is already running.': 'Aktualizacja już trwa.',
        'Sync skipped; pre-audit diff_total=0.': 'Nie było żadnych zmian do wykonania.',
        'Not found': 'Nie znaleziono takiej strony lub akcji.',
        'Unauthorized': 'Brak dostępu. Zaloguj się ponownie.'
      };
      if (map[raw]) return map[raw];

      const lower = raw.toLowerCase();
      if (lower.includes('accessdenied') || lower.includes('not authorized') || lower.includes('unauthorized')) {
        return 'Brak uprawnień do wykonania tej operacji. Powiadom administratora systemu.';
      }
      if (lower.includes('expiredtoken') || lower.includes('security token') || lower.includes('invalidclienttokenid')) {
        return 'Poświadczenia AWS są nieaktywne albo wygasły. Powiadom administratora systemu.';
      }
      if (lower.includes('throttl') || lower.includes('too many requests') || lower.includes('rate limit') || lower.includes('429')) {
        return 'Usługa zewnętrzna ograniczyła liczbę zapytań. Spróbuj ponownie za chwilę.';
      }
      if (lower.includes('timeout') || lower.includes('timed out')) {
        return 'Operacja trwała zbyt długo. Spróbuj ponownie za chwilę.';
      }
      if (lower.includes('failed to fetch') || lower.includes('networkerror') || lower.includes('could not connect') || lower.includes('endpointconnection') || lower.includes('connection')) {
        return 'Nie udało się połączyć z usługą zewnętrzną. Spróbuj ponownie za chwilę.';
      }
      if (lower.includes('bl api token') || lower.includes('bltoken') || lower.includes('api token')) {
        return 'Brak poprawnie skonfigurowanego tokenu Baselinker. Powiadom administratora systemu.';
      }
      if (lower.includes('baselinker') || lower.includes('getinventories') || lower.includes('getinventorywarehouses')) {
        return 'BaseLinker zwrócił błąd podczas pobierania danych. Spróbuj ponownie albo powiadom administratora systemu.';
      }
      if (lower.includes('failed to save sync config')) {
        return 'Nie udało się zapisać ustawień aktualizacji. Spróbuj ponownie albo powiadom administratora systemu.';
      }
      if (lower.includes('failed to load sync config')) {
        return 'Nie udało się pobrać ustawień aktualizacji. Spróbuj ponownie albo powiadom administratora systemu.';
      }
      if (lower.includes('malformed') || lower.includes('json') || lower.includes('decode')) {
        return 'Usługa zwróciła odpowiedź w nieoczekiwanym formacie. Powiadom administratora systemu.';
      }

      const polishPrefixes = ['Aktualizacja ', 'Adres ', 'Brak ', 'Kontrola ', 'Limit ', 'Nie ', 'Powiadom ', 'Wybierz ', 'Wybrany ', 'Wystąpił '];
      if (/[ąćęłńóśźż]/i.test(raw) || polishPrefixes.some((prefix) => raw.startsWith(prefix))) {
        return raw;
      }
      return 'Wystąpił problem techniczny. Spróbuj ponownie albo powiadom administratora systemu.';
    }

    function statusTextForStep(status) {
      const map = {
        waiting: 'czeka',
        ready: 'gotowe do startu',
        running: 'w toku',
        done: 'zakończone',
        skipped: 'pominięte',
        error: 'wymaga sprawdzenia'
      };
      return map[status] || status || 'czeka';
    }

    function renderAttention(sync) {
      const text = sync && sync.needs_admin_attention ? sync.admin_attention_message : '';
      if (text) {
        attentionAlert.textContent = text;
        attentionAlert.classList.remove('hidden');
      } else {
        attentionAlert.textContent = '';
        attentionAlert.classList.add('hidden');
      }
    }

    function renderSteps(steps, summaryLines) {
      const safeSteps = Array.isArray(steps) && steps.length ? steps : [
        { label: 'Liczenie różnic przed aktualizacją', status: 'waiting', summary: 'Czeka na uruchomienie.' },
        { label: 'Wprowadzanie zmian w Baselinkerze', status: 'waiting', summary: 'Czeka na wynik liczenia różnic.' },
        { label: 'Kontrola po aktualizacji', status: 'waiting', summary: 'Czeka na zakończenie zmian.' }
      ];
      stepsList.innerHTML = safeSteps.map((step) => `
        <div class="step ${step.status || 'waiting'}">
          <div class="step-mark" aria-hidden="true"></div>
          <div>
            <div class="step-title">${escapeHtml(step.label || '-')} · ${escapeHtml(statusTextForStep(step.status))}</div>
            <div class="step-summary">${escapeHtml(step.summary || '')}</div>
          </div>
        </div>
      `).join('');
      const lines = Array.isArray(summaryLines) ? summaryLines.filter(Boolean) : [];
      summaryText.innerHTML = lines.length
        ? lines.map((line) => `<div>${escapeHtml(line)}</div>`).join('')
        : 'Tu pojawi się krótkie podsumowanie po policzeniu różnic.';
    }

    function renderProgressDetails(sync) {
      const stage = sync.sync_stage || '';
      if (stage === 'pre_audit') {
        return 'Liczymy różnice przed aktualizacją. To może potrwać kilka minut.';
      }
      if (stage === 'post_audit') {
        return 'Sprawdzamy zgodność danych po aktualizacji.';
      }
      if (sync.sync_skipped_no_changes) {
        return 'Nie było żadnych zmian do wykonania.';
      }
      const writeTarget = Number(sync.global_write_target || sync.global_total_records || 0);
      const written = Number(sync.global_updated || sync.global_requested || sync.global_processed || 0);
      const deleteTarget = Number(sync.global_delete_target || 0);
      const deleted = Number(sync.global_delete_deleted || 0);
      const mutationTotal = Number(sync.mutation_total || (writeTarget + deleteTarget));
      const mutationDone = Number(sync.mutation_done || (written + deleted));
      if (mutationTotal > 0 || mutationDone > 0) {
        const doneText = sync.status === 'running'
          ? `Wykonano ${mutationDone} z ${mutationTotal} zmian`
          : `Wykonano ${mutationDone} zmian`;
        return `${doneText}: zapisano ${written} rekordów, usunięto ${deleted} zbędnych.`;
      }
      return 'Brak aktywnej aktualizacji.';
    }

    function renderCounters(sync) {
      const pre = sync.pre_audit_summary || {};
      const diffTotal = Number(pre.diff_total || 0);
      const changed = Number(pre.changed_records || sync.global_write_target || 0);
      const missing = Number(pre.missing_in_bl || 0);
      const extra = Number(pre.extra_in_bl || sync.global_delete_target || 0);
      const unchanged = Number(sync.unchanged_records || 0);
      if (diffTotal > 0 || changed > 0 || missing > 0 || extra > 0 || unchanged > 0) {
        countersText.textContent = `${diffTotal} rozjazdów / ${changed} do utworzenia albo aktualizacji / ${missing} nowych lub brakujących / ${extra} do usunięcia / ${unchanged} bez zmian`;
      } else if (sync.status === 'running' && sync.sync_stage === 'pre_audit') {
        countersText.textContent = 'Liczymy różnice przed aktualizacją.';
      } else {
        countersText.textContent = '-';
      }
    }

    function renderQueuedStart() {
      const optimistic = {
        status: 'running',
        sync_stage: 'pre_audit',
        updated_age_sec: 0,
        progress_percent: 0,
        message: 'Sync queued.',
        steps: [
          { label: 'Liczenie różnic przed aktualizacją', status: 'running', summary: 'Uruchamiamy liczenie różnic przed aktualizacją.' },
          { label: 'Wprowadzanie zmian w Baselinkerze', status: 'waiting', summary: 'Czeka na wynik liczenia różnic.' },
          { label: 'Kontrola po aktualizacji', status: 'waiting', summary: 'Czeka na zakończenie aktualizacji.' }
        ],
        summary_lines: ['Aktualizacja została uruchomiona. Najpierw policzymy różnice między Comarch e-Sklep a Baselinkerem.']
      };
      lastSync = optimistic;
      statusText.textContent = statusLabel(optimistic.status);
      statusDot.className = 'dot running';
      message.textContent = messageLabel(optimistic.message);
      renderAttention(optimistic);
      progressText.textContent = '0.00%';
      progressBar.style.width = '0%';
      recordsText.textContent = renderProgressDetails(optimistic);
      etaText.textContent = '-';
      updatedText.textContent = 'Oczekujemy na pierwszy zapis statusu z AWS.';
      renderSteps(optimistic.steps, optimistic.summary_lines);
      renderCounters(optimistic);
      runText.textContent = '';
      updateSyncButton(optimistic);
      scheduleNextRefresh(optimistic);
    }

    function escapeHtml(value) {
      return String(value || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
    }

    function renderBudget(budget) {
      const data = budget || {};
      const spent = Number(data.spent_pln || 0);
      const limit = Number(data.limit_pln || 0);
      const spentWhole = Math.floor(Math.max(0, spent));
      const limitWhole = Math.floor(Math.max(0, limit));
      const remainingWhole = Math.max(0, limitWhole - spentWhole);
      const percent = Number(data.percent_used || 0);
      const displayPercent = limitWhole > 0 ? (spentWhole * 100 / limitWhole) : percent;
      const currency = data.display_currency || data.currency || 'PLN';
      const currencyLabel = currency === 'PLN' ? 'zł' : currency;
      const rate = Number(data.usd_to_pln_rate || 0);
      budgetText.textContent = `${spentWhole} / ${limitWhole} ${currencyLabel}`;
      budgetText.className = `value ${displayPercent >= 100 ? 'budget-danger' : displayPercent >= 80 ? 'budget-warn' : 'budget-ok'}`;
      if (data.error) {
        budgetSmall.textContent = `Nie udało się pobrać budżetu: ${messageLabel(data.error)}`;
      } else {
        const fxSource = data.usd_to_pln_source || '';
        const fxDate = data.usd_to_pln_effective_date || '';
        const fxFetchedAt = data.usd_to_pln_fetched_at_iso || '';
        let rateText = '';
        if (rate > 0 && fxSource === 'nbp') {
          rateText = ` Kurs z ostatniego uruchomienia synca: NBP${fxDate ? ` z ${fxDate}` : ''}, 1 USD = ${rate.toFixed(2)} PLN.`;
        } else if (rate > 0) {
          rateText = ` Kurs awaryjny${fxFetchedAt ? ` zapisany ${fmt(fxFetchedAt)}` : ''}: 1 USD = ${rate.toFixed(2)} PLN.`;
        }
        budgetSmall.textContent = `${displayPercent.toFixed(1)}% miesięcznego limitu. Pozostało ${remainingWhole} ${currencyLabel}.${rateText}`;
      }
    }

    function setButtonLoading(button, loading, label) {
      const text = button.querySelector('.btnText');
      button.classList.toggle('loading', loading);
      if (text) text.textContent = label;
    }

    function optionLabel(item) {
      return item.name ? `${item.name}` : `${item.id}`;
    }

    function fillSelect(select, items, selectedValue, emptyLabel) {
      select.innerHTML = '';
      if (!items.length) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = emptyLabel;
        select.appendChild(option);
        return;
      }
      items.forEach((item) => {
        const option = document.createElement('option');
        option.value = String(item.id);
        option.textContent = optionLabel(item);
        select.appendChild(option);
      });
      select.value = String(selectedValue || items[0].id);
      if (select.value === '' && selectedValue) {
        const option = document.createElement('option');
        option.value = String(selectedValue);
        option.textContent = String(selectedValue);
        select.appendChild(option);
        select.value = String(selectedValue);
      }
    }

    function selectedInventory() {
      const id = Number(inventorySelect.value || 0);
      return configOptions.inventories.find((item) => Number(item.id) === id) || null;
    }

    function refreshWarehouseOptions(selectedValue) {
      const inventory = selectedInventory();
      let allowed = configOptions.warehouses;
      if (inventory && Array.isArray(inventory.warehouse_ids) && inventory.warehouse_ids.length) {
        const allowedIds = new Set(inventory.warehouse_ids.map(String));
        allowed = configOptions.warehouses.filter((item) => allowedIds.has(String(item.id)));
      }
      fillSelect(warehouseSelect, allowed, selectedValue || warehouseSelect.value, 'Brak magazynów');
    }

    async function loadConfig() {
      isConfigLoading = true;
      updateSyncButton(lastSync);
      try {
        const res = await fetch('/api/config', { credentials: 'include' });
        const data = await res.json();
        if (!res.ok) throw new Error(data.message || `Nie udało się pobrać ustawień (${res.status})`);
        const config = data.config || {};
        configOptions = data.options || { inventories: [], warehouses: [] };
        comarchUrlInput.value = config.comarch_xml_url || '';
        rpmInput.value = config.bl_api_max_rpm || 90;
        fillSelect(inventorySelect, configOptions.inventories || [], config.bl_inventory_id, 'Brak katalogów');
        refreshWarehouseOptions(config.bl_warehouse_id);
        configNote.innerHTML = 'Zmiany nie zapisują się podczas edycji. <strong>Zostaną użyte i zapamiętane dopiero po kliknięciu „Uruchom aktualizację”.</strong>';
      } catch (err) {
        configNote.textContent = messageLabel(err.message);
      } finally {
        isConfigLoading = false;
        updateSyncButton(lastSync);
      }
    }

    function collectConfig() {
      return {
        comarch_xml_url: comarchUrlInput.value.trim(),
        bl_inventory_id: Number(inventorySelect.value || 0),
        bl_warehouse_id: warehouseSelect.value,
        bl_api_max_rpm: Number(rpmInput.value || 90)
      };
    }

    function updateSyncButton(sync) {
      const running = sync.status === 'running' && (sync.updated_age_sec || 0) < 3600;
      const recentlyTriggered = Date.now() < triggerLockUntil;
      syncBtn.disabled = isTriggering || running || recentlyTriggered || isConfigLoading;
      if (isTriggering) {
        setButtonLoading(syncBtn, true, 'Uruchamianie...');
      } else if (isConfigLoading) {
        setButtonLoading(syncBtn, true, 'Ładowanie ustawień');
      } else if (running) {
        setButtonLoading(syncBtn, true, 'Aktualizacja trwa');
      } else if (recentlyTriggered) {
        setButtonLoading(syncBtn, true, 'Czekam na start');
      } else {
        setButtonLoading(syncBtn, false, 'Uruchom aktualizację');
      }
    }

    function scheduleNextRefresh(sync) {
      if (timer) clearTimeout(timer);
      const running = sync.status === 'running' && (sync.updated_age_sec || 0) < 3600;
      const forcePolling = Date.now() < forcePollingUntil;
        if (running || forcePolling) {
        timer = setTimeout(() => {
          loadStatus().catch((err) => {
            message.textContent = messageLabel(err.message);
          });
        }, 60000);
      }
    }

    async function loadStatus() {
      if (isRefreshing) return;
      isRefreshing = true;
      setButtonLoading(refreshBtn, true, 'Odświeżanie...');
      try {
        const res = await fetch('/api/status', { credentials: 'include' });
        if (!res.ok) throw new Error(`Nie udało się pobrać danych (${res.status})`);
        const data = await res.json();
        const sync = data.sync || {};
        lastSync = sync;
        const schedule = data.schedule || {};
        const budget = data.budget || {};
        const pct = Number(sync.progress_percent || 0);

        statusText.textContent = statusLabel(sync.status);
        statusDot.className = `dot ${sync.status || ''}`;
        message.textContent = messageLabel(sync.message);
        renderAttention(sync);
        progressText.textContent = `${pct.toFixed(2)}%`;
        progressBar.style.width = `${Math.max(0, Math.min(100, pct))}%`;
        recordsText.textContent = renderProgressDetails(sync);
        etaText.textContent = fmt(sync.eta_finish_iso);
        updatedText.textContent = `Ostatni zapis: ${fmt(sync.updated_at_iso)} (${sync.updated_age_sec || 0}s temu)`;
        nextRunText.textContent = fmt(schedule.next_run_iso);
        scheduleText.textContent = schedule.state === 'ENABLED' ? 'Harmonogram jest włączony' : 'Harmonogram nie jest włączony';
        renderBudget(budget);
        renderSteps(sync.steps, sync.summary_lines);
        renderCounters(sync);
        runText.textContent = sync.run_id ? `Numer uruchomienia: ${sync.run_id}` : '';
        updateSyncButton(sync);

        scheduleNextRefresh(sync);
      } finally {
        isRefreshing = false;
        setButtonLoading(refreshBtn, false, 'Odśwież dane');
      }
    }

    async function triggerSync() {
      if (isTriggering || syncBtn.disabled) return;
      isTriggering = true;
      syncBtn.disabled = true;
      setButtonLoading(syncBtn, true, 'Uruchamianie...');
      try {
        const res = await fetch('/api/sync', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ config: collectConfig() })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(messageLabel(data.message) || `Nie udało się uruchomić aktualizacji (${res.status})`);
        configNote.innerHTML = 'Ustawienia zostały zapisane i będą używane także przy kolejnych aktualizacjach.';
        triggerLockUntil = Date.now() + 30000;
        forcePollingUntil = Date.now() + 15 * 60 * 1000;
        renderQueuedStart();
        setTimeout(() => {
          loadStatus().catch((err) => {
            message.textContent = messageLabel(err.message);
          });
        }, 3000);
      } catch (err) {
        alert(messageLabel(err.message));
      } finally {
        isTriggering = false;
        updateSyncButton(lastSync);
      }
    }

    refreshBtn.addEventListener('click', () => loadStatus().catch((err) => {
      message.textContent = messageLabel(err.message);
      isRefreshing = false;
      setButtonLoading(refreshBtn, false, 'Odśwież dane');
    }));
    inventorySelect.addEventListener('change', () => refreshWarehouseOptions());
    syncBtn.addEventListener('click', triggerSync);
    loadConfig().catch((err) => {
      configNote.textContent = messageLabel(err.message);
    });
    loadStatus().catch((err) => {
      statusText.textContent = 'Błąd';
      message.textContent = messageLabel(err.message);
      isRefreshing = false;
      setButtonLoading(refreshBtn, false, 'Odśwież dane');
    });
  </script>
</body>
</html>"""


def lambda_handler(event, context):
    if not _is_authorized(event):
        return _unauthorized()

    method, path = _request_info(event)
    if method == "GET" and path in {"/", "/admin"}:
        return _html_response(200, _page())
    if method == "GET" and path == "/assets/favicon.svg":
        return _favicon_response()
    if method == "GET" and path == "/api/status":
        return _json_response(200, _status_payload())
    if method == "GET" and path == "/api/config":
        try:
            return _json_response(200, _config_payload())
        except Exception as exc:
            return _json_response(
                500,
                {"ok": False, "message": f"Failed to load sync config: {type(exc).__name__}: {exc}"},
            )
    if method == "POST" and path == "/api/sync":
        return _trigger_sync(event)
    return _json_response(404, {"ok": False, "message": "Not found"})
