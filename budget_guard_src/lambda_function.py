from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict

import boto3


lambda_api = boto3.client("lambda")
scheduler = boto3.client("scheduler")
ssm = boto3.client("ssm")
budgets = boto3.client("budgets", region_name="us-east-1")


SYNC_FUNCTION_NAME = os.getenv("SYNC_FUNCTION_NAME") or os.getenv(
    "TARGET_FUNCTION_NAME", "baselinker-sync"
)
SYNC_SCHEDULE_NAME = os.getenv("SYNC_SCHEDULE_NAME") or os.getenv(
    "SCHEDULE_NAME", "baselinker-sync-midnight"
)
SYNC_SCHEDULE_GROUP = os.getenv("SYNC_SCHEDULE_GROUP") or os.getenv("SCHEDULE_GROUP", "default")
CONTINUATION_QUEUE_ARN = os.getenv("CONTINUATION_QUEUE_ARN", "")
TARGET_RESERVED_CONCURRENCY = int(os.getenv("TARGET_RESERVED_CONCURRENCY", "1") or "1")
STATUS_PARAM = os.getenv("BUDGET_GUARD_STATUS_PARAM", "/baselinker-sync/budget-guard-status")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "")
BUDGET_NAME = os.getenv("BUDGET_NAME", "baselinker-sync-monthly-budget")
BUDGET_LIMIT_USD = float(os.getenv("BUDGET_LIMIT_USD", "30") or "30")


def _clean(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _event_action(event: Dict[str, Any]) -> str:
    requested_action = _clean(event.get("action", "")).lower()
    if requested_action in {"enable", "disable", "check", "status"}:
        return requested_action

    # A budget alarm reaches this Lambda through SNS. Such an event always
    # means that cost-generating synchronization resources must be disabled.
    records = event.get("Records", [])
    if isinstance(records, list):
        for record in records:
            if not isinstance(record, dict):
                continue
            event_source = _clean(
                record.get("EventSource") or record.get("eventSource")
            ).lower()
            if event_source == "aws:sns":
                return "disable"
    return "status"


def _load_budget_status() -> Dict[str, Any]:
    budget_status: Dict[str, Any] = {
        "name": BUDGET_NAME,
        "limit_usd": round(float(BUDGET_LIMIT_USD), 2),
        "spent_usd": 0.0,
        "percent_used": 0.0,
        "over_limit": False,
        "error": "",
    }
    if AWS_ACCOUNT_ID == "":
        budget_status["error"] = "missing_aws_account_id"
        return budget_status

    try:
        response = budgets.describe_budget(
            AccountId=AWS_ACCOUNT_ID,
            BudgetName=BUDGET_NAME,
        )
        budget = {}
        if isinstance(response, dict):
            budget = response.get("Budget", {})
        if not isinstance(budget, dict):
            budget = {}

        budget_limit = budget.get("BudgetLimit", {})
        if not isinstance(budget_limit, dict):
            budget_limit = {}

        calculated_spend = budget.get("CalculatedSpend", {})
        if not isinstance(calculated_spend, dict):
            calculated_spend = {}

        actual_spend = calculated_spend.get("ActualSpend", {})
        if not isinstance(actual_spend, dict):
            actual_spend = {}

        limit_usd = float(budget_limit.get("Amount") or BUDGET_LIMIT_USD)
        spent_usd = float(actual_spend.get("Amount") or 0.0)
        percent_used = 0.0
        if limit_usd > 0:
            percent_used = round(spent_usd * 100.0 / limit_usd, 2)

        budget_status.update(
            {
                "limit_usd": round(limit_usd, 2),
                "spent_usd": round(spent_usd, 4),
                "percent_used": percent_used,
                "over_limit": bool(limit_usd > 0 and spent_usd >= limit_usd),
            }
        )
    except Exception as exc:
        budget_status["error"] = f"{type(exc).__name__}: {exc}"
    return budget_status


def _update_schedule_state(enabled: bool) -> Dict[str, Any]:
    data = scheduler.get_schedule(Name=SYNC_SCHEDULE_NAME, GroupName=SYNC_SCHEDULE_GROUP)
    kwargs: Dict[str, Any] = {
        "Name": SYNC_SCHEDULE_NAME,
        "GroupName": SYNC_SCHEDULE_GROUP,
        "State": "ENABLED" if enabled else "DISABLED",
        "ScheduleExpression": data["ScheduleExpression"],
        "FlexibleTimeWindow": data["FlexibleTimeWindow"],
        "Target": data["Target"],
    }
    for key in (
        "ActionAfterCompletion",
        "Description",
        "EndDate",
        "KmsKeyArn",
        "ScheduleExpressionTimezone",
        "StartDate",
    ):
        if key in data and data[key] is not None:
            kwargs[key] = data[key]
    scheduler.update_schedule(**kwargs)
    return {"name": SYNC_SCHEDULE_NAME, "state": kwargs["State"]}


def _update_sqs_event_source_state(enabled: bool) -> Dict[str, Any]:
    if CONTINUATION_QUEUE_ARN == "":
        return {"configured": False, "updated": 0}
    mappings = lambda_api.list_event_source_mappings(
        FunctionName=SYNC_FUNCTION_NAME,
        EventSourceArn=CONTINUATION_QUEUE_ARN,
    ).get("EventSourceMappings", [])
    updated = 0
    for mapping in mappings:
        uuid = _clean(mapping.get("UUID"))
        if uuid == "":
            continue
        lambda_api.update_event_source_mapping(UUID=uuid, Enabled=enabled)
        updated += 1
    return {"configured": True, "updated": updated, "enabled": enabled}


def _write_status(payload: Dict[str, Any]) -> None:
    payload["updated_at_unix"] = int(time.time())
    payload["updated_at_iso"] = datetime.now(timezone.utc).isoformat()
    ssm.put_parameter(
        Name=STATUS_PARAM,
        Value=json.dumps(payload, ensure_ascii=False),
        Type="String",
        Overwrite=True,
        Tier="Standard",
    )


def _apply(action: str, reason: str) -> Dict[str, Any]:
    operation_result: Dict[str, Any] = {
        "ok": True,
        "action": action,
        "reason": reason,
        "sync_function": SYNC_FUNCTION_NAME,
        "errors": [],
    }
    try:
        if action == "disable":
            lambda_api.put_function_concurrency(
                FunctionName=SYNC_FUNCTION_NAME,
                ReservedConcurrentExecutions=0,
            )
            operation_result["reserved_concurrency"] = 0
        elif action == "enable":
            target_concurrency = max(1, TARGET_RESERVED_CONCURRENCY)
            lambda_api.put_function_concurrency(
                FunctionName=SYNC_FUNCTION_NAME,
                ReservedConcurrentExecutions=target_concurrency,
            )
            operation_result["reserved_concurrency"] = target_concurrency
    except Exception as exc:
        operation_result["ok"] = False
        operation_result["errors"].append(
            f"lambda_concurrency:{type(exc).__name__}: {exc}"
        )

    if action in {"disable", "enable"}:
        enabled = action == "enable"
        try:
            operation_result["schedule"] = _update_schedule_state(enabled)
        except Exception as exc:
            operation_result["ok"] = False
            operation_result["errors"].append(
                f"scheduler:{type(exc).__name__}: {exc}"
            )
        try:
            operation_result["sqs_event_source"] = _update_sqs_event_source_state(
                enabled
            )
        except Exception as exc:
            operation_result["ok"] = False
            operation_result["errors"].append(
                f"sqs_event_source:{type(exc).__name__}: {exc}"
            )

    _write_status(operation_result)
    return operation_result


def lambda_handler(event, _context):
    event = event if isinstance(event, dict) else {}
    action = _event_action(event)
    reason = _clean(event.get("reason", "")) or (
        "budget_threshold_reached" if action == "disable" else "monthly_budget_reset"
    )
    if action == "check":
        budget = _load_budget_status()
        if budget.get("over_limit"):
            result = _apply(action="disable", reason="budget_threshold_reached")
            result["budget"] = budget
            _write_status(result)
            return result
        payload = {
            "ok": True,
            "action": "check",
            "reason": reason,
            "budget": budget,
            "disabled": False,
        }
        _write_status(payload)
        return payload
    if action == "status":
        payload = {"ok": True, "action": "status", "reason": reason, "budget": _load_budget_status()}
        _write_status(payload)
        return payload
    return _apply(action=action, reason=reason)
