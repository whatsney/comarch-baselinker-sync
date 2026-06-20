import os
import sys
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import patch
from datetime import timezone
import types


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Python 3.8 test runner fallback: provide minimal zoneinfo API.
if "zoneinfo" not in sys.modules:
    zoneinfo_stub = types.ModuleType("zoneinfo")
    zoneinfo_stub.ZoneInfo = lambda _name: timezone.utc
    sys.modules["zoneinfo"] = zoneinfo_stub

import lambda_function as lf  # noqa: E402


class _FakeSSM:
    def __init__(self, payload=None, should_raise=False):
        self.payload = payload
        self.should_raise = should_raise

    def get_parameter(self, Name, **_kwargs):
        if self.should_raise:
            raise RuntimeError("ssm unavailable")
        return {"Parameter": {"Value": self.payload}}


class _FakeS3:
    def __init__(self, fail_keys=None):
        self.fail_keys = set(fail_keys or [])
        self.deleted = []

    def delete_object(self, Bucket, Key):
        if Key in self.fail_keys:
            raise RuntimeError("delete failed")
        self.deleted.append((Bucket, Key))


class _FakeSNS:
    def __init__(self):
        self.publish_calls = []

    def publish(self, **kwargs):
        self.publish_calls.append(kwargs)
        return {"MessageId": "message-123"}


class _FailingSNS:
    @staticmethod
    def publish(**_kwargs):
        raise RuntimeError("SNS unavailable")


class _Ctx:
    aws_request_id = "req-1"
    invoked_function_arn = "arn:aws:lambda:eu-north-1:111111111111:function:comarch-baselinker-sync"
    function_name = "comarch-baselinker-sync"

    @staticmethod
    def get_remaining_time_in_millis():
        return 600_000


class TestProductFeedParsing(unittest.TestCase):
    def test_parse_records_deduplicates_images_and_reads_attributes(self):
        root = ET.fromstring(
            """
            <products>
              <product>
                <id>100</id>
                <parent_id>0</parent_id>
                <name>Example product</name>
                <stock_quantity>2</stock_quantity>
                <main_image>https://example.com/main.jpg</main_image>
                <image_extra_1>https://example.com/main.jpg</image_extra_1>
                <images>
                  <image>https://example.com/extra.jpg</image>
                </images>
                <attributes>
                  <attribute>
                    <attribute_name>Kolor</attribute_name>
                    <attribute_value>Czarny</attribute_value>
                  </attribute>
                </attributes>
              </product>
            </products>
            """
        )

        records = lf._parse_records(root)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["parent_id"], "")
        self.assertEqual(records[0]["quantity"], 2)
        self.assertEqual(
            records[0]["images"],
            [
                "https://example.com/main.jpg",
                "https://example.com/extra.jpg",
            ],
        )
        self.assertEqual(records[0]["attributes"], [("Kolor", "Czarny")])

    def test_build_relationships_keeps_parent_and_positive_stock_variant(self):
        parent = {"id": "100", "parent_id": "", "quantity": 0}
        available_variant = {"id": "101", "parent_id": "100", "quantity": 3}
        unavailable_variant = {"id": "102", "parent_id": "100", "quantity": 0}
        orphan = {"id": "103", "parent_id": "999", "quantity": 4}

        output_records, variants_by_parent, stats = lf._build_relationships(
            records=[
                parent,
                available_variant,
                unavailable_variant,
                orphan,
            ],
            include_orphans_as_products=False,
        )

        self.assertEqual(
            [record["id"] for record in output_records],
            ["100", "101"],
        )
        self.assertEqual(
            [record["id"] for record in variants_by_parent["100"]],
            ["101", "102"],
        )
        self.assertEqual(stats["orphan_variants"], 1)
        self.assertEqual(stats["dropped_zero_qty_variant_products"], 1)
        self.assertEqual(stats["output_orphan_products"], 0)

    def test_build_relationships_can_include_positive_stock_orphan(self):
        orphan = {"id": "103", "parent_id": "999", "quantity": 4}

        output_records, _, stats = lf._build_relationships(
            records=[orphan],
            include_orphans_as_products=True,
        )

        self.assertEqual([record["id"] for record in output_records], ["103"])
        self.assertEqual(stats["output_orphan_products"], 1)


class TestSyncStatusHelpers(unittest.TestCase):
    def test_compact_sync_status_section_preserves_admin_panel_fields(self):
        compact = lf._compact_sync_status_section(
            {
                "global_total_records": "10",
                "global_processed": 4,
                "sync_stage": "sync",
                "pre_audit_executed": True,
                "pre_audit_summary": {
                    "diff_total": "3",
                    "unchanged_records": 7,
                    "ignored_detail": "not stored",
                },
                "large_debug_value": "not stored",
                "post_audit_alert_required": True,
                "post_audit_alert_published": True,
            }
        )

        self.assertEqual(compact["global_total_records"], 10)
        self.assertEqual(compact["global_processed"], 4)
        self.assertEqual(compact["sync_stage"], "sync")
        self.assertTrue(compact["pre_audit_executed"])
        self.assertTrue(compact["post_audit_alert_required"])
        self.assertTrue(compact["post_audit_alert_published"])
        self.assertEqual(
            compact["pre_audit_summary"],
            {
                "unchanged_records": 7,
                "diff_total": 3,
            },
        )
        self.assertNotIn("large_debug_value", compact)


    def test_safe_get_sync_status_returns_dict(self):
        fake = _FakeSSM(payload='{"status":"running","updated_at_unix":123}')
        with patch.object(lf, "ssm", fake):
            out = lf._safe_get_sync_status("/param")
        self.assertEqual(out["status"], "running")
        self.assertEqual(out["updated_at_unix"], 123)

    def test_safe_get_sync_status_returns_empty_on_bad_payload(self):
        fake = _FakeSSM(payload='["not-a-dict"]')
        with patch.object(lf, "ssm", fake):
            out = lf._safe_get_sync_status("/param")
        self.assertEqual(out, {})

    def test_safe_get_sync_status_returns_empty_on_exception(self):
        fake = _FakeSSM(should_raise=True)
        with patch.object(lf, "ssm", fake):
            out = lf._safe_get_sync_status("/param")
        self.assertEqual(out, {})

    def test_extract_status_updated_at_unix_prefers_numeric(self):
        payload = {
            "updated_at_unix": 1000,
            "progress": {"updated_at_unix": 2000},
            "result": {"updated_at_unix": 3000},
        }
        self.assertEqual(lf._extract_status_updated_at_unix(payload), 1000)

    def test_extract_status_updated_at_unix_from_iso(self):
        payload = {"updated_at_iso": "2026-06-01T16:00:00Z"}
        out = lf._extract_status_updated_at_unix(payload)
        self.assertGreater(out, 0)

    def test_extract_status_updated_at_unix_returns_zero_when_missing(self):
        self.assertEqual(lf._extract_status_updated_at_unix({}), 0)


class TestPostSyncAuditAlerts(unittest.TestCase):
    @staticmethod
    def _audit_result(diff_total=3, audit_error=""):
        has_differences = diff_total > 0
        return {
            "has_more_batches": False,
            "post_audit_executed": True,
            "post_audit_summary": {
                "diff_total": diff_total,
                "changed_records": 2 if has_differences else 0,
                "missing_in_bl": 1 if has_differences else 0,
                "extra_in_bl": 0,
                "diff_breakdown": (
                    {"missing_in_bl": 1, "price": 2}
                    if has_differences
                    else {}
                ),
            },
            "post_audit_error": audit_error,
            "post_audit_summary_key": "feeds/products.bl-audit-post.summary.json",
            "post_audit_details_key": "feeds/products.bl-audit-post.details.ndjson",
        }

    def test_publishes_details_and_portal_link_when_post_audit_finds_differences(self):
        fake_sns = _FakeSNS()

        with patch.object(lf, "sns_api", fake_sns):
            outcome = lf._publish_post_audit_alert(
                sync_result=self._audit_result(),
                run_id="run-123",
                topic_arn="arn:aws:sns:eu-north-1:111111111111:post-sync-alerts",
                output_bucket="audit-bucket",
                admin_portal_url="https://portal.example.com/",
            )

        self.assertTrue(outcome["required"])
        self.assertTrue(outcome["published"])
        self.assertEqual(outcome["message_id"], "message-123")
        self.assertEqual(len(fake_sns.publish_calls), 1)
        published_message = fake_sns.publish_calls[0]["Message"]
        self.assertIn("Detected differences: 3", published_message)
        self.assertIn("- price: 2", published_message)
        self.assertIn(
            "s3://audit-bucket/feeds/products.bl-audit-post.details.ndjson",
            published_message,
        )
        self.assertIn("Administration portal: https://portal.example.com/", published_message)

    def test_does_not_publish_when_post_audit_is_clean(self):
        fake_sns = _FakeSNS()

        with patch.object(lf, "sns_api", fake_sns):
            outcome = lf._publish_post_audit_alert(
                sync_result=self._audit_result(diff_total=0),
                run_id="run-123",
                topic_arn="arn:aws:sns:eu-north-1:111111111111:post-sync-alerts",
                output_bucket="audit-bucket",
                admin_portal_url="https://portal.example.com/",
            )

        self.assertFalse(outcome["required"])
        self.assertFalse(outcome["published"])
        self.assertEqual(fake_sns.publish_calls, [])

    def test_audit_failure_also_requires_notification(self):
        issue_details = lf._post_audit_issue_details(
            self._audit_result(diff_total=0, audit_error="TimeoutError: audit timed out")
        )

        self.assertIsNotNone(issue_details)
        self.assertEqual(issue_details["difference_count"], 0)
        self.assertIn("audit timed out", issue_details["audit_error"])

    def test_publish_failure_is_reported_without_failing_the_sync(self):
        with patch.object(lf, "sns_api", _FailingSNS()):
            outcome = lf._publish_post_audit_alert(
                sync_result=self._audit_result(),
                run_id="run-123",
                topic_arn="arn:aws:sns:eu-north-1:111111111111:post-sync-alerts",
                output_bucket="audit-bucket",
                admin_portal_url="https://portal.example.com/",
            )

        self.assertTrue(outcome["required"])
        self.assertFalse(outcome["published"])
        self.assertIn("SNS unavailable", outcome["error"])

    def test_intermediate_batch_never_requires_notification(self):
        sync_result = self._audit_result()
        sync_result["has_more_batches"] = True

        self.assertIsNone(lf._post_audit_issue_details(sync_result))


class TestStateKeyHelpers(unittest.TestCase):
    def test_state_key_for_xml(self):
        self.assertEqual(
            lf._state_key_for_api_sync("feeds/baselinker/products.xml"),
            "feeds/baselinker/products.bl-sync-state.json",
        )

    def test_snapshot_key_for_xml(self):
        self.assertEqual(
            lf._source_snapshot_key_for_api_sync("feeds/baselinker/products.xml"),
            "feeds/baselinker/products.bl-sync-source.xml",
        )


class TestMaybeResetStaleSyncState(unittest.TestCase):
    def test_no_reset_when_status_is_fresh(self):
        with patch.object(lf, "_safe_get_sync_status", return_value={"updated_at_unix": 1900}):
            with patch.object(lf.time, "time", return_value=2000):
                fake_s3 = _FakeS3()
                with patch.object(lf, "s3", fake_s3):
                    out = lf._maybe_reset_stale_sync_state(
                        output_bucket="bucket",
                        output_key="feeds/baselinker/products.xml",
                        sync_status_param="/status",
                        stale_after_sec=3600,
                    )
        self.assertFalse(out["reset_performed"])
        self.assertEqual(fake_s3.deleted, [])

    def test_reset_when_status_is_stale(self):
        with patch.object(lf, "_safe_get_sync_status", return_value={"status": "running", "run_id": "abc", "updated_at_unix": 1000}):
            with patch.object(lf.time, "time", return_value=5000):
                fake_s3 = _FakeS3()
                with patch.object(lf, "s3", fake_s3):
                    out = lf._maybe_reset_stale_sync_state(
                        output_bucket="bucket",
                        output_key="feeds/baselinker/products.xml",
                        sync_status_param="/status",
                        stale_after_sec=3600,
                    )
        self.assertTrue(out["reset_performed"])
        self.assertEqual(
            out["deleted_keys"],
            [
                "feeds/baselinker/products.bl-sync-state.json",
                "feeds/baselinker/products.bl-sync-source.xml",
            ],
        )

    def test_reset_collects_delete_errors(self):
        with patch.object(lf, "_safe_get_sync_status", return_value={"updated_at_unix": 1000}):
            with patch.object(lf.time, "time", return_value=5000):
                fake_s3 = _FakeS3(
                    fail_keys={"feeds/baselinker/products.bl-sync-source.xml"}
                )
                with patch.object(lf, "s3", fake_s3):
                    out = lf._maybe_reset_stale_sync_state(
                        output_bucket="bucket",
                        output_key="feeds/baselinker/products.xml",
                        sync_status_param="/status",
                        stale_after_sec=3600,
                    )
        self.assertTrue(out["reset_performed"])
        self.assertEqual(len(out["delete_errors"]), 1)
        self.assertIn("products.bl-sync-source.xml", out["delete_errors"][0])


class TestLambdaHandlerStaleResetFlow(unittest.TestCase):
    def _env(self):
        return {
            "COMARCH_URL": "https://example.com/feed.xml",
            "OUTPUT_BUCKET": "bucket",
            "OUTPUT_KEY": "feeds/baselinker/products.xml",
            "BL_INVENTORY_ID": "12345",
            "BL_WAREHOUSE_ID": "bl_12345",
            "BL_ENABLE_SELF_CHAIN": "false",
            "BL_RESET_STATE_IF_STATUS_STALE_ENABLED": "true",
            "BL_RESET_STATE_IF_STATUS_STALE_SEC": "3600",
        }

    def test_root_invoke_uses_stale_reset(self):
        stale_info = {
            "enabled": True,
            "reset_performed": True,
            "stale_after_sec": 3600,
            "status_updated_at_unix": 1000,
            "status_age_sec": 5000,
            "status": "running",
            "status_run_id": "old-run",
            "deleted_keys": ["k1", "k2", "k3"],
            "delete_errors": [],
        }
        with patch.dict(os.environ, self._env(), clear=False):
            with patch.object(lf, "_refresh_budget_fx_rate_ssm", return_value={}):
                with patch.object(lf, "_maybe_reset_stale_sync_state", return_value=stale_info) as mock_reset:
                    with patch.object(lf, "_maybe_reset_state_for_config_change", return_value={"reset_performed": False}):
                        with patch.object(lf, "_load_json_state", return_value={}):
                            with patch.object(lf, "_download", return_value=b"<products/>"):
                                with patch.object(lf, "_resolve_bl_api_token", return_value="token"):
                                    with patch.object(
                                        lf,
                                        "_sync_to_bl_api",
                                        return_value={"has_more_batches": False, "token_temporarily_blocked": False},
                                    ):
                                        with patch.object(lf, "_safe_put_sync_status", return_value=None):
                                            out = lf.lambda_handler({}, _Ctx())
        self.assertTrue(mock_reset.called)
        self.assertEqual(out["stale_state_reset"], stale_info)

    def test_chain_invoke_skips_stale_reset(self):
        with patch.dict(os.environ, self._env(), clear=False):
            with patch.object(lf, "_maybe_reset_stale_sync_state") as mock_reset:
                with patch.object(lf, "_load_json_state", return_value={}):
                    with patch.object(lf, "_download", return_value=b"<products/>"):
                        with patch.object(lf, "_resolve_bl_api_token", return_value="token"):
                            with patch.object(
                                lf,
                                "_sync_to_bl_api",
                                return_value={"has_more_batches": False, "token_temporarily_blocked": False},
                            ):
                                with patch.object(lf, "_safe_put_sync_status", return_value=None):
                                    out = lf.lambda_handler({"sync_chain": True}, _Ctx())
        mock_reset.assert_not_called()
        self.assertIn("stale_state_reset", out)
        self.assertFalse(out["stale_state_reset"]["reset_performed"])

    def test_resume_uses_snapshot_without_live_download(self):
        preview_state = {
            "sync_cursor_index": 12,
            "sync_source_snapshot_key": "feeds/baselinker/products.bl-sync-source.xml",
            "sync_source_live_digest_last_seen": "digest-from-live",
        }
        with patch.dict(os.environ, self._env(), clear=False):
            with patch.object(lf, "_refresh_budget_fx_rate_ssm", return_value={}):
                with patch.object(lf, "_maybe_reset_stale_sync_state", return_value={"reset_performed": False}):
                    with patch.object(lf, "_maybe_reset_state_for_config_change", return_value={"reset_performed": False}):
                        with patch.object(lf, "_load_json_state", return_value=preview_state):
                            with patch.object(lf, "_load_source_snapshot", return_value=b"<products/>"):
                                with patch.object(lf, "_download") as mock_download:
                                    with patch.object(lf, "_resolve_bl_api_token", return_value="token"):
                                        with patch.object(
                                            lf,
                                            "_sync_to_bl_api",
                                            return_value={"has_more_batches": False, "token_temporarily_blocked": False},
                                        ) as mock_sync:
                                            with patch.object(lf, "_safe_put_sync_status", return_value=None):
                                                out = lf.lambda_handler({}, _Ctx())
        mock_download.assert_not_called()
        self.assertEqual(out.get("source_fetch_mode"), "snapshot_resume")
        self.assertEqual(mock_sync.call_args.kwargs.get("source_xml"), b"<products/>")
        self.assertEqual(
            mock_sync.call_args.kwargs.get("source_live_digest_hint"),
            "digest-from-live",
        )

    def test_status_success_when_sync_skipped_no_changes(self):
        with patch.dict(os.environ, self._env(), clear=False):
            with patch.object(lf, "_refresh_budget_fx_rate_ssm", return_value={}):
                with patch.object(lf, "_maybe_reset_stale_sync_state", return_value={"reset_performed": False}):
                    with patch.object(lf, "_maybe_reset_state_for_config_change", return_value={"reset_performed": False}):
                        with patch.object(lf, "_load_json_state", return_value={}):
                            with patch.object(lf, "_download", return_value=b"<products/>"):
                                with patch.object(lf, "_resolve_bl_api_token", return_value="token"):
                                    with patch.object(
                                        lf,
                                        "_sync_to_bl_api",
                                        return_value={
                                            "has_more_batches": False,
                                            "token_temporarily_blocked": False,
                                            "sync_skipped_no_changes": True,
                                        },
                                    ):
                                        with patch.object(lf, "_safe_put_sync_status", return_value=None) as mock_status:
                                            out = lf.lambda_handler({}, _Ctx())
        self.assertTrue(out.get("sync_skipped_no_changes"))
        status_payload = mock_status.call_args_list[-1][0][1]
        self.assertEqual(status_payload.get("status"), "success")
        self.assertIn("diff_total=0", status_payload.get("message", ""))

class TestSqsContinuationHelpers(unittest.TestCase):
    def test_extract_event_payload_from_sqs_record(self):
        event = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "messageId": "msg-1",
                    "body": '{"sync_chain": true, "sync_chain_depth": 4, "continue_not_before_unix": 12345}',
                }
            ]
        }
        payload, source = lf._extract_event_payload(event)
        self.assertEqual(source, "sqs")
        self.assertTrue(payload.get("sync_chain"))
        self.assertEqual(payload.get("sync_chain_depth"), 4)
        self.assertEqual(payload.get("_sqs_message_id"), "msg-1")

    def test_enqueue_sync_continuation_uses_sqs_delay(self):
        with patch.object(lf.time, "time", return_value=1000):
            with patch.object(
                lf.sqs_api,
                "send_message",
                return_value={
                    "MessageId": "m-1",
                    "ResponseMetadata": {"HTTPStatusCode": 200},
                },
            ) as mock_send:
                out = lf._enqueue_sync_continuation(
                    function_arn="arn:aws:lambda:eu-north-1:111111111111:function:test",
                    next_chain_depth=5,
                    continuation_sqs_url="https://sqs.eu-north-1.amazonaws.com/111111111111/test-q",
                    continue_not_before_unix=1010,
                    chain_reason="blocked_token_resume",
                    blocked_min_delay_sec=65,
                )
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("mode"), "sqs_delay")
        self.assertEqual(out.get("delay_seconds"), 65)
        self.assertEqual(mock_send.call_args.kwargs.get("DelaySeconds"), 65)


class TestApiTokenResolution(unittest.TestCase):
    def test_resolve_bl_api_token_from_static_ssm_secure_string(self):
        fake = _FakeSSM(payload="token-from-ssm")
        with patch.object(lf, "ssm", fake):
            token = lf._resolve_bl_api_token()
        self.assertEqual(token, "token-from-ssm")


if __name__ == "__main__":
    unittest.main()
