import os
import sys
import unittest
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


class _Ctx:
    aws_request_id = "req-1"
    invoked_function_arn = "arn:aws:lambda:eu-north-1:111111111111:function:comarch-baselinker-sync"
    function_name = "comarch-baselinker-sync"

    @staticmethod
    def get_remaining_time_in_millis():
        return 600_000


class TestSyncStatusHelpers(unittest.TestCase):
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
