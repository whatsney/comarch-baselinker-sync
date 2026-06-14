import os
import sys
import unittest
from pathlib import Path
from datetime import timezone
import types
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
ADMIN_SRC = ROOT / "admin_src"
if str(ADMIN_SRC) not in sys.path:
    sys.path.insert(0, str(ADMIN_SRC))

os.environ.setdefault("AWS_DEFAULT_REGION", "eu-north-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

# Python 3.8 test runner fallback: provide minimal zoneinfo API.
if "zoneinfo" not in sys.modules:
    zoneinfo_stub = types.ModuleType("zoneinfo")
    zoneinfo_stub.ZoneInfo = lambda _name: timezone.utc
    sys.modules["zoneinfo"] = zoneinfo_stub

import admin_lambda as admin  # noqa: E402


class TestAdminStatusSummary(unittest.TestCase):
    def test_running_without_progress_stays_on_pre_audit(self):
        out = admin._summarize_status(
            {
                "status": "running",
                "run_id": "run-1",
                "result": {
                    "sync_stage": "finished",
                    "pre_audit_summary": {"diff_total": 0, "unchanged_records": 123},
                },
            }
        )

        self.assertEqual(out["sync_stage"], "pre_audit")
        self.assertEqual(out["steps"][0]["status"], "running")
        self.assertEqual(out["steps"][1]["status"], "waiting")
        self.assertEqual(out["steps"][0]["summary"], "Checking what needs to change.")
        self.assertFalse(out["pre_audit_executed"])

    def test_running_sync_progress_keeps_sync_step_active(self):
        out = admin._summarize_status(
            {
                "status": "running",
                "run_id": "run-1",
                "progress": {
                    "sync_stage": "sync",
                    "pre_audit_executed": True,
                    "pre_audit_summary": {
                        "diff_total": 5,
                        "changed_records": 4,
                        "missing_in_bl": 1,
                        "extra_in_bl": 1,
                        "unchanged_records": 10,
                    },
                    "global_total_records": 4,
                    "global_changed_target": 4,
                    "global_requested": 2,
                    "global_updated": 2,
                    "global_delete_target": 1,
                },
            }
        )

        self.assertEqual(out["sync_stage"], "sync")
        self.assertEqual(out["steps"][0]["status"], "done")
        self.assertEqual(out["steps"][1]["status"], "running")
        self.assertEqual(out["mutation_total"], 5)
        self.assertEqual(out["mutation_done"], 2)

    def test_running_post_audit_marks_sync_step_done(self):
        out = admin._summarize_status(
            {
                "status": "running",
                "run_id": "run-1",
                "progress": {
                    "sync_stage": "post_audit",
                    "pre_audit_executed": True,
                    "pre_audit_summary": {
                        "diff_total": 5,
                        "changed_records": 4,
                        "missing_in_bl": 1,
                        "extra_in_bl": 1,
                        "unchanged_records": 10,
                    },
                    "global_total_records": 4,
                    "global_changed_target": 4,
                    "global_requested": 4,
                    "global_updated": 4,
                    "global_delete_target": 1,
                    "global_delete_deleted": 1,
                },
            }
        )

        self.assertEqual(out["sync_stage"], "post_audit")
        self.assertEqual(out["steps"][0]["status"], "done")
        self.assertEqual(out["steps"][1]["status"], "done")
        self.assertEqual(out["steps"][2]["status"], "running")
        self.assertEqual(out["mutation_total"], 5)
        self.assertEqual(out["mutation_done"], 5)


class TestAdminBranding(unittest.TestCase):
    def test_default_locale_is_english(self):
        page = admin._page()

        self.assertIn('<html lang="en">', page)
        self.assertIn("Refresh data", page)
        self.assertIn("Start synchronization", page)
        self.assertIn("Calculating differences before synchronization", page)

    def test_polish_locale_translates_page_and_status_summary(self):
        with patch.object(admin, "ADMIN_LOCALE", "pl"):
            page = admin._page()
            summary = admin._summarize_status({"status": "running", "progress": {}})

        self.assertIn('<html lang="pl">', page)
        self.assertIn("Odśwież dane", page)
        self.assertIn("Uruchom aktualizację", page)
        self.assertEqual(
            summary["steps"][0]["summary"],
            "Sprawdzamy, co trzeba zmienić.",
        )

    def test_translation_catalogs_have_matching_keys(self):
        self.assertEqual(
            set(admin.TRANSLATIONS["en"]),
            set(admin.TRANSLATIONS["pl"]),
        )

    def test_private_branding_is_rendered_and_escaped(self):
        with patch.object(admin, "BRAND_NAME", 'Example <Store>'):
            with patch.object(admin, "BRAND_PANEL_TITLE", "Catalog sync"):
                with patch.object(admin, "BRAND_PANEL_SUBTITLE", "Private deployment"):
                    with patch.object(admin, "BRAND_PRIMARY_COLOR", "#112233"):
                        with patch.object(admin, "BRAND_PRIMARY_DARK_COLOR", "#223344"):
                            with patch.object(admin, "BRAND_SECONDARY_COLOR", "#334455"):
                                with patch.object(admin, "BRAND_LOGO_ENABLED", True):
                                    page = admin._page()

        self.assertIn("Example &lt;Store&gt; - synchronization", page)
        self.assertIn('<img src="/assets/client-logo.png"', page)
        self.assertIn("--orange: #112233", page)
        self.assertIn("--orange-dark: #223344", page)
        self.assertIn("--navy: #334455", page)
        self.assertNotIn("Example <Store>", page)

    def test_invalid_brand_color_uses_safe_default(self):
        with patch.object(admin, "BRAND_PRIMARY_COLOR", "red; display:none"):
            page = admin._page()

        self.assertIn("--orange: #1673b8", page)
        self.assertNotIn("display:none", page)


if __name__ == "__main__":
    unittest.main()
