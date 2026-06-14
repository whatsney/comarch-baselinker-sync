import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
BUDGET_GUARD_PATH = ROOT / "budget_guard_src" / "lambda_function.py"

os.environ.setdefault("AWS_DEFAULT_REGION", "eu-north-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

module_spec = importlib.util.spec_from_file_location(
    "budget_guard_lambda",
    BUDGET_GUARD_PATH,
)
budget_guard = importlib.util.module_from_spec(module_spec)
sys.modules[module_spec.name] = budget_guard
module_spec.loader.exec_module(budget_guard)


class _FakeBudgetsClient:
    def describe_budget(self, **_kwargs):
        return {
            "Budget": {
                "BudgetLimit": {
                    "Amount": "30",
                    "Unit": "USD",
                },
                "CalculatedSpend": {
                    "ActualSpend": {
                        "Amount": "12",
                        "Unit": "USD",
                    }
                },
            }
        }


class TestBudgetGuard(unittest.TestCase):
    def test_sns_event_disables_synchronization(self):
        action = budget_guard._event_action(
            {
                "Records": [
                    {
                        "EventSource": "aws:sns",
                    }
                ]
            }
        )

        self.assertEqual(action, "disable")

    def test_explicit_action_takes_precedence_over_event_source(self):
        action = budget_guard._event_action(
            {
                "action": "status",
                "Records": [
                    {
                        "EventSource": "aws:sns",
                    }
                ],
            }
        )

        self.assertEqual(action, "status")

    def test_budget_status_calculates_percentage(self):
        with patch.object(budget_guard, "AWS_ACCOUNT_ID", "111111111111"):
            with patch.object(budget_guard, "budgets", _FakeBudgetsClient()):
                status = budget_guard._load_budget_status()

        self.assertEqual(status["limit_usd"], 30.0)
        self.assertEqual(status["spent_usd"], 12.0)
        self.assertEqual(status["percent_used"], 40.0)
        self.assertFalse(status["over_limit"])


if __name__ == "__main__":
    unittest.main()
