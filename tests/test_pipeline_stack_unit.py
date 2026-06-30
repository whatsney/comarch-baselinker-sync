import sys
import unittest
from pathlib import Path

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template


ROOT = Path(__file__).resolve().parents[1]
CDK_APP = ROOT / "cdk_app"
if str(CDK_APP) not in sys.path:
    sys.path.insert(0, str(CDK_APP))

from pipeline_stack import ComarchBaseLinkerPipelineStack  # noqa: E402


def _pipeline_template(sync_enabled: bool) -> Template:
    app = cdk.App(
        context={
            "xmlUrl": "https://shop.example.com/xml?id=example",
            "bucketName": "migration-test-bucket",
            "scheduleName": "migration-test-sync-schedule",
            "scheduleExpression": "cron(0 0,12,17 * * ? *)",
            "scheduleTimezone": "Europe/Warsaw",
            "syncEnabled": str(sync_enabled).lower(),
            "blInventoryId": "12345",
            "blWarehouseId": "bl_12345",
            "adminPasswordHash": "0" * 64,
        }
    )
    stack = ComarchBaseLinkerPipelineStack(
        app,
        "MigrationTestStack",
        env=cdk.Environment(account="111111111111", region="eu-north-1"),
    )
    return Template.from_stack(stack)


class TestPipelineDeploymentState(unittest.TestCase):
    def test_paused_deployment_disables_all_sync_entry_points(self):
        template = _pipeline_template(sync_enabled=False)

        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "FunctionName": "comarch-baselinker-sync",
                "ReservedConcurrentExecutions": 0,
            },
        )
        template.has_resource_properties(
            "AWS::Lambda::EventSourceMapping",
            {"Enabled": False},
        )
        template.has_resource_properties(
            "AWS::Scheduler::Schedule",
            {
                "Name": "migration-test-sync-schedule",
                "State": "DISABLED",
                "ScheduleExpression": "cron(0 0,12,17 * * ? *)",
                "ScheduleExpressionTimezone": "Europe/Warsaw",
            },
        )
        template.has_resource_properties(
            "AWS::Scheduler::Schedule",
            {
                "Name": "comarch-baselinker-budget-guard-monthly-enable",
                "State": "DISABLED",
            },
        )
        template.has_output("LambdaReservedConcurrency", {"Value": "0"})

    def test_enabled_deployment_restores_sync_entry_points(self):
        template = _pipeline_template(sync_enabled=True)

        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "FunctionName": "comarch-baselinker-sync",
                "ReservedConcurrentExecutions": 1,
            },
        )
        template.has_resource_properties(
            "AWS::Lambda::EventSourceMapping",
            {"Enabled": True},
        )
        template.has_resource_properties(
            "AWS::Scheduler::Schedule",
            {
                "Name": "migration-test-sync-schedule",
                "State": "ENABLED",
            },
        )
        template.has_resource_properties(
            "AWS::Scheduler::Schedule",
            {
                "Name": "comarch-baselinker-budget-guard-monthly-enable",
                "State": "ENABLED",
            },
        )
        template.has_output("LambdaReservedConcurrency", {"Value": "1"})

        template.resource_count_is("AWS::Lambda::EventSourceMapping", 1)
        template.has_resource_properties(
            "AWS::Scheduler::Schedule",
            Match.object_like({"Name": "comarch-baselinker-budget-guard-hourly-check"}),
        )


if __name__ == "__main__":
    unittest.main()
