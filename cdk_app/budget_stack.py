from __future__ import annotations

from typing import Optional

from aws_cdk import CfnOutput, Stack, aws_budgets as budgets
from constructs import Construct


class ComarchBaseLinkerBudgetStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        def ctx(name: str, default: Optional[str] = None, required: bool = False) -> str:
            value = self.node.try_get_context(name)
            if value is None or str(value).strip() == "":
                value = default
            if required and (value is None or str(value).strip() == ""):
                raise ValueError(f"Missing required CDK context: {name}")
            return str(value) if value is not None else ""

        budget_name = ctx("budgetName", "comarch-baselinker-sync-monthly-budget")
        budget_alert_email = ctx("budgetAlertEmail", "")
        budget_limit_usd = ctx("budgetLimitUsd", "30")

        subscribers = []
        if budget_alert_email.strip() != "":
            subscribers.append(
                budgets.CfnBudget.SubscriberProperty(
                    subscription_type="EMAIL",
                    address=budget_alert_email.strip(),
                )
            )

        notifications = []
        if subscribers:
            for threshold, threshold_type in (
                (0.01, "ABSOLUTE_VALUE"),
                (100, "PERCENTAGE"),
            ):
                notifications.append(
                    budgets.CfnBudget.NotificationWithSubscribersProperty(
                        notification=budgets.CfnBudget.NotificationProperty(
                            comparison_operator="GREATER_THAN",
                            notification_type="ACTUAL",
                            threshold=threshold,
                            threshold_type=threshold_type,
                        ),
                        subscribers=subscribers,
                    )
                )

        budgets.CfnBudget(
            self,
            "MonthlyBudget",
            budget=budgets.CfnBudget.BudgetDataProperty(
                budget_name=budget_name,
                budget_type="COST",
                time_unit="MONTHLY",
                budget_limit=budgets.CfnBudget.SpendProperty(
                    amount=float(budget_limit_usd),
                    unit="USD",
                ),
            ),
            notifications_with_subscribers=notifications or None,
        )

        CfnOutput(self, "BudgetName", value=budget_name)
        CfnOutput(self, "BudgetLimitUsd", value=budget_limit_usd)
