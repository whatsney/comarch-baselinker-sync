from __future__ import annotations

from aws_cdk import CfnOutput, Stack, aws_budgets as budgets
from constructs import Construct

from context_values import get_context_text


class BaseLinkerSyncBudgetStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        budget_name = get_context_text(
            self.node,
            "budgetName",
            "baselinker-sync-monthly-budget",
        )
        budget_alert_email = get_context_text(
            self.node,
            "budgetAlertEmail",
            "",
        )
        budget_limit_usd = get_context_text(
            self.node,
            "budgetLimitUsd",
            "30",
        )

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
