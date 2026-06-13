#!/usr/bin/env python3
import os

import aws_cdk as cdk

from budget_stack import ComarchBaseLinkerBudgetStack
from pipeline_stack import ComarchBaseLinkerPipelineStack


app = cdk.App()
account = app.node.try_get_context("account") or os.getenv("CDK_DEFAULT_ACCOUNT")
region = (
    app.node.try_get_context("region")
    or os.getenv("CDK_DEFAULT_REGION")
    or "eu-north-1"
)

if not account:
    raise ValueError("AWS account is required through CDK context or CDK_DEFAULT_ACCOUNT.")

pipeline_stack_name = (
    app.node.try_get_context("pipelineStackName")
    or "ComarchBaseLinkerSyncStack"
)
budget_stack_name = (
    app.node.try_get_context("budgetStackName")
    or "ComarchBaseLinkerBudgetStack"
)

ComarchBaseLinkerPipelineStack(
    app,
    pipeline_stack_name,
    env=cdk.Environment(account=account, region=region),
)

ComarchBaseLinkerBudgetStack(
    app,
    budget_stack_name,
    synthesizer=cdk.BootstraplessSynthesizer(),
    env=cdk.Environment(account=account, region="us-east-1"),
)

app.synth()
