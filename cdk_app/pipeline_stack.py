from __future__ import annotations

from typing import Optional

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as apigwv2_integrations,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_sqs as sqs,
    aws_s3 as s3,
    aws_scheduler as scheduler,
)
from constructs import Construct


class ComarchBaseLinkerPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        def ctx(name: str, default: Optional[str] = None, required: bool = False) -> str:
            value = self.node.try_get_context(name)
            if value is None or str(value).strip() == "":
                value = default
            if required and (value is None or str(value).strip() == ""):
                raise ValueError(f"Missing required CDK context: {name}")
            return str(value) if value is not None else ""

        def ctx_bool(name: str, default: bool) -> bool:
            raw = self.node.try_get_context(name)
            if raw is None:
                return default
            return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

        def ctx_int(name: str, default: int) -> int:
            raw = self.node.try_get_context(name)
            if raw is None or str(raw).strip() == "":
                return int(default)
            return int(str(raw).strip())

        comarch_url = ctx("comarchUrl", required=True)
        bucket_name = ctx("bucketName", required=True)
        output_key = ctx("outputKey", "feeds/baselinker/products.xml")
        function_name = ctx("functionName", "comarch-baselinker-sync")
        continuation_queue_name = ctx("continuationQueueName", "")
        lambda_function_arn = (
            f"arn:aws:lambda:{self.region}:{self.account}:function:{function_name}"
        )
        schedule_name = ctx("scheduleName", "comarch-baselinker-sync-midnight")
        schedule_expression = ctx("scheduleExpression", "cron(0 0 * * ? *)")
        schedule_timezone = ctx("scheduleTimezone", "Europe/Warsaw")
        admin_api_name = ctx("adminApiName", "comarch-baselinker-sync-admin-api")
        admin_function_name = ctx("adminFunctionName", "comarch-baselinker-sync-admin")
        admin_username = ctx("adminUsername", "admin")
        admin_password_hash = ctx("adminPasswordHash", required=True)
        brand_name = ctx("brandName", "Comarch → BaseLinker Sync")
        brand_panel_title = ctx("brandPanelTitle", "Synchronizacja produktów")
        brand_panel_subtitle = ctx(
            "brandPanelSubtitle",
            "Podgląd i ręczne uruchamianie synchronizacji produktów",
        )
        brand_primary_color = ctx("brandPrimaryColor", "#1673b8")
        brand_primary_dark_color = ctx("brandPrimaryDarkColor", "#0f5d96")
        brand_secondary_color = ctx("brandSecondaryColor", "#183c5c")
        brand_logo_enabled = ctx_bool("brandLogoEnabled", False)
        budget_name = ctx("budgetName", "comarch-baselinker-sync-monthly-budget")
        budget_limit_usd = ctx("budgetLimitUsd", "30")
        budget_usd_to_pln_rate = ctx("budgetUsdToPlnRate", "4.00")
        budget_fx_rate_ssm_param = ctx("budgetFxRateSsmParam", "/comarch-baselinker-sync/usd-pln-rate")
        budget_guard_function_name = ctx("budgetGuardFunctionName", "comarch-baselinker-budget-guard")
        budget_guard_monthly_schedule_name = ctx(
            "budgetGuardMonthlyScheduleName",
            "comarch-baselinker-budget-guard-monthly-enable",
        )
        budget_guard_hourly_schedule_name = ctx(
            "budgetGuardHourlyScheduleName",
            "comarch-baselinker-budget-guard-hourly-check",
        )
        budget_guard_status_ssm_param = ctx(
            "budgetGuardStatusSsmParam",
            "/comarch-baselinker-sync/budget-guard-status",
        )
        bl_api_token_ssm_param = ctx(
            "blApiTokenSsmParam",
            "/comarch-baselinker-sync/api-token",
        )

        include_orphans = ctx_bool("includeOrphansAsProducts", False)
        make_public_feed = ctx_bool("makePublicFeed", False)

        request_timeout_sec = ctx_int("requestTimeoutSec", 180)
        lambda_timeout_sec = ctx_int("lambdaTimeoutSec", 900)
        lambda_memory_mb = ctx_int("lambdaMemoryMb", 1280)

        bl_inventory_id = ctx("blInventoryId", required=True)
        bl_warehouse_id = ctx("blWarehouseId", required=True)
        bl_api_timeout_sec = ctx_int("blApiTimeoutSec", 60)
        bl_api_max_rpm = ctx_int("blApiMaxRpm", 90)
        bl_max_upserts_per_run = ctx_int("blMaxUpsertsPerRun", 200)
        bl_max_records_per_run = ctx_int("blMaxRecordsPerRun", 600)
        bl_enable_self_chain = ctx_bool("blEnableSelfChain", True)
        bl_progress_update_every = ctx_int("blProgressUpdateEvery", 50)
        bl_min_remaining_ms_for_continue = ctx_int("blMinRemainingMsForContinue", 180000)
        bl_remote_cache_ttl_sec = ctx_int("blRemoteCacheTtlSec", 21600)
        bl_bulk_update_enabled = ctx_bool("blBulkUpdateEnabled", True)
        bl_bulk_update_max_items = ctx_int("blBulkUpdateMaxItems", 1000)
        bl_bulk_update_min_items = ctx_int("blBulkUpdateMinItems", 5)
        bl_eta_moving_avg_enabled = ctx_bool("blEtaMovingAvgEnabled", True)
        bl_eta_ma_alpha = ctx("blEtaMaAlpha", "0.30")
        bl_eta_ma_min_rpm = ctx_int("blEtaMaMinRpm", 1)
        bl_eta_ma_bootstrap_sec = ctx_int("blEtaMaBootstrapSec", 45)
        bl_full_audit_enabled = ctx_bool("blFullAuditEnabled", True)
        bl_full_audit_details_limit_per_type = ctx_int("blFullAuditDetailsLimitPerType", 20)
        bl_full_audit_max_details_rows = ctx_int("blFullAuditMaxDetailsRows", 3000)
        bl_reset_state_if_status_stale_enabled = ctx_bool(
            "blResetStateIfStatusStaleEnabled", True
        )
        bl_reset_state_if_status_stale_sec = ctx_int("blResetStateIfStatusStaleSec", 3600)
        bl_sync_status_ssm_param = ctx("blSyncStatusSsmParam", "/comarch-baselinker-sync/push-sync-status")
        bl_sync_config_ssm_param = ctx("blSyncConfigSsmParam", "/comarch-baselinker-sync/sync-config")
        bl_continuation_blocked_min_delay_sec = ctx_int(
            "blContinuationBlockedMinDelaySec", 65
        )
        bl_blocked_token_max_inline_wait_sec = ctx_int(
            "blBlockedTokenMaxInlineWaitSec", 70
        )

        reserved_concurrency_raw = ctx("reservedConcurrency", "1").strip().lower()
        if reserved_concurrency_raw in {"", "none", "unreserved"}:
            reserved_concurrency = None
        else:
            reserved_concurrency = int(reserved_concurrency_raw)

        bucket = s3.Bucket(
            self,
            "FeedBucket",
            bucket_name=bucket_name,
            enforce_ssl=True,
            versioned=False,
            auto_delete_objects=False,
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                ignore_public_acls=True,
                block_public_policy=not make_public_feed,
                restrict_public_buckets=not make_public_feed,
            ),
        )

        if make_public_feed:
            bucket.add_to_resource_policy(
                iam.PolicyStatement(
                    sid="AllowPublicReadFeedObject",
                    effect=iam.Effect.ALLOW,
                    principals=[iam.AnyPrincipal()],
                    actions=["s3:GetObject"],
                    resources=[f"arn:aws:s3:::{bucket_name}/{output_key}"],
                )
            )

        lambda_env = {
            "COMARCH_URL": comarch_url,
            "OUTPUT_BUCKET": bucket.bucket_name,
            "OUTPUT_KEY": output_key,
            "REQUEST_TIMEOUT_SEC": str(request_timeout_sec),
            "INCLUDE_ORPHANS_AS_PRODUCTS": str(include_orphans).lower(),
            "BL_INVENTORY_ID": bl_inventory_id,
            "BL_WAREHOUSE_ID": bl_warehouse_id,
            "BL_API_TIMEOUT_SEC": str(bl_api_timeout_sec),
            "BL_API_MAX_RPM": str(bl_api_max_rpm),
            "BL_MAX_UPSERTS_PER_RUN": str(bl_max_upserts_per_run),
            "BL_MAX_RECORDS_PER_RUN": str(bl_max_records_per_run),
            "BL_ENABLE_SELF_CHAIN": str(bl_enable_self_chain).lower(),
            "BL_PROGRESS_UPDATE_EVERY": str(bl_progress_update_every),
            "BL_MIN_REMAINING_MS_FOR_CONTINUE": str(bl_min_remaining_ms_for_continue),
            "BL_REMOTE_CACHE_TTL_SEC": str(bl_remote_cache_ttl_sec),
            "BL_BULK_UPDATE_ENABLED": str(bl_bulk_update_enabled).lower(),
            "BL_BULK_UPDATE_MAX_ITEMS": str(bl_bulk_update_max_items),
            "BL_BULK_UPDATE_MIN_ITEMS": str(bl_bulk_update_min_items),
            "BL_ETA_MOVING_AVG_ENABLED": str(bl_eta_moving_avg_enabled).lower(),
            "BL_ETA_MA_ALPHA": bl_eta_ma_alpha,
            "BL_ETA_MA_MIN_RPM": str(bl_eta_ma_min_rpm),
            "BL_ETA_MA_BOOTSTRAP_SEC": str(bl_eta_ma_bootstrap_sec),
            "BL_FULL_AUDIT_ENABLED": str(bl_full_audit_enabled).lower(),
            "BL_FULL_AUDIT_DETAILS_LIMIT_PER_TYPE": str(
                bl_full_audit_details_limit_per_type
            ),
            "BL_FULL_AUDIT_MAX_DETAILS_ROWS": str(bl_full_audit_max_details_rows),
            "BL_RESET_STATE_IF_STATUS_STALE_ENABLED": str(
                bl_reset_state_if_status_stale_enabled
            ).lower(),
            "BL_RESET_STATE_IF_STATUS_STALE_SEC": str(bl_reset_state_if_status_stale_sec),
            "BL_SYNC_STATUS_SSM_PARAM": bl_sync_status_ssm_param,
            "BL_SYNC_CONFIG_SSM_PARAM": bl_sync_config_ssm_param,
            "BUDGET_FX_RATE_SSM_PARAM": budget_fx_rate_ssm_param,
            "BUDGET_USD_TO_PLN_RATE": budget_usd_to_pln_rate,
            "BL_API_TOKEN_SSM_PARAM": bl_api_token_ssm_param,
            "BL_CONTINUATION_BLOCKED_MIN_DELAY_SEC": str(
                bl_continuation_blocked_min_delay_sec
            ),
            "BL_BLOCKED_TOKEN_MAX_INLINE_WAIT_SEC": str(bl_blocked_token_max_inline_wait_sec),
        }

        fn = lambda_.Function(
            self,
            "ComarchToBlLambda",
            function_name=function_name,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../src"),
            timeout=Duration.seconds(lambda_timeout_sec),
            memory_size=lambda_memory_mb,
            reserved_concurrent_executions=reserved_concurrency,
            environment=lambda_env,
        )

        bucket.grant_read_write(fn)

        param_resource_path = bl_sync_status_ssm_param.lstrip("/")
        param_arn = f"arn:aws:ssm:{self.region}:{self.account}:parameter/{param_resource_path}"
        config_param_resource_path = bl_sync_config_ssm_param.lstrip("/")
        config_param_arn = (
            f"arn:aws:ssm:{self.region}:{self.account}:parameter/{config_param_resource_path}"
        )
        budget_fx_param_resource_path = budget_fx_rate_ssm_param.lstrip("/")
        budget_fx_param_arn = (
            f"arn:aws:ssm:{self.region}:{self.account}:parameter/{budget_fx_param_resource_path}"
        )
        fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter", "ssm:PutParameter"],
                resources=[param_arn],
            )
        )
        fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter"],
                resources=[config_param_arn],
            )
        )
        fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:PutParameter"],
                resources=[budget_fx_param_arn],
            )
        )
        token_param_resource_path = bl_api_token_ssm_param.lstrip("/")
        token_param_arn = (
            f"arn:aws:ssm:{self.region}:{self.account}:parameter/{token_param_resource_path}"
        )
        fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter"],
                resources=[token_param_arn],
            )
        )
        fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction"],
                resources=[lambda_function_arn, f"{lambda_function_arn}:*"],
            )
        )

        queue_kwargs = {
            "visibility_timeout": Duration.seconds(max(1200, lambda_timeout_sec + 120)),
            "retention_period": Duration.days(4),
        }
        if continuation_queue_name != "":
            queue_kwargs["queue_name"] = continuation_queue_name
        continuation_queue = sqs.Queue(
            self,
            "ContinuationQueue",
            **queue_kwargs,
        )
        continuation_queue.grant_send_messages(fn)
        continuation_queue.grant_consume_messages(fn)
        fn.add_environment("BL_CONTINUATION_SQS_URL", continuation_queue.queue_url)
        fn.add_event_source(
            lambda_event_sources.SqsEventSource(
                continuation_queue,
                batch_size=1,
                max_batching_window=Duration.seconds(0),
                enabled=True,
            )
        )

        budget_guard_fn = lambda_.Function(
            self,
            "BudgetGuardLambda",
            function_name=budget_guard_function_name,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../budget_guard_src"),
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "SYNC_FUNCTION_NAME": fn.function_name,
                "SYNC_SCHEDULE_NAME": schedule_name,
                "SYNC_SCHEDULE_GROUP": "default",
                "CONTINUATION_QUEUE_ARN": continuation_queue.queue_arn,
                "TARGET_RESERVED_CONCURRENCY": (
                    str(reserved_concurrency)
                    if reserved_concurrency is not None
                    else "1"
                ),
                "BUDGET_GUARD_STATUS_PARAM": budget_guard_status_ssm_param,
                "AWS_ACCOUNT_ID": self.account,
                "BUDGET_NAME": budget_name,
                "BUDGET_LIMIT_USD": budget_limit_usd,
            },
        )
        budget_guard_status_resource_path = budget_guard_status_ssm_param.lstrip("/")
        budget_guard_status_arn = (
            f"arn:aws:ssm:{self.region}:{self.account}:parameter/{budget_guard_status_resource_path}"
        )
        budget_guard_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:PutFunctionConcurrency"],
                resources=[lambda_function_arn],
            )
        )
        budget_guard_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "lambda:ListEventSourceMappings",
                    "lambda:UpdateEventSourceMapping",
                ],
                resources=["*"],
            )
        )
        budget_guard_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["scheduler:GetSchedule", "scheduler:UpdateSchedule"],
                resources=[
                    f"arn:aws:scheduler:{self.region}:{self.account}:schedule/default/{schedule_name}"
                ],
            )
        )
        budget_guard_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:PutParameter"],
                resources=[budget_guard_status_arn],
            )
        )
        budget_guard_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["budgets:DescribeBudget", "budgets:ViewBudget"],
                resources=["*"],
            )
        )

        scheduler_role = iam.Role(
            self,
            "SchedulerInvokeRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
            description="Execution role for EventBridge Scheduler to invoke Comarch-BaseLinker sync Lambda",
        )
        scheduler_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction"],
                resources=[lambda_function_arn, f"{lambda_function_arn}:*"],
            )
        )

        scheduler.CfnSchedule(
            self,
            "DailySchedule",
            name=schedule_name,
            group_name="default",
            state="ENABLED",
            schedule_expression=schedule_expression,
            schedule_expression_timezone=schedule_timezone,
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(mode="OFF"),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=lambda_function_arn,
                role_arn=scheduler_role.role_arn,
                input="{}",
                retry_policy=scheduler.CfnSchedule.RetryPolicyProperty(
                    maximum_event_age_in_seconds=86400,
                    maximum_retry_attempts=3,
                ),
            ),
        )

        budget_guard_scheduler_role = iam.Role(
            self,
            "BudgetGuardSchedulerInvokeRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
            description="Execution role for EventBridge Scheduler to re-enable Comarch-BaseLinker sync monthly",
        )
        budget_guard_scheduler_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction"],
                resources=[budget_guard_fn.function_arn, f"{budget_guard_fn.function_arn}:*"],
            )
        )
        scheduler.CfnSchedule(
            self,
            "BudgetGuardMonthlyEnableSchedule",
            name=budget_guard_monthly_schedule_name,
            group_name="default",
            state="ENABLED",
            schedule_expression="cron(5 0 1 * ? *)",
            schedule_expression_timezone="Europe/Warsaw",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(mode="OFF"),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=budget_guard_fn.function_arn,
                role_arn=budget_guard_scheduler_role.role_arn,
                input='{"action":"enable","reason":"monthly_budget_reset"}',
                retry_policy=scheduler.CfnSchedule.RetryPolicyProperty(
                    maximum_event_age_in_seconds=3600,
                    maximum_retry_attempts=2,
                ),
            ),
        )
        scheduler.CfnSchedule(
            self,
            "BudgetGuardHourlyCheckSchedule",
            name=budget_guard_hourly_schedule_name,
            group_name="default",
            state="ENABLED",
            schedule_expression="cron(0 * * * ? *)",
            schedule_expression_timezone="Europe/Warsaw",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(mode="OFF"),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=budget_guard_fn.function_arn,
                role_arn=budget_guard_scheduler_role.role_arn,
                input='{"action":"check","reason":"hourly_budget_check"}',
                retry_policy=scheduler.CfnSchedule.RetryPolicyProperty(
                    maximum_event_age_in_seconds=3600,
                    maximum_retry_attempts=2,
                ),
            ),
        )

        admin_fn = lambda_.Function(
            self,
            "SyncAdminLambda",
            function_name=admin_function_name,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="admin_lambda.lambda_handler",
            code=lambda_.Code.from_asset("../admin_src"),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "SYNC_STATUS_PARAM": bl_sync_status_ssm_param,
                "SYNC_CONFIG_PARAM": bl_sync_config_ssm_param,
                "SYNC_FUNCTION_NAME": fn.function_name,
                "DEFAULT_COMARCH_XML_URL": comarch_url,
                "DEFAULT_BL_INVENTORY_ID": bl_inventory_id,
                "DEFAULT_BL_WAREHOUSE_ID": bl_warehouse_id,
                "DEFAULT_BL_API_MAX_RPM": str(bl_api_max_rpm),
                "AWS_ACCOUNT_ID": self.account,
                "BUDGET_NAME": budget_name,
                "BUDGET_LIMIT_USD": budget_limit_usd,
                "BUDGET_USD_TO_PLN_RATE": budget_usd_to_pln_rate,
                "BUDGET_FX_RATE_SSM_PARAM": budget_fx_rate_ssm_param,
                "BL_API_TOKEN_SSM_PARAM": bl_api_token_ssm_param,
                "ADMIN_USERNAME": admin_username,
                "ADMIN_PASSWORD_SHA256": admin_password_hash,
                "BRAND_NAME": brand_name,
                "BRAND_PANEL_TITLE": brand_panel_title,
                "BRAND_PANEL_SUBTITLE": brand_panel_subtitle,
                "BRAND_PRIMARY_COLOR": brand_primary_color,
                "BRAND_PRIMARY_DARK_COLOR": brand_primary_dark_color,
                "BRAND_SECONDARY_COLOR": brand_secondary_color,
                "BRAND_LOGO_ENABLED": str(brand_logo_enabled).lower(),
                "SCHEDULE_NAME": schedule_name,
                "SCHEDULE_GROUP": "default",
            },
        )
        admin_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter"],
                resources=[param_arn],
            )
        )
        admin_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter", "ssm:PutParameter"],
                resources=[config_param_arn],
            )
        )
        admin_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter"],
                resources=[token_param_arn],
            )
        )
        admin_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter"],
                resources=[budget_fx_param_arn],
            )
        )
        admin_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction"],
                resources=[lambda_function_arn, f"{lambda_function_arn}:*"],
            )
        )
        admin_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["scheduler:GetSchedule"],
                resources=[
                    f"arn:aws:scheduler:{self.region}:{self.account}:schedule/default/{schedule_name}"
                ],
            )
        )
        admin_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["budgets:DescribeBudget", "budgets:ViewBudget"],
                resources=["*"],
            )
        )

        admin_api = apigwv2.HttpApi(
            self,
            "SyncAdminHttpApi",
            api_name=admin_api_name,
        )
        admin_integration = apigwv2_integrations.HttpLambdaIntegration(
            "SyncAdminIntegration",
            admin_fn,
        )
        admin_api.add_routes(
            path="/",
            methods=[apigwv2.HttpMethod.GET],
            integration=admin_integration,
        )
        admin_api.add_routes(
            path="/admin",
            methods=[apigwv2.HttpMethod.GET],
            integration=admin_integration,
        )
        admin_api.add_routes(
            path="/assets/client-logo.png",
            methods=[apigwv2.HttpMethod.GET],
            integration=admin_integration,
        )
        admin_api.add_routes(
            path="/assets/favicon.svg",
            methods=[apigwv2.HttpMethod.GET],
            integration=admin_integration,
        )
        admin_api.add_routes(
            path="/api/status",
            methods=[apigwv2.HttpMethod.GET],
            integration=admin_integration,
        )
        admin_api.add_routes(
            path="/api/config",
            methods=[apigwv2.HttpMethod.GET],
            integration=admin_integration,
        )
        admin_api.add_routes(
            path="/api/sync",
            methods=[apigwv2.HttpMethod.POST],
            integration=admin_integration,
        )

        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "FeedObjectKey", value=output_key)
        CfnOutput(self, "FeedUrl", value=f"https://{bucket.bucket_name}.s3.{self.region}.amazonaws.com/{output_key}")
        CfnOutput(self, "LambdaName", value=fn.function_name)
        CfnOutput(
            self,
            "LambdaReservedConcurrency",
            value=(
                str(reserved_concurrency)
                if reserved_concurrency is not None
                else "unreserved"
            ),
        )
        CfnOutput(self, "ContinuationQueueUrl", value=continuation_queue.queue_url)
        CfnOutput(self, "ContinuationQueueArn", value=continuation_queue.queue_arn)
        CfnOutput(self, "ScheduleName", value=schedule_name)
        CfnOutput(self, "BudgetName", value=budget_name)
        CfnOutput(self, "BudgetGuardLambdaName", value=budget_guard_fn.function_name)
        CfnOutput(self, "AdminUrl", value=admin_api.url or "")
