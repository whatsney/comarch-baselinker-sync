#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import sys
from typing import Any, Iterable, Sequence

import boto3
from botocore.exceptions import BotoCoreError, ClientError, WaiterError


DEFAULT_APP_REGION = "eu-north-1"
DEFAULT_BUDGET_REGION = "us-east-1"
DEFAULT_PIPELINE_STACK_NAME = "ComarchBaseLinkerSyncStack"
DEFAULT_BUDGET_STACK_NAME = "ComarchBaseLinkerBudgetStack"
DEFAULT_CDK_BOOTSTRAP_STACK_NAME = "CDKToolkit"
DEFAULT_CDK_QUALIFIER = "hnb659fds"
DEFAULT_BUDGET_NAME = "comarch-baselinker-sync-monthly-budget"
DEFAULT_FUNCTION_NAMES = (
    "comarch-baselinker-sync",
    "comarch-baselinker-sync-admin",
    "comarch-baselinker-budget-guard",
)
DEFAULT_SSM_PARAMETER_NAMES = (
    "/comarch-baselinker-sync/api-token",
    "/comarch-baselinker-sync/push-sync-status",
    "/comarch-baselinker-sync/sync-config",
    "/comarch-baselinker-sync/usd-pln-rate",
    "/comarch-baselinker-sync/budget-guard-status",
)


@dataclass(frozen=True)
class TeardownConfig:
    profile: str | None
    app_region: str
    budget_region: str
    expected_account_id: str
    apply: bool
    confirm_account_id: str
    pipeline_stack_name: str
    budget_stack_name: str
    retained_bucket_names: tuple[str, ...] = field(default_factory=tuple)
    ssm_parameter_names: tuple[str, ...] = DEFAULT_SSM_PARAMETER_NAMES
    ssm_parameter_prefixes: tuple[str, ...] = field(default_factory=tuple)
    log_group_names: tuple[str, ...] = field(default_factory=tuple)
    budget_names: tuple[str, ...] = (DEFAULT_BUDGET_NAME,)
    sns_topic_arns: tuple[str, ...] = field(default_factory=tuple)
    delete_cdk_bootstrap: bool = False
    cdk_bootstrap_stack_name: str = DEFAULT_CDK_BOOTSTRAP_STACK_NAME
    cdk_qualifier: str = DEFAULT_CDK_QUALIFIER
    iam_user_names: tuple[str, ...] = field(default_factory=tuple)
    iam_role_names: tuple[str, ...] = field(default_factory=tuple)
    final_assume_role_arn: str = ""
    wait_for_stacks: bool = True


class TeardownReporter:
    def __init__(self, should_apply: bool) -> None:
        self.should_apply = should_apply

    def info(self, message: str) -> None:
        print(message)

    def action(self, message: str) -> None:
        prefix = "APPLY" if self.should_apply else "DRY-RUN"
        print(f"[{prefix}] {message}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Safely tear down a deployed XML to BaseLinker AWS environment. "
            "The default mode is a read-only dry run."
        )
    )
    parser.add_argument("--profile", help="AWS CLI profile to use.")
    parser.add_argument("--app-region", default=DEFAULT_APP_REGION)
    parser.add_argument("--budget-region", default=DEFAULT_BUDGET_REGION)
    parser.add_argument("--expected-account-id", required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform destructive changes. Omit this flag for a dry run.",
    )
    parser.add_argument(
        "--confirm-account-id",
        default="",
        help="Required with --apply and must match --expected-account-id.",
    )
    parser.add_argument("--pipeline-stack-name", default=DEFAULT_PIPELINE_STACK_NAME)
    parser.add_argument("--budget-stack-name", default=DEFAULT_BUDGET_STACK_NAME)
    parser.add_argument(
        "--retained-bucket-name",
        action="append",
        default=[],
        help="Retained application bucket to empty and delete. Repeat for more buckets.",
    )
    parser.add_argument(
        "--ssm-parameter-name",
        action="append",
        default=[],
        help=(
            "Exact SSM parameter to delete. Defaults cover the repository's "
            "standard parameter names unless --no-default-ssm-parameters is set."
        ),
    )
    parser.add_argument(
        "--no-default-ssm-parameters",
        action="store_true",
        help="Do not delete the repository's default SSM parameter names.",
    )
    parser.add_argument(
        "--ssm-parameter-prefix",
        action="append",
        default=[],
        help=(
            "SSM parameter prefix to delete. Use only after inventorying the account; "
            "the script deletes names, never parameter values."
        ),
    )
    parser.add_argument(
        "--function-name",
        action="append",
        default=[],
        help=(
            "Lambda function whose /aws/lambda/<name> log group should be deleted. "
            "Defaults cover the repository's standard function names."
        ),
    )
    parser.add_argument(
        "--no-default-log-groups",
        action="store_true",
        help="Do not delete log groups for the repository's default function names.",
    )
    parser.add_argument(
        "--budget-name",
        action="append",
        default=[],
        help=(
            "AWS Budget name to delete from the budget region. Defaults to the "
            "repository's standard budget name unless --no-default-budget is set."
        ),
    )
    parser.add_argument(
        "--no-default-budget",
        action="store_true",
        help="Do not delete the repository's default AWS Budget name.",
    )
    parser.add_argument(
        "--sns-topic-arn",
        action="append",
        default=[],
        help="Extra SNS topic ARN to delete. Repeat for more topics.",
    )
    parser.add_argument(
        "--delete-cdk-bootstrap",
        action="store_true",
        help=(
            "Delete CDKToolkit and the CDK asset buckets in the app and budget regions. "
            "Only use when the account has no other CDK deployments."
        ),
    )
    parser.add_argument("--cdk-bootstrap-stack-name", default=DEFAULT_CDK_BOOTSTRAP_STACK_NAME)
    parser.add_argument("--cdk-qualifier", default=DEFAULT_CDK_QUALIFIER)
    parser.add_argument(
        "--iam-user-name",
        action="append",
        default=[],
        help="IAM user to delete during final cleanup. Repeat for more users.",
    )
    parser.add_argument(
        "--iam-role-name",
        action="append",
        default=[],
        help="IAM role to delete during final cleanup. Repeat for more roles.",
    )
    parser.add_argument(
        "--final-assume-role-arn",
        default="",
        help=(
            "Role to assume before IAM cleanup. Use this when deleting the current "
            "profile's IAM user; the assumed session remains valid long enough to "
            "delete that user and then the role itself."
        ),
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Start stack deletion but do not wait for completion.",
    )
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> TeardownConfig:
    ssm_parameter_names = list(args.ssm_parameter_name)
    if not args.no_default_ssm_parameters:
        ssm_parameter_names.extend(DEFAULT_SSM_PARAMETER_NAMES)

    function_names = list(args.function_name)
    if not args.no_default_log_groups:
        function_names.extend(DEFAULT_FUNCTION_NAMES)
    log_group_names = tuple(f"/aws/lambda/{function_name}" for function_name in unique_texts(function_names))

    budget_names = list(args.budget_name)
    if not args.no_default_budget:
        budget_names.append(DEFAULT_BUDGET_NAME)

    return TeardownConfig(
        profile=args.profile,
        app_region=args.app_region,
        budget_region=args.budget_region,
        expected_account_id=args.expected_account_id,
        apply=args.apply,
        confirm_account_id=args.confirm_account_id,
        pipeline_stack_name=args.pipeline_stack_name,
        budget_stack_name=args.budget_stack_name,
        retained_bucket_names=tuple(unique_texts(args.retained_bucket_name)),
        ssm_parameter_names=tuple(unique_texts(ssm_parameter_names)),
        ssm_parameter_prefixes=tuple(unique_texts(args.ssm_parameter_prefix)),
        log_group_names=log_group_names,
        budget_names=tuple(unique_texts(budget_names)),
        sns_topic_arns=tuple(unique_texts(args.sns_topic_arn)),
        delete_cdk_bootstrap=args.delete_cdk_bootstrap,
        cdk_bootstrap_stack_name=args.cdk_bootstrap_stack_name,
        cdk_qualifier=args.cdk_qualifier,
        iam_user_names=tuple(unique_texts(args.iam_user_name)),
        iam_role_names=tuple(unique_texts(args.iam_role_name)),
        final_assume_role_arn=args.final_assume_role_arn,
        wait_for_stacks=not args.no_wait,
    )


def unique_texts(values: Iterable[str]) -> list[str]:
    unique_values: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned_value = value.strip()
        if not cleaned_value or cleaned_value in seen:
            continue
        seen.add(cleaned_value)
        unique_values.append(cleaned_value)
    return unique_values


def validate_config(config: TeardownConfig) -> None:
    if not config.apply:
        return
    if config.confirm_account_id != config.expected_account_id:
        raise ValueError(
            "--apply requires --confirm-account-id with the same value as "
            "--expected-account-id."
        )


def aws_session(config: TeardownConfig) -> boto3.Session:
    return boto3.Session(profile_name=config.profile, region_name=config.app_region)


def verify_account(session: boto3.Session, expected_account_id: str) -> dict[str, str]:
    identity = session.client("sts").get_caller_identity()
    account_id = identity["Account"]
    if account_id != expected_account_id:
        raise RuntimeError(
            f"Refusing teardown in account {account_id}; expected {expected_account_id}."
        )
    return {"account": account_id, "arn": identity.get("Arn", "")}


def assert_current_user_can_be_deleted(
    identity_arn: str,
    iam_user_names: Sequence[str],
    final_assume_role_arn: str,
) -> None:
    if final_assume_role_arn:
        return
    for user_name in iam_user_names:
        if identity_arn.endswith(f":user/{user_name}"):
            raise RuntimeError(
                f"Refusing to delete current IAM user {user_name} without "
                "--final-assume-role-arn."
            )


def delete_stack(
    session: boto3.Session,
    region: str,
    stack_name: str,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    cloudformation = session.client("cloudformation", region_name=region)
    if not stack_exists(cloudformation, stack_name):
        reporter.info(f"CloudFormation stack absent in {region}: {stack_name}")
        return

    reporter.action(f"delete CloudFormation stack {stack_name} in {region}")
    if not config.apply:
        return

    cloudformation.delete_stack(StackName=stack_name)
    if not config.wait_for_stacks:
        return

    waiter = cloudformation.get_waiter("stack_delete_complete")
    waiter.wait(StackName=stack_name, WaiterConfig={"Delay": 20, "MaxAttempts": 90})


def stack_exists(cloudformation_client: Any, stack_name: str) -> bool:
    try:
        cloudformation_client.describe_stacks(StackName=stack_name)
        return True
    except ClientError as error:
        if "does not exist" in str(error):
            return False
        raise


def delete_budget(
    session: boto3.Session,
    budget_name: str,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    budgets = session.client("budgets", region_name=config.budget_region)
    if not budget_exists(budgets, config.expected_account_id, budget_name):
        reporter.info(f"AWS Budget absent: {budget_name}")
        return

    reporter.action(f"delete AWS Budget {budget_name}")
    if config.apply:
        budgets.delete_budget(AccountId=config.expected_account_id, BudgetName=budget_name)


def budget_exists(budgets_client: Any, account_id: str, budget_name: str) -> bool:
    try:
        budgets_client.describe_budget(AccountId=account_id, BudgetName=budget_name)
        return True
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "NotFoundException":
            return False
        raise


def delete_sns_topic(
    session: boto3.Session,
    topic_arn: str,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    region = region_from_arn(topic_arn) or config.app_region
    sns = session.client("sns", region_name=region)
    reporter.action(f"delete SNS topic {topic_arn}")
    if config.apply:
        sns.delete_topic(TopicArn=topic_arn)


def delete_exact_ssm_parameter(
    session: boto3.Session,
    parameter_name: str,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    ssm = session.client("ssm", region_name=config.app_region)
    if not ssm_parameter_exists(ssm, parameter_name):
        reporter.info(f"SSM parameter absent: {parameter_name}")
        return

    reporter.action(f"delete SSM parameter {parameter_name}")
    if config.apply:
        ssm.delete_parameter(Name=parameter_name)


def ssm_parameter_exists(ssm_client: Any, parameter_name: str) -> bool:
    try:
        ssm_client.get_parameter(Name=parameter_name, WithDecryption=False)
        return True
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "ParameterNotFound":
            return False
        raise


def delete_ssm_parameter_prefix(
    session: boto3.Session,
    parameter_prefix: str,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    ssm = session.client("ssm", region_name=config.app_region)
    parameter_names = list_ssm_parameter_names_by_prefix(ssm, parameter_prefix)
    if not parameter_names:
        reporter.info(f"No SSM parameters found under prefix: {parameter_prefix}")
        return

    for parameter_name in parameter_names:
        reporter.action(f"delete SSM parameter {parameter_name}")
        if config.apply:
            ssm.delete_parameter(Name=parameter_name)


def list_ssm_parameter_names_by_prefix(ssm_client: Any, parameter_prefix: str) -> list[str]:
    parameter_names: list[str] = []
    paginator = ssm_client.get_paginator("describe_parameters")
    pages = paginator.paginate(
        ParameterFilters=[
            {
                "Key": "Name",
                "Option": "BeginsWith",
                "Values": [parameter_prefix],
            }
        ]
    )
    for page in pages:
        for parameter in page.get("Parameters", []):
            parameter_name = parameter.get("Name")
            if parameter_name:
                parameter_names.append(parameter_name)
    return sorted(parameter_names)


def delete_log_group(
    session: boto3.Session,
    log_group_name: str,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    logs = session.client("logs", region_name=config.app_region)
    if not log_group_exists(logs, log_group_name):
        reporter.info(f"CloudWatch log group absent: {log_group_name}")
        return

    reporter.action(f"delete CloudWatch log group {log_group_name}")
    if config.apply:
        logs.delete_log_group(logGroupName=log_group_name)


def log_group_exists(logs_client: Any, log_group_name: str) -> bool:
    response = logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)
    return any(group.get("logGroupName") == log_group_name for group in response.get("logGroups", []))


def empty_and_delete_bucket(
    session: boto3.Session,
    bucket_name: str,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    s3 = session.client("s3")
    if not bucket_exists(s3, bucket_name):
        reporter.info(f"S3 bucket absent: {bucket_name}")
        return

    object_count = count_bucket_object_versions(s3, bucket_name)
    reporter.action(f"empty {object_count} object versions/delete markers from S3 bucket {bucket_name}")
    reporter.action(f"delete S3 bucket {bucket_name}")
    if not config.apply:
        return

    delete_all_bucket_object_versions(s3, bucket_name)
    s3.delete_bucket(Bucket=bucket_name)


def bucket_exists(s3_client: Any, bucket_name: str) -> bool:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"404", "NoSuchBucket"}:
            return False
        raise


def count_bucket_object_versions(s3_client: Any, bucket_name: str) -> int:
    count = 0
    paginator = s3_client.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket_name):
        count += len(page.get("Versions", []))
        count += len(page.get("DeleteMarkers", []))
    return count


def delete_all_bucket_object_versions(s3_client: Any, bucket_name: str) -> int:
    deleted_count = 0
    while True:
        object_identifiers = list_bucket_object_version_identifiers(s3_client, bucket_name)
        if not object_identifiers:
            return deleted_count

        response = s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": object_identifiers, "Quiet": True},
        )
        delete_errors = response.get("Errors", [])
        if delete_errors:
            raise RuntimeError(f"Failed to delete objects from {bucket_name}: {delete_errors}")
        deleted_count += len(object_identifiers)


def list_bucket_object_version_identifiers(s3_client: Any, bucket_name: str) -> list[dict[str, str]]:
    object_identifiers: list[dict[str, str]] = []
    paginator = s3_client.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket_name):
        for version in page.get("Versions", []):
            object_identifiers.append(
                {
                    "Key": version["Key"],
                    "VersionId": version["VersionId"],
                }
            )
        for marker in page.get("DeleteMarkers", []):
            object_identifiers.append(
                {
                    "Key": marker["Key"],
                    "VersionId": marker["VersionId"],
                }
            )
        if len(object_identifiers) >= 1000:
            return object_identifiers
    return object_identifiers


def delete_cdk_bootstrap(
    session: boto3.Session,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    for region in unique_texts([config.app_region, config.budget_region]):
        bucket_name = cdk_asset_bucket_name(config.cdk_qualifier, config.expected_account_id, region)
        empty_and_delete_bucket(session, bucket_name, config, reporter)
        delete_stack(session, region, config.cdk_bootstrap_stack_name, config, reporter)


def cdk_asset_bucket_name(cdk_qualifier: str, account_id: str, region: str) -> str:
    return f"cdk-{cdk_qualifier}-assets-{account_id}-{region}"


def assume_final_iam_session(
    session: boto3.Session,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> boto3.Session:
    if not config.final_assume_role_arn:
        return session

    reporter.action(f"assume final IAM cleanup role {config.final_assume_role_arn}")
    if not config.apply:
        return session

    sts = session.client("sts")
    response = sts.assume_role(
        RoleArn=config.final_assume_role_arn,
        RoleSessionName="xml-baselinker-teardown-final-iam",
    )
    credentials = response["Credentials"]
    return boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name=config.app_region,
    )


def delete_iam_users_and_roles(
    session: boto3.Session,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    if not config.iam_user_names and not config.iam_role_names:
        return

    iam_session = assume_final_iam_session(session, config, reporter)
    iam = iam_session.client("iam")
    final_role_name = role_name_from_arn(config.final_assume_role_arn)

    for user_name in config.iam_user_names:
        delete_iam_user(iam, user_name, config, reporter)

    for role_name in config.iam_role_names:
        if role_name != final_role_name:
            delete_iam_role(iam, role_name, config, reporter)
    if final_role_name and final_role_name in config.iam_role_names:
        delete_iam_role(iam, final_role_name, config, reporter)


def delete_iam_user(
    iam: Any,
    user_name: str,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    if not iam_user_exists(iam, user_name):
        reporter.info(f"IAM user absent: {user_name}")
        return

    reporter.action(f"delete IAM user {user_name} and its credentials/policies")
    if not config.apply:
        return

    ignore_no_such_entity(iam.delete_login_profile, UserName=user_name)
    delete_user_access_keys(iam, user_name)
    delete_user_signing_certificates(iam, user_name)
    delete_user_ssh_public_keys(iam, user_name)
    delete_user_service_specific_credentials(iam, user_name)
    delete_user_mfa_devices(iam, user_name)
    detach_user_policies(iam, user_name)
    delete_user_inline_policies(iam, user_name)
    remove_user_from_groups(iam, user_name)
    ignore_no_such_entity(iam.delete_user_permissions_boundary, UserName=user_name)
    iam.delete_user(UserName=user_name)


def iam_user_exists(iam: Any, user_name: str) -> bool:
    try:
        iam.get_user(UserName=user_name)
        return True
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "NoSuchEntity":
            return False
        raise


def delete_user_access_keys(iam: Any, user_name: str) -> None:
    for page in iam.get_paginator("list_access_keys").paginate(UserName=user_name):
        for access_key in page.get("AccessKeyMetadata", []):
            iam.delete_access_key(UserName=user_name, AccessKeyId=access_key["AccessKeyId"])


def delete_user_signing_certificates(iam: Any, user_name: str) -> None:
    for page in iam.get_paginator("list_signing_certificates").paginate(UserName=user_name):
        for certificate in page.get("Certificates", []):
            iam.delete_signing_certificate(UserName=user_name, CertificateId=certificate["CertificateId"])


def delete_user_ssh_public_keys(iam: Any, user_name: str) -> None:
    for page in iam.get_paginator("list_ssh_public_keys").paginate(UserName=user_name):
        for public_key in page.get("SSHPublicKeys", []):
            iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=public_key["SSHPublicKeyId"])


def delete_user_service_specific_credentials(iam: Any, user_name: str) -> None:
    response = iam.list_service_specific_credentials(UserName=user_name)
    for credential in response.get("ServiceSpecificCredentials", []):
        iam.delete_service_specific_credential(
            UserName=user_name,
            ServiceSpecificCredentialId=credential["ServiceSpecificCredentialId"],
        )


def delete_user_mfa_devices(iam: Any, user_name: str) -> None:
    for page in iam.get_paginator("list_mfa_devices").paginate(UserName=user_name):
        for device in page.get("MFADevices", []):
            serial_number = device["SerialNumber"]
            iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial_number)
            if ":mfa/" in serial_number:
                ignore_no_such_entity(iam.delete_virtual_mfa_device, SerialNumber=serial_number)


def detach_user_policies(iam: Any, user_name: str) -> None:
    for page in iam.get_paginator("list_attached_user_policies").paginate(UserName=user_name):
        for policy in page.get("AttachedPolicies", []):
            iam.detach_user_policy(UserName=user_name, PolicyArn=policy["PolicyArn"])


def delete_user_inline_policies(iam: Any, user_name: str) -> None:
    for page in iam.get_paginator("list_user_policies").paginate(UserName=user_name):
        for policy_name in page.get("PolicyNames", []):
            iam.delete_user_policy(UserName=user_name, PolicyName=policy_name)


def remove_user_from_groups(iam: Any, user_name: str) -> None:
    for page in iam.get_paginator("list_groups_for_user").paginate(UserName=user_name):
        for group in page.get("Groups", []):
            iam.remove_user_from_group(GroupName=group["GroupName"], UserName=user_name)


def delete_iam_role(
    iam: Any,
    role_name: str,
    config: TeardownConfig,
    reporter: TeardownReporter,
) -> None:
    if not iam_role_exists(iam, role_name):
        reporter.info(f"IAM role absent: {role_name}")
        return

    reporter.action(f"delete IAM role {role_name} and its policies")
    if not config.apply:
        return

    remove_role_from_instance_profiles(iam, role_name)
    detach_role_policies(iam, role_name)
    delete_role_inline_policies(iam, role_name)
    ignore_no_such_entity(iam.delete_role_permissions_boundary, RoleName=role_name)
    iam.delete_role(RoleName=role_name)


def iam_role_exists(iam: Any, role_name: str) -> bool:
    try:
        iam.get_role(RoleName=role_name)
        return True
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "NoSuchEntity":
            return False
        raise


def remove_role_from_instance_profiles(iam: Any, role_name: str) -> None:
    for page in iam.get_paginator("list_instance_profiles_for_role").paginate(RoleName=role_name):
        for instance_profile in page.get("InstanceProfiles", []):
            iam.remove_role_from_instance_profile(
                InstanceProfileName=instance_profile["InstanceProfileName"],
                RoleName=role_name,
            )


def detach_role_policies(iam: Any, role_name: str) -> None:
    for page in iam.get_paginator("list_attached_role_policies").paginate(RoleName=role_name):
        for policy in page.get("AttachedPolicies", []):
            iam.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])


def delete_role_inline_policies(iam: Any, role_name: str) -> None:
    for page in iam.get_paginator("list_role_policies").paginate(RoleName=role_name):
        for policy_name in page.get("PolicyNames", []):
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)


def ignore_no_such_entity(function: Any, **kwargs: Any) -> None:
    try:
        function(**kwargs)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "NoSuchEntity":
            return
        raise


def region_from_arn(arn: str) -> str:
    parts = arn.split(":")
    if len(parts) > 3:
        return parts[3]
    return ""


def role_name_from_arn(role_arn: str) -> str:
    if not role_arn:
        return ""
    return role_arn.rsplit("/", 1)[-1]


def run_teardown(config: TeardownConfig) -> None:
    validate_config(config)
    session = aws_session(config)
    identity = verify_account(session, config.expected_account_id)
    assert_current_user_can_be_deleted(
        identity_arn=identity["arn"],
        iam_user_names=config.iam_user_names,
        final_assume_role_arn=config.final_assume_role_arn,
    )

    reporter = TeardownReporter(should_apply=config.apply)
    mode = "apply" if config.apply else "dry-run"
    reporter.info(
        f"Starting {mode} teardown in account {identity['account']} "
        f"with caller {identity['arn']}."
    )

    delete_stack(session, config.budget_region, config.budget_stack_name, config, reporter)
    delete_stack(session, config.app_region, config.pipeline_stack_name, config, reporter)

    for budget_name in config.budget_names:
        delete_budget(session, budget_name, config, reporter)
    for topic_arn in config.sns_topic_arns:
        delete_sns_topic(session, topic_arn, config, reporter)
    for parameter_name in config.ssm_parameter_names:
        delete_exact_ssm_parameter(session, parameter_name, config, reporter)
    for parameter_prefix in config.ssm_parameter_prefixes:
        delete_ssm_parameter_prefix(session, parameter_prefix, config, reporter)
    for log_group_name in config.log_group_names:
        delete_log_group(session, log_group_name, config, reporter)
    for bucket_name in config.retained_bucket_names:
        empty_and_delete_bucket(session, bucket_name, config, reporter)
    if config.delete_cdk_bootstrap:
        delete_cdk_bootstrap(session, config, reporter)
    delete_iam_users_and_roles(session, config, reporter)

    reporter.info("Teardown run finished.")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        config = build_config(args)
        run_teardown(config)
        return 0
    except (BotoCoreError, ClientError, RuntimeError, ValueError, WaiterError) as error:
        print(f"Teardown failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
