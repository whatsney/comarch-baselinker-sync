# AWS account migration runbook

This runbook moves the active deployment to another AWS account while keeping
the old account disabled as a rollback environment. It assumes temporary AWS
credentials, an application stack in `eu-north-1`, and a budget stack in
`us-east-1`.

Never place access keys, session tokens, BaseLinker tokens, private feed URLs,
account IDs, or customer configuration in this repository or command output.

## Account isolation

Use two named AWS CLI profiles and two GitHub Environments:

| Role | CLI profile example | GitHub Environment |
| --- | --- | --- |
| Current account | `migration-source` | `source-rollback` |
| Client account | `migration-client` | `client-production` |

Each GitHub Environment must have its own temporary AWS credential set,
resource names, and `EXPECTED_AWS_ACCOUNT_ID`. Every plan or deployment stops
if STS returns a different account.

For ongoing deployments after migration, prefer a dedicated IAM user whose
only permission is `sts:AssumeRole` on the account-specific deployment role.
Store that user's access key pair as environment secrets and set the role ARN
as `AWS_ROLE_ARN`. Do not grant infrastructure permissions directly to the IAM
user, and remove `AWS_SESSION_TOKEN` when the stored key pair is long-lived.

Before any live command, set and verify the intended profile explicitly:

```bash
export SOURCE_AWS_PROFILE=migration-source
export TARGET_AWS_PROFILE=migration-client
export TARGET_REGION=eu-north-1

aws sts get-caller-identity --profile "$SOURCE_AWS_PROFILE" --query Account --output text
aws sts get-caller-identity --profile "$TARGET_AWS_PROFILE" --query Account --output text
```

Compare both results with the separately supplied expected account IDs. Do not
continue when either value is missing, expired, or assigned to the wrong role.

## Phase A: prepare

1. Set `SYNC_ENABLED=false` in `client-production`.
2. Set `SCHEDULE_EXPRESSION=cron(0 0,12,17 * * ? *)` and
   `SCHEDULE_TIMEZONE=Europe/Warsaw`.
3. Set a new, explicit, globally unique `BUCKET_NAME`. Do not reuse the source
   bucket name.
4. Set `XML_URL` to the active source XML URL, then configure only the
   BaseLinker inventory, warehouse, request limit, and token. Do not copy
   synchronization status or state.
5. Run unit tests, source compilation, repository safety checks, and CDK synth.
6. Run the deployment workflow with operation `DIFF` and review both stacks.
7. Confirm that the target template has:
   - the sync EventBridge schedule set to `DISABLED`,
   - the SQS event source mapping set to disabled,
   - sync Lambda reserved concurrency set to `0`,
   - the monthly budget reset schedule set to `DISABLED`.

## Phase B: deploy paused

1. Verify the client account through STS.
2. Bootstrap CDK in `eu-north-1` and `us-east-1`.
3. Store the BaseLinker token as an SSM `SecureString` without printing it.
4. Deploy the application and budget stacks with `SYNC_ENABLED=false`.
5. Confirm the email subscriptions for budget and post-sync audit alerts.
6. Inventory and verify the admin URL, Lambdas, SQS queue and mapping, SSM
   parameters, S3 bucket, budget, and all EventBridge schedules.
7. Confirm again that the sync Lambda has concurrency `0`, the SQS mapping is
   disabled, and the sync schedule is disabled.
8. Do not invoke synchronization.

## Phase C: cut over

Perform cutover during the day and outside the configured synchronization
times.

1. In the source account, verify the latest run completed, post-audit ran, and
   `diff_total=0`.
2. Disable the source sync schedule, SQS event source mapping, and sync Lambda
   by setting reserved concurrency to `0`.
3. Wait until Lambda has no active invocations and both visible and in-flight
   SQS message counts are zero.
4. Keep the source bucket unchanged. Do not copy its objects or source SSM
   status/state parameters.
5. Re-verify the target `XML_URL`, inventory, warehouse, request limit, and
   token path.
6. Run target `DIFF` with `SYNC_ENABLED=true`. Confirm that only the target
   sync entry points and monthly budget reset move to enabled state.
7. Deploy the reviewed target change. Never enable the source and target at the
   same time.
8. Start one manual target synchronization. A fresh target bucket forces the
   intended cold start.
9. Accept the cutover only after all conditions are true:
   - synchronization completed successfully,
   - post-audit completed with `diff_total=0`,
   - SQS visible and in-flight counts are zero,
   - target S3 contains the new state, snapshot, and audit artifacts,
   - the admin panel shows the completed target run.
10. Observe the next scheduled runs at 12:00, 17:00, and 00:00 Warsaw time as
    applicable to the cutover time.

If the target fails, first set target `SYNC_ENABLED=false` and verify all three
entry-point blocks. Restore the source only after the target is confirmed
inactive.

## Phase D: observe and clean up

Keep the source account disabled for at least 24-48 hours. Before deletion,
record the exact identifiers and retention decision for:

- application and budget CloudFormation stacks in both regions,
- the retained source S3 bucket and all remaining objects,
- every SSM parameter, including token, status, runtime config, FX rate, and
  budget guard status,
- CloudWatch log groups for sync, admin, budget guard, and API components,
- deployment IAM users or roles, Lambda roles, and temporary credentials,
- AWS Budget definitions and subscribers,
- sync, monthly reset, and hourly budget EventBridge schedules,
- the SQS queue, event source mapping, SNS topics/subscriptions, API Gateway,
  and Lambda functions.

Stop after producing this inventory. Obtain explicit final approval before
deleting any source-account resource or emptying the retained bucket.

After approval, remove resources in a controlled order, then scan both regions
and the global billing view for remaining resources and costs. Revoke the
client's temporary credentials after migration verification is complete.
