import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEARDOWN_PATH = ROOT / "scripts" / "teardown_aws_account.py"

module_spec = importlib.util.spec_from_file_location("teardown_aws_account", TEARDOWN_PATH)
teardown = importlib.util.module_from_spec(module_spec)
sys.modules[module_spec.name] = teardown
module_spec.loader.exec_module(teardown)


class _FakePaginator:
    def __init__(self, page_factory):
        self._page_factory = page_factory

    def paginate(self, **_kwargs):
        return self._page_factory()


class _FakeS3Client:
    def __init__(self):
        self.deleted_batches = []
        self.versions = [
            {"Key": "feeds/baselinker/products.bl-sync-state.json", "VersionId": "v1"},
            {"Key": "feeds/baselinker/products.bl-sync-source.xml", "VersionId": "v2"},
        ]
        self.delete_markers = [
            {"Key": "feeds/baselinker/old.json", "VersionId": "m1"},
        ]

    def get_paginator(self, operation_name):
        if operation_name != "list_object_versions":
            raise AssertionError(f"Unexpected paginator: {operation_name}")
        return _FakePaginator(self._list_object_versions_pages)

    def _list_object_versions_pages(self):
        if not self.versions and not self.delete_markers:
            return []
        return [
            {
                "Versions": list(self.versions),
                "DeleteMarkers": list(self.delete_markers),
            }
        ]

    def delete_objects(self, **kwargs):
        self.deleted_batches.append(kwargs["Delete"]["Objects"])
        self.versions.clear()
        self.delete_markers.clear()
        return {}


class TestTeardownConfig(unittest.TestCase):
    def test_apply_requires_matching_account_confirmation(self):
        args = teardown.parse_args(
            [
                "--expected-account-id",
                "111111111111",
                "--apply",
                "--confirm-account-id",
                "222222222222",
            ]
        )
        config = teardown.build_config(args)

        with self.assertRaises(ValueError):
            teardown.validate_config(config)

    def test_current_user_deletion_requires_final_assume_role(self):
        with self.assertRaises(RuntimeError):
            teardown.assert_current_user_can_be_deleted(
                identity_arn="arn:aws:iam::111111111111:user/DeployUser",
                iam_user_names=("DeployUser",),
                final_assume_role_arn="",
            )

    def test_default_log_groups_are_derived_from_function_names(self):
        args = teardown.parse_args(["--expected-account-id", "111111111111"])
        config = teardown.build_config(args)

        self.assertIn("/aws/lambda/baselinker-sync", config.log_group_names)
        self.assertIn("/aws/lambda/baselinker-sync-admin", config.log_group_names)
        self.assertIn("/aws/lambda/baselinker-budget-guard", config.log_group_names)

    def test_cdk_asset_bucket_name_uses_qualifier_account_and_region(self):
        bucket_name = teardown.cdk_asset_bucket_name(
            cdk_qualifier="hnb659fds",
            account_id="111111111111",
            region="eu-north-1",
        )

        self.assertEqual(bucket_name, "cdk-hnb659fds-assets-111111111111-eu-north-1")


class TestBucketCleanup(unittest.TestCase):
    def test_delete_all_bucket_object_versions_deletes_versions_and_markers(self):
        fake_s3 = _FakeS3Client()

        deleted_count = teardown.delete_all_bucket_object_versions(
            fake_s3,
            "example-retained-bucket",
        )

        self.assertEqual(deleted_count, 3)
        self.assertEqual(
            fake_s3.deleted_batches,
            [
                [
                    {
                        "Key": "feeds/baselinker/products.bl-sync-state.json",
                        "VersionId": "v1",
                    },
                    {
                        "Key": "feeds/baselinker/products.bl-sync-source.xml",
                        "VersionId": "v2",
                    },
                    {
                        "Key": "feeds/baselinker/old.json",
                        "VersionId": "m1",
                    },
                ]
            ],
        )


if __name__ == "__main__":
    unittest.main()
