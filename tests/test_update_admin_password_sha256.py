import argparse
import hashlib
import io
import importlib.util
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "update_admin_password_sha256.py"

module_spec = importlib.util.spec_from_file_location(
    "update_admin_password_sha256",
    SCRIPT_PATH,
)
update_admin_password = importlib.util.module_from_spec(module_spec)
sys.modules[module_spec.name] = update_admin_password
module_spec.loader.exec_module(update_admin_password)


class TestUpdateAdminPasswordArguments(unittest.TestCase):
    def test_expected_account_id_is_required(self):
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                update_admin_password.parse_args([])

    def test_defaults_are_deployment_neutral(self):
        args = update_admin_password.parse_args(["--expected-account-id", "111111111111"])

        self.assertIsNone(args.profile)
        self.assertEqual(args.region, "eu-north-1")
        self.assertEqual(args.function_name, "baselinker-sync-admin")


class TestUpdateAdminPasswordHashing(unittest.TestCase):
    def test_password_file_hashes_locally_without_using_literal_hash_input(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            env_file = Path(temporary_directory) / "password.env"
            env_file.write_text(
                "\n".join(
                    [
                        "ADMIN_PANEL_NEW_PASSWORD='example-password'",
                        "ADMIN_PANEL_NEW_PASSWORD_CONFIRM='example-password'",
                    ]
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                password_env_file=env_file,
                password_env_key="ADMIN_PANEL_NEW_PASSWORD",
                password_confirmation_env_key="ADMIN_PANEL_NEW_PASSWORD_CONFIRM",
            )

            password_hash = update_admin_password.get_password_hash(args)

        expected_hash = hashlib.sha256("example-password".encode("utf-8")).hexdigest()
        self.assertEqual(password_hash, expected_hash)

    def test_password_file_requires_matching_confirmation(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            env_file = Path(temporary_directory) / "password.env"
            env_file.write_text(
                "\n".join(
                    [
                        "ADMIN_PANEL_NEW_PASSWORD=first-password",
                        "ADMIN_PANEL_NEW_PASSWORD_CONFIRM=second-password",
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ValueError):
                update_admin_password.read_password_from_env_file(
                    env_file,
                    "ADMIN_PANEL_NEW_PASSWORD",
                    "ADMIN_PANEL_NEW_PASSWORD_CONFIRM",
                )


class _FakeWaiter:
    def __init__(self):
        self.wait_calls = []

    def wait(self, **kwargs):
        self.wait_calls.append(kwargs)


class _FakeLambdaClient:
    def __init__(self):
        self.waiter = _FakeWaiter()
        self.update_request = None

    def get_function_configuration(self, **_kwargs):
        return {
            "RevisionId": "revision-1",
            "Environment": {
                "Variables": {
                    "EXISTING_SETTING": "kept",
                }
            },
        }

    def update_function_configuration(self, **kwargs):
        self.update_request = kwargs

    def get_waiter(self, waiter_name):
        if waiter_name != "function_updated_v2":
            raise AssertionError(f"Unexpected waiter: {waiter_name}")
        return self.waiter


class TestUpdateAdminPasswordLambdaUpdate(unittest.TestCase):
    def test_update_preserves_existing_environment_variables(self):
        lambda_client = _FakeLambdaClient()

        update_admin_password.update_password_hash(
            lambda_client,
            function_name="baselinker-sync-admin",
            password_hash="a" * 64,
        )

        self.assertEqual(
            lambda_client.update_request,
            {
                "FunctionName": "baselinker-sync-admin",
                "Environment": {
                    "Variables": {
                        "EXISTING_SETTING": "kept",
                        "ADMIN_PASSWORD_SHA256": "a" * 64,
                    }
                },
                "RevisionId": "revision-1",
            },
        )
        self.assertEqual(lambda_client.waiter.wait_calls, [{"FunctionName": "baselinker-sync-admin"}])


if __name__ == "__main__":
    unittest.main()
