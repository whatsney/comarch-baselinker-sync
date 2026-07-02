#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
import hashlib
from pathlib import Path
import subprocess
import sys
import warnings
from typing import Any, Sequence

import boto3
from botocore.exceptions import BotoCoreError, ClientError


DEFAULT_REGION = "eu-north-1"
DEFAULT_FUNCTION_NAME = "baselinker-sync-admin"
PASSWORD_ENV_KEY = "ADMIN_PASSWORD_SHA256"
PASSWORD_INPUT_ENV_KEY = "ADMIN_PANEL_NEW_PASSWORD"
PASSWORD_CONFIRMATION_ENV_KEY = "ADMIN_PANEL_NEW_PASSWORD_CONFIRM"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Update the admin Lambda password hash without printing the password "
            "or hash value."
        )
    )
    parser.add_argument("--profile", help="AWS CLI profile to use. Omit to use the default AWS credential chain.")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--function-name", default=DEFAULT_FUNCTION_NAME)
    parser.add_argument("--expected-account-id", required=True)
    parser.add_argument(
        "--password-env-file",
        type=Path,
        help=(
            f"Read the new admin password from {PASSWORD_INPUT_ENV_KEY} in a local "
            "env file instead of prompting."
        ),
    )
    parser.add_argument("--password-env-key", default=PASSWORD_INPUT_ENV_KEY)
    parser.add_argument("--password-confirmation-env-key", default=PASSWORD_CONFIRMATION_ENV_KEY)
    parser.add_argument(
        "--prompt-mode",
        choices=("terminal", "macos-dialog"),
        default="terminal",
        help="Read the password from the terminal or a macOS hidden-answer dialog.",
    )
    return parser.parse_args(argv)


def get_password_hash(args: argparse.Namespace) -> str:
    if args.password_env_file:
        password = read_password_from_env_file(
            args.password_env_file,
            args.password_env_key,
            args.password_confirmation_env_key,
        )
    else:
        password = prompt_for_password(args.prompt_mode)

    if not password:
        raise ValueError("Password cannot be empty.")

    password_bytes = password.encode("utf-8")
    return hashlib.sha256(password_bytes).hexdigest()


def prompt_for_password(prompt_mode: str) -> str:
    password = prompt_hidden("New admin password:", prompt_mode)
    confirmation = prompt_hidden("Confirm new admin password:", prompt_mode)

    if not password:
        raise ValueError("Password cannot be empty.")
    if password != confirmation:
        raise ValueError("Password confirmation does not match.")

    return password


def read_password_from_env_file(
    env_file_path: Path,
    password_key: str,
    confirmation_key: str,
) -> str:
    env_values = read_local_env_file(env_file_path)
    password = env_values.get(password_key, "")
    confirmation = env_values.get(confirmation_key, "")

    if not password:
        raise ValueError(f"{password_key} is empty or missing in {env_file_path}.")
    if confirmation and password != confirmation:
        raise ValueError(f"{confirmation_key} does not match {password_key}.")

    return password


def read_local_env_file(env_file_path: Path) -> dict[str, str]:
    if not env_file_path.exists():
        raise ValueError(f"{env_file_path} does not exist.")

    values: dict[str, str] = {}
    for line in env_file_path.read_text(encoding="utf-8").splitlines():
        stripped_line = line.strip()
        if not stripped_line or stripped_line.startswith("#") or "=" not in line:
            continue

        key, raw_value = line.split("=", 1)
        values[key.strip()] = parse_env_value(raw_value)

    return values


def parse_env_value(raw_value: str) -> str:
    value = raw_value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def prompt_hidden(prompt: str, prompt_mode: str) -> str:
    if prompt_mode == "macos-dialog":
        return prompt_hidden_with_macos_dialog(prompt)

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            return getpass.getpass(f"{prompt} ")
    except getpass.GetPassWarning:
        return prompt_hidden_with_stty(f"{prompt} ")


def prompt_hidden_with_stty(prompt: str) -> str:
    print(prompt, end="", file=sys.stderr, flush=True)
    try:
        subprocess.run(["stty", "-echo"], check=True)
        value = sys.stdin.readline()
    except (OSError, subprocess.CalledProcessError) as error:
        raise RuntimeError("Could not disable terminal echo for password input.") from error
    finally:
        subprocess.run(["stty", "echo"], check=False)
        print(file=sys.stderr)

    return value.rstrip("\n")


def prompt_hidden_with_macos_dialog(prompt: str) -> str:
    apple_script = (
        'display dialog "{prompt}" default answer "" with hidden answer '
        'buttons {{"Cancel", "OK"}} default button "OK" cancel button "Cancel"\n'
        "text returned of result"
    ).format(prompt=prompt.replace("\\", "\\\\").replace('"', '\\"'))

    try:
        result = subprocess.run(
            ["osascript", "-e", apple_script],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise RuntimeError("Could not read password from the macOS dialog.") from error

    return result.stdout.rstrip("\n")


def verify_account(session: boto3.Session, expected_account_id: str) -> str:
    sts_client = session.client("sts")
    identity = sts_client.get_caller_identity()
    account_id = identity["Account"]
    if account_id != expected_account_id:
        raise RuntimeError(
            f"Refusing to update Lambda in account {account_id}; "
            f"expected {expected_account_id}."
        )
    return account_id


def get_environment_variables(lambda_client: Any, function_name: str) -> tuple[dict[str, str], str | None]:
    configuration = lambda_client.get_function_configuration(FunctionName=function_name)
    environment = configuration.get("Environment", {})
    variables = dict(environment.get("Variables", {}))
    revision_id = configuration.get("RevisionId")
    return variables, revision_id


def update_password_hash(
    lambda_client: Any,
    function_name: str,
    password_hash: str,
) -> None:
    variables, revision_id = get_environment_variables(lambda_client, function_name)
    variables[PASSWORD_ENV_KEY] = password_hash

    update_request: dict[str, Any] = {
        "FunctionName": function_name,
        "Environment": {"Variables": variables},
    }
    if revision_id:
        update_request["RevisionId"] = revision_id

    lambda_client.update_function_configuration(**update_request)
    waiter = lambda_client.get_waiter("function_updated_v2")
    waiter.wait(FunctionName=function_name)


def verify_password_hash_presence(lambda_client: Any, function_name: str) -> bool:
    variables, _revision_id = get_environment_variables(lambda_client, function_name)
    return PASSWORD_ENV_KEY in variables


def main() -> int:
    args = parse_args()

    try:
        password_hash = get_password_hash(args)
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        account_id = verify_account(session, args.expected_account_id)
        lambda_client = session.client("lambda")

        print(
            f"Updating {PASSWORD_ENV_KEY} for {args.function_name} "
            f"in account {account_id}, region {args.region}."
        )
        update_password_hash(lambda_client, args.function_name, password_hash)

        if not verify_password_hash_presence(lambda_client, args.function_name):
            raise RuntimeError(f"{PASSWORD_ENV_KEY} was not present after the update.")

        print(f"Lambda update finished. {PASSWORD_ENV_KEY} is present.")
        return 0
    except (BotoCoreError, ClientError, RuntimeError, ValueError) as error:
        print(f"Update failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
