from __future__ import annotations

from typing import Optional

from constructs import Node


TRUTHY_CONTEXT_VALUES = {"1", "true", "yes", "y", "on"}


def get_context_text(
    node: Node,
    name: str,
    default: Optional[str] = None,
    required: bool = False,
) -> str:
    value = node.try_get_context(name)
    if value is None or str(value).strip() == "":
        value = default

    if required and (value is None or str(value).strip() == ""):
        raise ValueError(f"Missing required CDK context: {name}")

    if value is None:
        return ""
    return str(value)


def get_context_bool(node: Node, name: str, default: bool) -> bool:
    raw_value = node.try_get_context(name)
    if raw_value is None:
        return default
    normalized_value = str(raw_value).strip().lower()
    return normalized_value in TRUTHY_CONTEXT_VALUES


def get_context_int(node: Node, name: str, default: int) -> int:
    raw_value = node.try_get_context(name)
    if raw_value is None or str(raw_value).strip() == "":
        return int(default)
    return int(str(raw_value).strip())
