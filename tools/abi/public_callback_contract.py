#!/usr/bin/env python3
"""Validate the installed TurboRaft public callback surface against its contract."""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

SCHEMA_VERSION = 1
REQUIRED_FIELDS = ("owner", "borrow", "retain", "reentry", "failure", "release")
TYPEDEF_CALLBACK_RE = re.compile(
    r"typedef\s+[^;{}]*?\(\s*\*([A-Za-z_][A-Za-z0-9_]*)\s*\)"
    r"\s*\([^;{}]*?\)\s*;",
    re.S,
)
STRUCT_RE = re.compile(
    r"typedef\s+struct(?:\s+[A-Za-z_][A-Za-z0-9_]*)?\s*"
    r"\{(.*?)\}\s*([A-Za-z_][A-Za-z0-9_]*)\s*;",
    re.S,
)
FIELD_CALLBACK_RE = re.compile(
    r"\(\s*\*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*\(",
)


def fail(message: str) -> "None":
    raise SystemExit(message)


def discover_callbacks(include_dir: pathlib.Path) -> set[str]:
    result: set[str] = set()
    headers = sorted(include_dir.rglob("*.h"))
    if not headers:
        fail(f"no installed TurboRaft headers under {include_dir}")

    for header in headers:
        text = header.read_text(encoding="utf-8")
        for match in TYPEDEF_CALLBACK_RE.finditer(text):
            result.add(match.group(1))
        for match in STRUCT_RE.finditer(text):
            body = match.group(1)
            type_name = match.group(2)
            for field in FIELD_CALLBACK_RE.finditer(body):
                result.add(f"{type_name}.{field.group(1)}")
    return result


def validate(args: argparse.Namespace) -> int:
    root = pathlib.Path(args.sdk_root).resolve()
    include_dir = root / "include" / "turboraft"
    contract_path = pathlib.Path(args.contract).resolve()
    contract = json.loads(contract_path.read_text(encoding="utf-8"))

    if contract.get("schema_version") != SCHEMA_VERSION:
        fail(f"unsupported callback contract schema: {contract.get('schema_version')!r}")

    families = contract.get("families")
    callbacks = contract.get("callbacks")
    if not isinstance(families, dict) or not isinstance(callbacks, dict):
        fail("callback contract must contain object-valued families and callbacks")

    errors: list[str] = []
    for name, family in sorted(families.items()):
        if not isinstance(family, dict):
            errors.append(f"family {name} is not an object")
            continue
        for field in REQUIRED_FIELDS:
            value = family.get(field)
            if not isinstance(value, str) or not value.strip():
                errors.append(f"family {name} missing non-empty {field}")

    for surface, family_name in sorted(callbacks.items()):
        if family_name not in families:
            errors.append(f"callback {surface} references unknown family {family_name}")

    discovered = discover_callbacks(include_dir)
    declared = set(callbacks)
    missing = sorted(discovered - declared)
    stale = sorted(declared - discovered)
    if missing:
        errors.append(f"public callbacks without contract: {missing}")
    if stale:
        errors.append(f"contract callbacks absent from installed headers: {stale}")

    if errors:
        for error in errors:
            print(f"callback contract mismatch: {error}", file=sys.stderr)
        return 1

    print(
        f"public callback contract verified: {len(discovered)} callbacks, "
        f"{len(families)} semantic families"
    )
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    root.add_argument("--sdk-root", required=True)
    root.add_argument("--contract", required=True)
    return root


def main() -> int:
    return validate(parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
