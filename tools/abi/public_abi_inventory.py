#!/usr/bin/env python3
"""Capture and compare TurboRaft packaged static-archive ABI inventories."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import pathlib
import re
import shutil
import subprocess
import sys
from typing import Iterable

SCHEMA_VERSION = 2
SYMBOL_RE = re.compile(r"\b(tr_[A-Za-z0-9_]+)\b")
DECL_RE = re.compile(r"\b(tr_[A-Za-z0-9_]+)\s*\(")
ENUM_BLOCK_RE = re.compile(
    r"typedef\s+enum(?:\s+[A-Za-z_][A-Za-z0-9_]*)?\s*"
    r"\{(.*?)\}\s*([A-Za-z_][A-Za-z0-9_]*)\s*;",
    re.S,
)
ENUM_ITEM_RE = re.compile(
    r"^([A-Za-z_][A-Za-z0-9_]*)(?:\s*=\s*(.+))?$",
    re.S,
)
C_INTEGER_SUFFIX_RE = re.compile(
    r"(?<![A-Za-z0-9_])"
    r"(0[xX][0-9A-Fa-f]+|0[bB][01]+|0[0-7]+|[0-9]+)"
    r"([uUlL]+)\b"
)


def fail(message: str) -> "None":
    raise SystemExit(message)


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    return re.sub(r"//[^\n]*", " ", text)


def declared_symbols(headers: Iterable[pathlib.Path]) -> list[str]:
    result: set[str] = set()
    for header in headers:
        text = strip_comments(header.read_text(encoding="utf-8"))
        for statement in text.split(";"):
            if "static inline" in statement:
                continue
            for match in DECL_RE.finditer(statement):
                name = match.group(1)
                # Function-like macros are removed with preprocessor lines below.
                prefix = statement[: match.start()]
                if "#" in prefix.split("\n")[-1]:
                    continue
                result.add(name)
    return sorted(result)


def _normalize_c_integer_literals(expression: str) -> str:
    return C_INTEGER_SUFFIX_RE.sub(lambda match: match.group(1), expression)


def _eval_enum_expression(expression: str, names: dict[str, int]) -> int:
    expression = _normalize_c_integer_literals(expression.strip())
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        fail(f"unsupported public enum expression {expression!r}: {exc}")

    def evaluate(node: ast.AST) -> int:
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, int):
            return int(node.value)
        if isinstance(node, ast.Name):
            if node.id not in names:
                fail(
                    f"public enum expression references unknown name "
                    f"{node.id!r}: {expression!r}"
                )
            return names[node.id]
        if isinstance(node, ast.UnaryOp):
            value = evaluate(node.operand)
            if isinstance(node.op, ast.UAdd):
                return value
            if isinstance(node.op, ast.USub):
                return -value
            if isinstance(node.op, ast.Invert):
                return ~value
        if isinstance(node, ast.BinOp):
            left = evaluate(node.left)
            right = evaluate(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.LShift):
                return left << right
            if isinstance(node.op, ast.RShift):
                return left >> right
            if isinstance(node.op, ast.BitOr):
                return left | right
            if isinstance(node.op, ast.BitAnd):
                return left & right
            if isinstance(node.op, ast.BitXor):
                return left ^ right
        fail(
            "unsupported public enum expression node "
            f"{type(node).__name__}: {expression!r}"
        )

    return evaluate(tree)


def public_enum_inventory(
    headers: Iterable[pathlib.Path], root: pathlib.Path
) -> dict[str, dict]:
    enums: dict[str, dict] = {}
    global_names: dict[str, int] = {}

    for header in headers:
        text = strip_comments(header.read_text(encoding="utf-8"))
        for match in ENUM_BLOCK_RE.finditer(text):
            body = match.group(1)
            type_name = match.group(2)
            if type_name in enums:
                fail(f"duplicate public enum typedef: {type_name}")

            values: dict[str, int] = {}
            next_value = 0
            for raw_item in body.split(","):
                item = raw_item.strip()
                if not item:
                    continue
                parsed = ENUM_ITEM_RE.fullmatch(item)
                if parsed is None:
                    fail(
                        f"unsupported public enum item in {type_name}: "
                        f"{item!r}"
                    )
                name = parsed.group(1)
                expression = parsed.group(2)
                if expression is None:
                    value = next_value
                else:
                    value = _eval_enum_expression(
                        expression, {**global_names, **values}
                    )
                values[name] = value
                global_names[name] = value
                next_value = value + 1

            if not values:
                fail(f"public enum has no values: {type_name}")
            enums[type_name] = {
                "header": header.relative_to(root).as_posix(),
                "values": values,
            }

    if not enums:
        fail("no public typedef enum declarations found")
    return dict(sorted(enums.items()))


def run(command: list[str]) -> str:
    proc = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode != 0:
        fail(
            f"command failed ({proc.returncode}): {' '.join(command)}\n"
            f"{proc.stdout}"
        )
    return proc.stdout


def linux_defined_symbols(lib_dir: pathlib.Path) -> list[str]:
    nm = shutil.which("nm")
    if nm is None:
        fail("nm is required for Linux ABI inventory")
    archives = sorted(lib_dir.glob("*.a"))
    if not archives:
        fail(f"no static archives found under {lib_dir}")
    result: set[str] = set()
    for archive in archives:
        output = run([nm, "-g", "--defined-only", str(archive)])
        result.update(SYMBOL_RE.findall(output))
    return sorted(result)


def windows_defined_symbols(lib_dir: pathlib.Path) -> list[str]:
    dumpbin = shutil.which("dumpbin")
    if dumpbin is None:
        fail("dumpbin is required for Windows ABI inventory")
    archives = sorted(lib_dir.glob("*.lib"))
    if not archives:
        fail(f"no static libraries found under {lib_dir}")
    result: set[str] = set()
    for archive in archives:
        output = run([dumpbin, "/nologo", "/linkermember:1", str(archive)])
        result.update(SYMBOL_RE.findall(output))
    return sorted(result)


def capture(args: argparse.Namespace) -> int:
    root = pathlib.Path(args.sdk_root).resolve()
    include_dir = root / "include" / "turboraft"
    lib_dir = root / "lib"
    if not include_dir.is_dir():
        fail(f"missing public header directory: {include_dir}")
    if not lib_dir.is_dir():
        fail(f"missing SDK library directory: {lib_dir}")

    headers = sorted(include_dir.rglob("*.h"))
    if not headers:
        fail(f"no public TurboRaft headers found under {include_dir}")
    suspicious = [
        p.relative_to(root).as_posix()
        for p in headers
        if any(token in p.name.lower() for token in ("internal", "test_only", "fault_inject"))
    ]
    if suspicious:
        fail(f"private/test-looking header installed: {suspicious}")

    declared = declared_symbols(headers)
    if not declared:
        fail("no public tr_* function declarations found")
    enums = public_enum_inventory(headers, root)

    if args.platform == "linux":
        defined = linux_defined_symbols(lib_dir)
    elif args.platform == "windows":
        defined = windows_defined_symbols(lib_dir)
    else:
        fail(f"unsupported platform: {args.platform}")

    declared_set = set(declared)
    defined_set = set(defined)
    public_link = sorted(declared_set & defined_set)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "platform": args.platform,
        "headers": [
            {
                "path": p.relative_to(root).as_posix(),
                "sha256": sha256(p),
            }
            for p in headers
        ],
        "declared_symbols": declared,
        "public_enums": enums,
        "defined_tr_symbols": defined,
        "public_link_symbols": public_link,
        "declared_without_link_symbol": sorted(declared_set - defined_set),
        "link_symbols_without_public_declaration": sorted(
            defined_set - declared_set
        ),
    }
    if not public_link:
        fail("no declared TurboRaft symbol is defined by the installed SDK")

    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        f"captured {args.platform}: "
        f"{len(headers)} headers, {len(declared)} declarations, "
        f"{len(enums)} public enums, {len(defined)} tr_* archive symbols, "
        f"{len(public_link)} public link symbols"
    )
    return 0


def load_inventory(path: str) -> dict:
    data = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    if data.get("schema_version") != SCHEMA_VERSION:
        fail(f"unsupported inventory schema in {path}")
    return data


def compare(args: argparse.Namespace) -> int:
    left = load_inventory(args.left)
    right = load_inventory(args.right)
    errors: list[str] = []

    left_headers = {
        item["path"]: item["sha256"] for item in left.get("headers", [])
    }
    right_headers = {
        item["path"]: item["sha256"] for item in right.get("headers", [])
    }
    if left_headers != right_headers:
        only_left = sorted(set(left_headers) - set(right_headers))
        only_right = sorted(set(right_headers) - set(left_headers))
        changed = sorted(
            path
            for path in set(left_headers) & set(right_headers)
            if left_headers[path] != right_headers[path]
        )
        errors.append(
            "public header inventory differs: "
            f"left_only={only_left} right_only={only_right} changed={changed}"
        )

    for field in ("declared_symbols", "public_link_symbols"):
        a = set(left.get(field, []))
        b = set(right.get(field, []))
        if a != b:
            errors.append(
                f"{field} differs: "
                f"left_only={sorted(a - b)} right_only={sorted(b - a)}"
            )

    left_enums = left.get("public_enums", {})
    right_enums = right.get("public_enums", {})
    if left_enums != right_enums:
        left_types = set(left_enums)
        right_types = set(right_enums)
        details: list[str] = []
        if left_types != right_types:
            details.append(
                f"types left_only={sorted(left_types - right_types)} "
                f"right_only={sorted(right_types - left_types)}"
            )
        for type_name in sorted(left_types & right_types):
            left_values = left_enums[type_name].get("values", {})
            right_values = right_enums[type_name].get("values", {})
            if left_values != right_values:
                left_names = set(left_values)
                right_names = set(right_values)
                changed = sorted(
                    name
                    for name in left_names & right_names
                    if left_values[name] != right_values[name]
                )
                details.append(
                    f"{type_name}: left_only={sorted(left_names - right_names)} "
                    f"right_only={sorted(right_names - left_names)} "
                    f"changed={changed}"
                )
        errors.append("public enum inventory differs: " + "; ".join(details))

    if args.baseline:
        baseline = load_inventory(args.baseline)
        current_symbols = set(left.get("public_link_symbols", []))
        baseline_symbols = set(baseline.get("public_link_symbols", []))
        removed = sorted(baseline_symbols - current_symbols)
        if removed:
            errors.append(f"baseline public symbols removed: {removed}")
        baseline_enums = baseline.get("public_enums", {})
        current_enums = left.get("public_enums", {})
        for type_name, baseline_entry in baseline_enums.items():
            if type_name not in current_enums:
                errors.append(f"baseline public enum removed: {type_name}")
                continue
            baseline_values = baseline_entry.get("values", {})
            current_values = current_enums[type_name].get("values", {})
            for name, value in baseline_values.items():
                if name not in current_values:
                    errors.append(
                        f"baseline enum value removed: {type_name}.{name}"
                    )
                elif current_values[name] != value:
                    errors.append(
                        f"baseline enum value changed: {type_name}.{name} "
                        f"{value} -> {current_values[name]}"
                    )

    if errors:
        for error in errors:
            print(f"ABI inventory mismatch: {error}", file=sys.stderr)
        return 1

    print(
        "ABI inventories match: "
        f"{len(left_headers)} headers, "
        f"{len(left.get('public_enums', {}))} public enums, "
        f"{len(left.get('public_link_symbols', []))} public link symbols"
    )
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command", required=True)

    cap = sub.add_parser("capture")
    cap.add_argument("--sdk-root", required=True)
    cap.add_argument("--platform", choices=("linux", "windows"), required=True)
    cap.add_argument("--output", required=True)
    cap.set_defaults(func=capture)

    cmp = sub.add_parser("compare")
    cmp.add_argument("--left", required=True)
    cmp.add_argument("--right", required=True)
    cmp.add_argument("--baseline")
    cmp.set_defaults(func=compare)
    return root


def main() -> int:
    args = parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
