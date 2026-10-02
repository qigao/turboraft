#!/usr/bin/env python3
"""Capture and compare TurboRaft packaged static-archive ABI inventories."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import shutil
import subprocess
import sys
from typing import Iterable

SCHEMA_VERSION = 1
SYMBOL_RE = re.compile(r"\b(tr_[A-Za-z0-9_]+)\b")
DECL_RE = re.compile(r"\b(tr_[A-Za-z0-9_]+)\s*\(")


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
        f"{len(defined)} tr_* archive symbols, {len(public_link)} public link symbols"
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

    if args.baseline:
        baseline = load_inventory(args.baseline)
        current_symbols = set(left.get("public_link_symbols", []))
        baseline_symbols = set(baseline.get("public_link_symbols", []))
        removed = sorted(baseline_symbols - current_symbols)
        if removed:
            errors.append(f"baseline public symbols removed: {removed}")

    if errors:
        for error in errors:
            print(f"ABI inventory mismatch: {error}", file=sys.stderr)
        return 1

    print(
        "ABI inventories match: "
        f"{len(left_headers)} headers, "
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
