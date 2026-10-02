#!/usr/bin/env python3
"""Create a read-only TurboRaft incident bundle from an installed SDK."""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import hashlib
import json
import os
import pathlib
import platform
import shutil
import tarfile
import tempfile
import urllib.error
import urllib.request


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_record(path: pathlib.Path) -> dict:
    stat = path.stat()
    return {
        "name": path.name,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sha256": sha256_file(path),
    }


def storage_files(prefix: str) -> list[pathlib.Path]:
    candidates = set()
    for pattern in (
        prefix + ".*.wal",
        prefix + ".manifest",
        prefix + ".snapshot.*",
    ):
        for item in glob.glob(pattern):
            path = pathlib.Path(item)
            if path.is_file() and not path.name.endswith(".tmp"):
                candidates.add(path)
    return sorted(candidates, key=lambda item: item.name)


def fetch_status(url: str, timeout: float) -> tuple[dict | None, str | None]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            payload = response.read()
            if response.status != 200:
                return None, f"HTTP {response.status}"
        parsed = json.loads(payload.decode("utf-8"))
        if not isinstance(parsed, dict):
            return None, "status response is not a JSON object"
        return parsed, None
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        return None, str(exc)


def write_json(path: pathlib.Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create a read-only TurboRaft incident bundle")
    parser.add_argument("--output", required=True,
                        help="output .tar.gz path")
    parser.add_argument("--status-url",
                        help="optional ControlPlane /raft/status URL")
    parser.add_argument("--wal-prefix", action="append", default=[],
                        help="WAL path prefix; repeat for multiple groups")
    parser.add_argument("--config", action="append", default=[],
                        help="explicit sanitized config file to copy")
    parser.add_argument("--label", action="append", default=[],
                        help="evidence label in KEY=VALUE form")
    parser.add_argument("--timeout", type=float, default=5.0,
                        help="status request timeout in seconds")
    args = parser.parse_args()

    labels: dict[str, str] = {}
    for raw in args.label:
        if "=" not in raw:
            parser.error("--label requires KEY=VALUE")
        key, value = raw.split("=", 1)
        if not key:
            parser.error("--label key must not be empty")
        labels[key] = value

    output = pathlib.Path(args.output).resolve()
    if output.suffixes[-2:] != [".tar", ".gz"]:
        parser.error("--output must end in .tar.gz")
    output.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="turboraft-incident-") as temp:
        root = pathlib.Path(temp) / "turboraft-incident"
        root.mkdir()
        configs = root / "config"
        configs.mkdir()

        metadata = {
            "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "hostname": platform.node(),
            "platform": platform.platform(),
            "python": platform.python_version(),
            "labels": labels,
            "status_url": args.status_url,
            "wal_prefix_count": len(args.wal_prefix),
        }
        write_json(root / "metadata.json", metadata)

        if args.status_url:
            status, error = fetch_status(args.status_url, args.timeout)
            if status is not None:
                write_json(root / "raft-status.json", status)
            else:
                (root / "raft-status-error.txt").write_text(
                    error + "\n", encoding="utf-8")

        storage = []
        for prefix in args.wal_prefix:
            files = storage_files(prefix)
            storage.append({
                "prefix": prefix,
                "files": [file_record(path) for path in files],
            })
        write_json(root / "storage.json", storage)

        copied_configs = []
        for raw in args.config:
            source = pathlib.Path(raw).resolve()
            if not source.is_file():
                raise SystemExit(f"config file does not exist: {source}")
            target = configs / source.name
            if target.exists():
                raise SystemExit(
                    f"duplicate config basename in bundle: {source.name}")
            shutil.copyfile(source, target)
            copied_configs.append({
                "name": source.name,
                "sha256": sha256_file(source),
                "size": source.stat().st_size,
            })
        write_json(root / "configs.json", copied_configs)

        with tarfile.open(output, "w:gz") as archive:
            archive.add(root, arcname="turboraft-incident")

    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
