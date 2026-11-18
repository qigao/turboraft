"""Index the installed SDKs produced by the native release matrix."""
import hashlib
import json
import os
from pathlib import Path
import tarfile


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


version = os.environ["RELEASE_VERSION"]
dist = Path("dist")
sdks = []
checksums = []
for profile in ("linux-gcc-x64", "linux-clang-arm64", "macos-clang-arm64", "windows-msvc-x64"):
    path = dist / f"turboraft-{version}-{profile}.tar.gz"
    core = "turboraft_core.lib" if profile == "windows-msvc-x64" else "libturboraft_core.a"
    required = {
        "include/turboraft/raft_core.h", f"lib/{core}",
        "lib/cmake/TurboRaft/TurboRaftConfig.cmake",
        "lib/cmake/TurboRaft/TurboRaftConfigVersion.cmake",
        "lib/cmake/TurboRaft/TurboRaftTargets-release.cmake",
        "share/turboraft/schema/turboraft_node.schema",
        "share/turboraft/abi/PUBLIC_CALLBACK_CONTRACTS.json",
        "LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md",
    }
    with tarfile.open(path, "r:gz") as archive:
        files = {entry.name.removeprefix("./") for entry in archive if entry.isfile() and entry.size}
    if missing := required - files:
        raise ValueError(f"{path}: missing SDK files {sorted(missing)}")
    digest = sha256(path)
    sdks.append({"profile": profile, "asset": path.name, "size": path.stat().st_size, "sha256": digest})
    checksums.append(f"{digest}  {path.name}")

manifest = {
    "repository": os.environ["GITHUB_REPOSITORY"],
    "version": version,
    "commit": os.environ["GITHUB_SHA"],
    "producer_run": int(os.environ["GITHUB_RUN_ID"]),
    "dependencies": {key: os.environ[key + "_VERSION"] for key in (
        "VCPKG_CACHE", "SALTS_BASE", "CMETA", "SALTS_CNET", "SALTS_UTILS", "CHTTP"
    )},
    "test_dependencies": {"TURBODB": os.environ["TURBODB_VERSION"]},
    "sdks": sdks,
}
index = dist / f"turboraft-{version}-manifest.json"
index.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
checksums.append(f"{sha256(index)}  {index.name}")
(dist / "SHA256SUMS").write_text("\n".join(checksums) + "\n", encoding="utf-8")
