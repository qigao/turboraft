"""Restore the release archives for one native TurboRaft compiler profile."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile


def api(repo, path):
    return json.loads(subprocess.check_output(["gh", "api", f"repos/qigao/{repo}/{path}"]))


def fetch(release, name):
    asset, = (item for item in release["assets"] if item["name"] == name)
    digest = asset["digest"].removeprefix("sha256:")
    path = downloads / name
    transport.verified_download(asset["browser_download_url"], path, digest, asset["size"])
    return path, digest


def restore(repo, key, profile):
    version = os.environ[key + "_VERSION"]
    release = api(repo, "releases/tags/v" + version)
    if release["draft"] or release["prerelease"] or release["tag_name"] != "v" + version:
        raise ValueError(f"{repo}: expected stable release {version}")
    metadata, _ = fetch(release, f"{repo}-{version}-manifest.json")
    manifest = json.loads(metadata.read_bytes())
    camel = repo in ("salts-base", "cmeta")
    identity = "https://github.com/qigao/" + repo if camel else "qigao/" + repo
    if manifest["repository"] != identity or manifest["version"] != version:
        raise ValueError(f"{repo}: release manifest identity mismatch")
    ref = api(repo, f"git/ref/tags/v{version}")["object"]
    if ref["type"] == "tag":
        ref = api(repo, "git/tags/" + ref["sha"])["object"]
    if ref["sha"] != manifest["commit"]:
        raise ValueError(f"{repo}: release tag differs from the manifest commit")
    run_id = manifest["ci"]["runId"] if camel else manifest["producer_run"]
    run = api(repo, f"actions/runs/{run_id}")
    tree = manifest["sourceTree"] if camel else manifest["tree"]
    if run["conclusion"] != "success" or api(repo, "git/commits/" + run["head_sha"])["tree"]["sha"] != tree:
        raise ValueError(f"{repo}: SDK producer did not qualify this source tree")
    selector, filename = ("platform", "file") if camel else ("profile", "asset")
    sdk, = (entry for entry in manifest["sdks"] if entry[selector] == profile)
    cache = os.environ["VCPKG_CACHE_VERSION"]
    base = os.environ["SALTS_BASE_VERSION"]
    if repo == "salts-base":
        matched = manifest["dependencies"]["release"] == "v" + cache
    elif repo == "salts-cnet":
        matched = (sdk["sdk_manifest"]["base_version"], sdk["sdk_manifest"]["cache_version"]) == (base, cache)
    else:
        deps = manifest["dependencies"]
        matched = (deps["SaltsBase"], deps["vcpkg-cache"]) == (base, cache)
        if repo == "salts-utils":
            matched = matched and (deps["CMeta"], deps["cmetaCommit"]) == (os.environ["CMETA_VERSION"], cmeta_commit)
    if not matched:
        raise ValueError(f"{repo}: SDK composition differs from .env")
    archive, digest = fetch(release, sdk[filename])
    if digest != sdk["sha256"]:
        raise ValueError(f"{repo}: manifest digest differs from release asset")
    root = Path("build/dependencies", f"{repo}-{version}-{profile}").resolve()
    root.mkdir(parents=True)
    with tarfile.open(archive) as package:
        package.extractall(root, filter="data")
    with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as output:
        print(f"{key}_ROOT={root.as_posix()}", file=output)
    print(f"{repo} {version} {profile} {digest}", flush=True)
    return manifest["commit"]


if __name__ == "__main__":
    sdk_profile, cnet_profile = sys.argv[1:]
    downloads = Path("build/sdk-downloads")
    downloads.mkdir(parents=True)
    module = Path(os.environ["VCPKG_CACHE_REPOSITORY_ROOT"], "scripts/setup-compiler-toolchain.py")
    spec = importlib.util.spec_from_file_location("transport", module)
    transport = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(transport)
    restore("salts-base", "SALTS_BASE", sdk_profile)
    cmeta_commit = restore("cmeta", "CMETA", sdk_profile)
    restore("salts-cnet", "SALTS_CNET", cnet_profile)
    restore("salts-utils", "SALTS_UTILS", sdk_profile)
