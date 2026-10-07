"""Export the SDKs selected by NuGet restore, using project.assets.json as authority."""

import argparse
import json
import os
from pathlib import Path


PACKAGES = {
    "salts": ("Salts.Native", "SALTS"),
    "salts-utils": ("SaltsUtils.Native", "SALTS_UTILS"),
    "flowmq": ("FlowMQ.Native", "FLOWMQ"),
    "chttp": ("CHttp.Native", "CHTTP"),
    "turboraft": ("TurboRaft.Native", "TURBORAFT"),
    "turbodb": ("TurboDB.Native", "TURBODB"),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assets", type=Path, required=True)
    parser.add_argument("--rid", choices=("linux-x64", "windows-x64"), required=True)
    parser.add_argument("--packages", nargs="+", choices=PACKAGES,
                        default=["salts", "salts-utils", "flowmq", "chttp"])
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--runtime-paths", action="store_true",
                        help="Add SDK tools and shared libraries to the job environment")
    args = parser.parse_args()
    assets = json.loads(args.assets.read_text(encoding="utf-8-sig"))
    environment = []
    metadata = []
    sdk_roots = []
    for key in args.packages:
        package, prefix = PACKAGES[key]
        entries = [(name, value) for name, value in assets["libraries"].items()
                   if name.split("/")[0].lower() == package.lower()]
        if len(entries) != 1:
            raise SystemExit(f"Expected one resolved {package}, found {len(entries)}")
        name, entry = entries[0]
        roots = { (Path(folder) / entry["path"] / "sdk" / args.rid).resolve()
                  for folder in assets["packageFolders"] }
        roots = [root for root in roots if root.is_dir()]
        if len(roots) != 1:
            raise SystemExit(f"Expected one {args.rid} SDK for {name}, found {len(roots)}")
        version = name.split("/", 1)[1]
        sdk_roots.append(roots[0])
        environment.extend((f"{prefix}_ROOT={roots[0].as_posix()}",
                            f"{prefix}_SDK_VERSION={version}"))
        metadata.append(f"{prefix.lower()}_package={name}")

    if args.runtime_paths and args.rid == "linux-x64":
        libraries = [str(root / "lib") for root in sdk_roots]
        libraries.extend(filter(None, [os.environ.get("LD_LIBRARY_PATH", "")]))
        environment.append("LD_LIBRARY_PATH=" + ":".join(libraries))

    # Validate every package before publishing any environment changes.
    if any("\n" in line or "\r" in line for line in environment + metadata):
        raise SystemExit("SDK metadata must contain only single-line values")
    with Path(os.environ["GITHUB_ENV"]).open("a", encoding="utf-8") as output:
        output.write("\n".join(environment) + "\n")
    if args.runtime_paths:
        with Path(os.environ["GITHUB_PATH"]).open("a", encoding="utf-8") as output:
            output.write("\n".join((root / "bin").as_posix() for root in sdk_roots) + "\n")
    for line in metadata:
        print(line)
    if args.metadata:
        with args.metadata.open("a", encoding="utf-8") as output:
            output.write("\n".join(metadata) + "\n")


if __name__ == "__main__":
    main()
