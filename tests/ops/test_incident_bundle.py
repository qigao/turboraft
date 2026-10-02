import hashlib
import json
import pathlib
import subprocess
import sys
import tarfile
import tempfile


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_json(archive: tarfile.TarFile, name: str):
    member = archive.getmember(name)
    stream = archive.extractfile(member)
    assert stream is not None
    return json.loads(stream.read().decode("utf-8"))


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: test_incident_bundle.py <tool>")
    tool = pathlib.Path(sys.argv[1]).resolve()

    with tempfile.TemporaryDirectory(prefix="turboraft-incident-test-") as temp:
        root = pathlib.Path(temp)
        prefix = root / "raft-group-7"
        files = {
            root / "raft-group-7.00000001.wal": b"wal-bytes",
            root / "raft-group-7.manifest": b"manifest-bytes",
            root / "raft-group-7.snapshot.4.2": b"snapshot-bytes",
        }
        for path, data in files.items():
            path.write_bytes(data)
        (root / "raft-group-7.manifest.tmp").write_bytes(b"staging")
        config = root / "node-config.json"
        config.write_text('{"node_id":7}\n', encoding="utf-8")
        output = root / "incident.tar.gz"

        subprocess.run(
            [
                sys.executable,
                str(tool),
                "--output",
                str(output),
                "--wal-prefix",
                str(prefix),
                "--config",
                str(config),
                "--label",
                "test_case=installed-bundle",
            ],
            check=True,
        )

        with tarfile.open(output, "r:gz") as archive:
            names = set(archive.getnames())
            expected = {
                "turboraft-incident/metadata.json",
                "turboraft-incident/storage.json",
                "turboraft-incident/configs.json",
                "turboraft-incident/config/node-config.json",
            }
            assert expected.issubset(names)
            assert not any(name.endswith(".tmp") for name in names)

            metadata = read_json(
                archive, "turboraft-incident/metadata.json")
            assert metadata["labels"]["test_case"] == "installed-bundle"

            storage = read_json(
                archive, "turboraft-incident/storage.json")
            assert len(storage) == 1
            records = {entry["name"]: entry for entry in storage[0]["files"]}
            assert set(records) == {path.name for path in files}
            for path, data in files.items():
                assert records[path.name]["size"] == len(data)
                assert records[path.name]["sha256"] == digest(data)

            configs = read_json(
                archive, "turboraft-incident/configs.json")
            assert configs == [{
                "name": "node-config.json",
                "sha256": digest(config.read_bytes()),
                "size": len(config.read_bytes()),
            }]

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
