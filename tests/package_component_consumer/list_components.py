#!/usr/bin/env python3
import json
import pathlib
import sys

if len(sys.argv) != 2:
    raise SystemExit("usage: list_components.py <profile>")
profile = sys.argv[1]
path = pathlib.Path(__file__).with_name("components.json")
data = json.loads(path.read_text(encoding="utf-8"))
for component in data["components"]:
    if profile in component["profiles"]:
        print(component["name"])
