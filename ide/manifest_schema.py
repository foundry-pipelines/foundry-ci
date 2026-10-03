#!/usr/bin/env python3
"""Writes the JSON Schema IDEs use to autocomplete PipelineManifest.yaml (Config: from configRegistry's
profiles, AllowedTypes: from Foundry.Common.Models' classes).

    python ide/manifest_schema.py --registry CONFIG_REGISTRY_DIR [--models FOUNDRY_COMMON_DIR] [-o FILE]
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))
from pipeline import CONNECTION_KEYS, model_types, run  # noqa: E402


def schema(registry: Path, types: set[str]) -> dict:
    strings = {"type": "array", "items": {"type": "string"}, "minItems": 1}
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Foundry PipelineManifest",
        "type": "object",
        "required": ["Name", "ConfigRegistry", "Kafkas", "Processors", "Flow"],
        "additionalProperties": False,
        "properties": {
            "Name": {"type": "string"},
            "ConfigRegistry": {"type": "object", "required": ["Repo", "Ref"], "additionalProperties": False,
                               "properties": {"Repo": {"type": "string"}, "Ref": {"type": "string"}}},
            "Kafkas": {"type": "object", "required": ["Input", "Output"], "additionalProperties": {
                "type": "object", "required": ["Config", "AllowedTypes", "Topic"], "additionalProperties": False,
                "properties": {
                    "Config": {"enum": sorted(f"kafka/{p.stem}" for p in (registry / "kafka").glob("*.json"))},
                    "AllowedTypes": {**strings, "items": {"enum": sorted(types)}},
                    "Topic": {"type": "string"},
                    "ConnectionSettings": {"type": "object", "additionalProperties": False,
                                           "properties": {k: {"type": "string"} for k in sorted(CONNECTION_KEYS)}},
                }}},
            "Processors": {"type": "object", "additionalProperties": {
                "type": "object", "required": ["Repo"], "additionalProperties": False,
                "properties": {"Repo": {"type": "string"}, "Ref": {"type": "string", "description": "Defaults to main"},
                               "Path": {"type": "string", "description": "The processor's folder in the repo"}}}},
            "Flow": {"type": "array", "items": {"type": "string", "pattern": r"^\s*\S+\s*->\s*\S+\s*$"}},
        },
    }


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--registry", type=Path, required=True, help="configRegistry checkout")
    p.add_argument("--models", type=Path, help="local foundry-common checkout (default: clone it)")
    p.add_argument("-o", "--output", type=Path, help="write here instead of stdout")
    args = p.parse_args()
    text = json.dumps(schema(args.registry, model_types(args.models)), indent=2) + "\n"
    if args.output:
        args.output.write_text(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    sys.exit(run(main))
