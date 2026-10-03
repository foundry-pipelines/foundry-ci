#!/usr/bin/env python3
"""Writes the JSON Schemas IDEs use to autocomplete PipelineManifest.yaml (Config: from configRegistry's
profiles; AllowedTypes: schemaRegistry's types) and operator.yaml (in/out: schemaRegistry's types). A type
is a schemaRegistry folder with a metadata.yaml, ignoreSchema ones included.

    python ide/manifest_schema.py --registry CONFIG_REGISTRY_DIR [--schema-registry DIR] [-o FILE] [-O FILE]

schemaRegistry: a local clone (--schema-registry), else read through the GitLab API.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))
from pipeline import CONNECTION_KEYS, DECL_KEYS, RUNTIMES, registry_type_names, run  # noqa: E402


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


def operator_schema(types: set[str]) -> dict:
    names = {"type": "array", "items": {"enum": sorted(types)}, "minItems": 1, "uniqueItems": True}
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Foundry operator.yaml",
        "type": "object",
        "required": sorted(DECL_KEYS),
        "additionalProperties": False,
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "Runtime": {"enum": list(RUNTIMES)},
            "in": {**names, "description": "Types it reads"},
            "out": {**names, "description": "Types it writes, its own"},
        },
    }


def write(text: str, path: Path | None):
    if path:
        path.write_text(text)
    else:
        sys.stdout.write(text)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--registry", type=Path, required=True, help="configRegistry checkout")
    p.add_argument("--schema-registry", type=Path, help="local schemaRegistry clone (default: the GitLab API)")
    p.add_argument("-o", "--output", type=Path, help="the manifest schema: write here instead of stdout")
    p.add_argument("-O", "--operator-output", type=Path, help="also write the operator.yaml schema here")
    args = p.parse_args()
    types = registry_type_names(args.schema_registry)
    write(json.dumps(schema(args.registry, types), indent=2) + "\n", args.output)
    if args.operator_output:
        write(json.dumps(operator_schema(types), indent=2) + "\n", args.operator_output)


if __name__ == "__main__":
    sys.exit(run(main))
