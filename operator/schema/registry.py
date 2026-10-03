#!/usr/bin/env python3
"""Checks or publishes the JSON Schemas in DIR (<Type>.json, from operator/schema/SchemaGen or a Maven operator's
jsonschema-maven-plugin) against a schema-registry checkout (<Type>/metadata.yaml, <Type>/v<N>.json).

    python operator/schema/registry.py check DIR --owner OPERATOR --registry SCHEMA_REGISTRY_DIR
    python operator/schema/registry.py publish DIR --owner OPERATOR --registry SCHEMA_REGISTRY_DIR

Both fail if OPERATOR (its project path, plus /<OPERATOR_PATH> if set) isn't a type's owner, or a schema
breaks its latest version. publish then writes each changed schema as its type's next version; the caller
commits and pushes the checkout. Types with ignoreSchema: true are skipped past the owner check, and an
operator whose types all ignore their schema may have no DIR at all.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from pipeline import ConfigError, SchemaRegistry, run  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("action", choices=["check", "publish"])
    p.add_argument("dir", type=Path)
    p.add_argument("--owner", required=True)
    p.add_argument("--registry", type=Path, required=True, help="schema-registry checkout")
    args = p.parse_args()
    registry = SchemaRegistry(args.registry)
    schemas = {f.stem: json.loads(f.read_text()) for f in sorted(args.dir.glob("*.json"))}
    if not schemas:
        print(f"ok: {args.dir}: no schemas")
        return
    errors = [m for name, schema in schemas.items() for m in registry.check(name, schema, args.owner)]
    if errors:
        raise ConfigError(errors)
    for name, schema in schemas.items():
        if registry.ignores_schema(name):
            print(f"ok: {name}: ignoreSchema, skipped")
        elif args.action == "check":
            print(f"ok: {name}: compatible")
        elif (version := registry.publish(name, schema, args.owner)) is None:
            print(f"ok: {name}: unchanged")
        else:
            print(f"ok: {name}: v{version}")


if __name__ == "__main__":
    sys.exit(run(main))
