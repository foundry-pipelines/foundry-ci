#!/usr/bin/env python3
"""Checks an operator's operator.yaml: its keys, Runtime, and its types. Each out type is a class
(C# or Java) of the operator's own project: it owns and registers them. Each in type is one of its own
classes (one arriving via Input) or a type in schemaRegistry. A Maven operator's pom.xml generates
the schemas of exactly its out types but those with ignoreSchema: true (jsonschema-maven-plugin classNames;
no plugin if there are none).

    python validators/processors/typecheck.py [OPERATOR_DIR...] [--schema-registry DIR]

OPERATOR_DIR defaults to the current directory; schemaRegistry is cloned at main unless given.
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from pipeline import DECL_FILE, ConfigError, declared_classes, read_decl, run, schema_registry, types_of  # noqa: E402


def check(decl: dict, own: set[str], registered: set[str]) -> list[str]:
    errors = [f"{DECL_FILE} out: {t!r} is not a class of this project" for t in types_of(decl["out"]) if t not in own]
    errors += [f"{DECL_FILE} in: {t!r} is neither a class of this project nor in schemaRegistry"
               for t in types_of(decl["in"]) if t not in own | registered]
    return errors


def pom_schema_types(pom: Path) -> set[str] | None:
    """The simple class names jsonschema-maven-plugin generates schemas for; None without the plugin."""
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    try:
        root = ET.parse(pom).getroot()
    except ET.ParseError as e:
        raise ConfigError([f"pom.xml: not XML ({e})"])
    for plugin in root.iterfind("m:build/m:plugins/m:plugin", ns):
        if plugin.findtext("m:artifactId", "", ns).strip() == "jsonschema-maven-plugin":
            return {n.text.strip().rsplit(".", 1)[-1] for n in plugin.iterfind(".//m:classNames/m:className", ns) if n.text}
    return None


def check_pom(decl: dict, pom: Path, ignored: set[str] = frozenset()) -> list[str]:
    """ignored: types with ignoreSchema: true, which get no schema."""
    if not pom.is_file():
        return []
    outs = set(types_of(decl["out"])) - ignored
    generated = pom_schema_types(pom)
    if generated is None:
        return [f"pom.xml: no jsonschema-maven-plugin, so {', '.join(sorted(outs))} get no schemas"] if outs else []
    return ([f"pom.xml: jsonschema-maven-plugin classNames lack out type {t!r}" for t in sorted(outs - generated)]
            + [f"pom.xml: jsonschema-maven-plugin classNames has {t!r}, not an out type in {DECL_FILE}"
               + (" (it has ignoreSchema: true)" if t in ignored else "")
               for t in sorted(generated - outs)])


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("dirs", type=Path, nargs="*", default=[Path(".")])
    p.add_argument("--schema-registry", type=Path, help="local schemaRegistry checkout (default: clone it)")
    args = p.parse_args()
    with schema_registry(args.schema_registry) as registry:
        registered = registry.types()
        ignored = {t for t in registered if registry.ignores_schema(t)}
    errors = []
    for d in args.dirs:
        try:
            decl = read_decl(d / DECL_FILE)
        except ConfigError as e:
            errors += [f"{d}: {m}" for m in e.errors]
            continue
        try:
            found = check(decl, declared_classes(d), registered) + check_pom(decl, d / "pom.xml", ignored)
        except ConfigError as e:
            found = e.errors
        found = [f"{d}: {m}" for m in found]
        errors += found
        if not found:
            print(f"ok: {d}: {decl['name']}: {', '.join(decl['in'])} -> {', '.join(decl['out'])}")
    if errors:
        raise ConfigError(errors)


if __name__ == "__main__":
    sys.exit(run(main))
