#!/usr/bin/env python3
"""Type-checks a pipeline: its Kafkas, Processors and Flow, against each processor's operator.yaml.

    python validators/pipelines/typecheck.py PipelineManifest.yaml... [--schema-registry DIR] [--config-registry DIR]

- Kafkas have a Config, Topic and AllowedTypes; Input and Output exist. Each Config is a profile in
  configRegistry (at the manifest's Ref, or --config-registry). AllowedTypes and processors' in/out
  types have schemas in schemaRegistry (cloned at main, or --schema-registry).
- ConfigRegistry has a Repo and a Ref; processors have a Repo and an operator.yaml at Ref.
- Flow connects a Kafka to a processor or a processor to a Kafka, between things the manifest defines,
  and everything it defines is connected. Nothing writes Input.
- A processor reads every type its input Kafkas allow (in), and each type it reads comes from one of
  them. Its output Kafka allows every type it writes (out), and each type a Kafka allows is written
  there by one of its writers. Every type a processor reads is written by a processor of the pipeline
  or comes via Input.
- dotnet processors read one Kafka; every processor writes one.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from pipeline import (DEFAULT_REF, KAFKA_KEYS, MANDATORY_KAFKAS, MANIFEST_KEYS, PROCESSOR_KEYS,  # noqa: E402
                      ConfigError, checkout, flow, load_manifest, load_profile, processor_decls, run,
                      schema_registry, section, types_of)


def check(manifest: dict, decls: dict[str, dict], registered: set[str], profiles: Path | None = None) -> list[str]:
    """profiles: a configRegistry checkout to find each Kafka's Config in; None skips that check."""
    errors = [f"{k}: unknown top-level key" for k in manifest if k not in MANIFEST_KEYS]
    if not manifest.get("Name"):
        errors.append("Name: required")
    registry = section(manifest, "ConfigRegistry")
    errors += [f"ConfigRegistry.{k}: required" for k in ("Repo", "Ref") if not registry.get(k)]
    kafkas, processors = section(manifest, "Kafkas"), section(manifest, "Processors")
    errors += [f"Kafkas.{k}: required" for k in MANDATORY_KAFKAS if k not in kafkas]
    errors += [f"{n}: both a Kafka and a processor" for n in kafkas.keys() & processors.keys()]

    allowed: dict[str, set[str]] = {}
    for name, spec in kafkas.items():
        spec = spec if isinstance(spec, dict) else {}
        errors += [f"Kafkas.{name}: unknown key {k!r}" for k in spec if k not in KAFKA_KEYS]
        errors += [f"Kafkas.{name}: missing {k}" for k in ("Config", "Topic", "AllowedTypes") if not spec.get(k)]
        types = types_of(spec.get("AllowedTypes"))
        if spec.get("AllowedTypes") and not types:
            errors.append(f"Kafkas.{name}.AllowedTypes: must be a list of type names, e.g. [Packets]")
        errors += [f"Kafkas.{name}.AllowedTypes: {t!r} is not in schemaRegistry"
                   for t in types if t not in registered]
        if profiles is not None and spec.get("Config"):
            try:
                load_profile(profiles, spec["Config"])
            except ConfigError as e:
                errors += [f"Kafkas.{name}.Config: {m}" for m in e.errors]
        allowed[name] = set(types)
    for name, spec in processors.items():
        spec = spec if isinstance(spec, dict) else {}
        errors += [f"Processors.{name}: unknown key {k!r}" for k in spec if k not in PROCESSOR_KEYS]
        if not spec.get("Repo"):
            errors.append(f"Processors.{name}: missing Repo")

    edges, bad = flow(manifest)
    errors += bad
    reads: dict[str, list[str]] = {p: [] for p in processors}
    writes: dict[str, list[str]] = {p: [] for p in processors}
    used = set()
    for a, b in edges:
        where = f"Flow: {a} -> {b}"
        if missing := [n for n in (a, b) if n not in kafkas and n not in processors]:
            errors.append(f"{where}: no Kafka or processor {', '.join(missing)}")
        elif a in kafkas and b in processors:
            reads[b].append(a)
        elif a in processors and b in kafkas:
            writes[a].append(b)
            if b == "Input":
                errors.append(f"{where}: nothing in the pipeline writes Input")
        else:
            errors.append(f"{where}: must connect a Kafka and a processor")
        used |= {a, b}
    errors += [f"{n}: not in Flow" for n in [*kafkas, *processors] if n not in used]
    errors += [f"Kafkas.{k}: nothing writes it" for k in kafkas
               if k != "Input" and k in used and not any(k in w for w in writes.values())]

    for name in processors:
        decl = decls.get(name)
        if decl is None:
            continue  # no operator.yaml: reported by processor_decls
        ins, outs = set(types_of(decl.get("in"))), set(types_of(decl.get("out")))
        errors += [f"{name}: {t!r} is not in schemaRegistry" for t in sorted(ins | outs) if t not in registered]
        if not reads[name]:
            errors.append(f"{name}: reads no Kafka")
        if decl.get("Runtime", "dotnet") == "dotnet" and len(reads[name]) > 1:
            errors.append(f"{name}: a dotnet processor reads one Kafka, not {', '.join(reads[name])}")
        if len(writes[name]) != 1:
            errors.append(f"{name}: writes {len(writes[name])} Kafkas ({', '.join(writes[name]) or 'none'}), needs one")
        for k in reads[name]:
            if extra := allowed.get(k, set()) - ins:
                errors.append(f"Flow: {k} -> {name}: {k} allows {', '.join(sorted(extra))} but {name} reads {', '.join(sorted(ins))}")
        if reads[name] and (unread := ins - set().union(*(allowed.get(k, set()) for k in reads[name]))):
            errors.append(f"{name}: reads {', '.join(sorted(unread))} but none of {', '.join(reads[name])} allows it")
        for k in writes[name]:
            if extra := outs - allowed.get(k, set()):
                errors.append(f"Flow: {name} -> {k}: {name} writes {', '.join(sorted(extra))} but {k} allows {', '.join(sorted(allowed.get(k, ()))) or 'nothing'}")
    produced = {t for d in decls.values() for t in types_of(d.get("out"))}
    via_input = set(types_of(section(kafkas, "Input").get("AllowedTypes")))
    for name, decl in decls.items():
        errors += [f"{name}: reads {t} but no processor writes it and Input doesn't allow it"
                   for t in types_of(decl.get("in")) if t not in produced | via_input]
    for k in kafkas:
        writers = [p for p, w in writes.items() if k in w and p in decls]
        if k != "Input" and writers and (unwritten := allowed[k] - {t for p in writers for t in types_of(decls[p].get("out"))}):
            errors.append(f"Kafkas.{k}: allows {', '.join(sorted(unwritten))} but nothing writes it there")
    return errors


def with_profiles(manifest: dict, local: Path | None, fn) -> list[str]:
    """fn(configRegistry checkout): local, or the manifest's Repo at Ref."""
    if local is not None:
        return fn(local)
    registry = section(manifest, "ConfigRegistry")
    if not registry.get("Repo"):
        return fn(None)  # reported by check
    try:
        with checkout(registry["Repo"], str(registry.get("Ref") or DEFAULT_REF)) as (root, _):
            return fn(root)
    except ConfigError as e:
        return [f"ConfigRegistry: {m}" for m in e.errors]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("manifests", type=Path, nargs="+")
    p.add_argument("--schema-registry", type=Path, help="local schemaRegistry checkout (default: clone it)")
    p.add_argument("--config-registry", type=Path, help="local configRegistry checkout (default: clone the manifest's)")
    args = p.parse_args()
    with schema_registry(args.schema_registry) as registry:
        registered = registry.types()
    errors = []
    for path in args.manifests:
        manifest = load_manifest(path)
        decls, _, found = processor_decls(manifest)
        found += with_profiles(manifest, args.config_registry,
                               lambda profiles: check(manifest, decls, registered, profiles))
        errors += [f"{path}: {m}" for m in found]
        if not found:
            print(f"ok: {path}: types line up")
    if errors:
        raise ConfigError(errors)


if __name__ == "__main__":
    sys.exit(run(main))
