#!/usr/bin/env python3
"""Type-checks a pipeline: its Kafkas, Processors and Flow, against each processor's processor.yaml.

    python validators/pipelines/typecheck.py PipelineManifest.yaml... [--models FOUNDRY_COMMON_DIR]

- Kafkas have a Config, Topic and AllowedTypes; Input and Output exist. AllowedTypes are classes in
  Foundry.Common.Models (or types a processor of the pipeline declares). Nothing checks the shape of
  the data, only type names.
- ConfigRegistry has a Repo and a Ref; processors have a Repo and a processor.yaml at Ref.
- Flow connects a Kafka to a processor or a processor to a Kafka, between things the manifest defines,
  and everything it defines is connected. Nothing writes Input.
- A processor reads every type its input Kafkas allow (in), and each type it reads comes from one of
  them. Each type it writes (out) is allowed by one of its output Kafkas, and each output Kafka allows
  one of them.
- dotnet processors read one Kafka; every processor writes one.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from pipeline import (KAFKA_KEYS, MANDATORY_KAFKAS, MANIFEST_KEYS, PROCESSOR_KEYS, ConfigError,  # noqa: E402
                      flow, load_manifest, model_types, processor_decls, run, section, types_of)


def check(manifest: dict, decls: dict[str, dict], models: set[str]) -> list[str]:
    errors = [f"{k}: unknown top-level key" for k in manifest if k not in MANIFEST_KEYS]
    if not manifest.get("Name"):
        errors.append("Name: required")
    registry = section(manifest, "ConfigRegistry")
    errors += [f"ConfigRegistry.{k}: required" for k in ("Repo", "Ref") if not registry.get(k)]
    kafkas, processors = section(manifest, "Kafkas"), section(manifest, "Processors")
    errors += [f"Kafkas.{k}: required" for k in MANDATORY_KAFKAS if k not in kafkas]
    errors += [f"{n}: both a Kafka and a processor" for n in kafkas.keys() & processors.keys()]

    known = models | {t for d in decls.values() for t in types_of(d.get("in")) + types_of(d.get("out"))}
    allowed: dict[str, set[str]] = {}
    for name, spec in kafkas.items():
        spec = spec if isinstance(spec, dict) else {}
        errors += [f"Kafkas.{name}: unknown key {k!r}" for k in spec if k not in KAFKA_KEYS]
        errors += [f"Kafkas.{name}: missing {k}" for k in ("Config", "Topic", "AllowedTypes") if not spec.get(k)]
        types = types_of(spec.get("AllowedTypes"))
        if spec.get("AllowedTypes") and not types:
            errors.append(f"Kafkas.{name}.AllowedTypes: must be a list of type names, e.g. [Packets]")
        errors += [f"Kafkas.{name}.AllowedTypes: unknown type {t!r} (known: {', '.join(sorted(known))})"
                   for t in types if t not in known]
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
            continue  # no processor.yaml: reported by processor_decls
        ins, outs = set(types_of(decl["in"])), set(types_of(decl["out"]))
        if not reads[name]:
            errors.append(f"{name}: reads no Kafka")
        if decl["Runtime"] == "dotnet" and len(reads[name]) > 1:
            errors.append(f"{name}: a dotnet processor reads one Kafka, not {', '.join(reads[name])}")
        if len(writes[name]) != 1:
            errors.append(f"{name}: writes {len(writes[name])} Kafkas ({', '.join(writes[name]) or 'none'}), needs one")
        for k in reads[name]:
            if extra := allowed.get(k, set()) - ins:
                errors.append(f"{name}: {k} allows {', '.join(sorted(extra))} but {name} reads {', '.join(sorted(ins))}")
        if reads[name] and (unread := ins - set().union(*(allowed.get(k, set()) for k in reads[name]))):
            errors.append(f"{name}: reads {', '.join(sorted(unread))} but none of {', '.join(reads[name])} allows it")
        for k in writes[name]:
            if not outs & allowed.get(k, set()):
                errors.append(f"{name}: writes {', '.join(sorted(outs))} but {k} allows {', '.join(sorted(allowed.get(k, ()))) or 'nothing'}")
        if writes[name] and (homeless := outs - set().union(*(allowed.get(k, set()) for k in writes[name]))):
            errors.append(f"{name}: writes {', '.join(sorted(homeless))} but none of {', '.join(writes[name])} allows it")
    for k in kafkas:
        writers = [p for p, w in writes.items() if k in w and p in decls]
        if k != "Input" and writers and (unwritten := allowed[k] - {t for p in writers for t in types_of(decls[p]["out"])}):
            errors.append(f"Kafkas.{k}: allows {', '.join(sorted(unwritten))} but nothing writes it there")
    return errors


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("manifests", type=Path, nargs="+")
    p.add_argument("--models", type=Path, help="local foundry-common checkout (default: clone it)")
    args = p.parse_args()
    models = model_types(args.models)
    errors = []
    for path in args.manifests:
        manifest = load_manifest(path)
        decls, _, found = processor_decls(manifest)
        found += check(manifest, decls, models)
        errors += [f"{path}: {m}" for m in found]
        if not found:
            print(f"ok: {path}: types line up")
    if errors:
        raise ConfigError(errors)


if __name__ == "__main__":
    sys.exit(run(main))
