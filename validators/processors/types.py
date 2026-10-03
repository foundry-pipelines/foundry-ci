#!/usr/bin/env python3
"""Checks a processor's processor.yaml: its keys, Runtime, and that every in/out type name is a C# class,
in Foundry.Common.Models or declared in the processor's own project.

    python validators/processors/types.py [PROCESSOR_DIR...] [--models FOUNDRY_COMMON_DIR]

PROCESSOR_DIR defaults to the current directory.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from pipeline import DECL_FILE, ConfigError, declared_classes, model_types, read_decl, run, types_of  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("dirs", type=Path, nargs="*", default=[Path(".")])
    p.add_argument("--models", type=Path, help="local foundry-common checkout (default: clone it)")
    args = p.parse_args()
    models = model_types(args.models)
    errors = []
    for d in args.dirs:
        try:
            decl = read_decl(d / DECL_FILE)
        except ConfigError as e:
            errors += [f"{d}: {m}" for m in e.errors]
            continue
        known = models | declared_classes(d)
        found = [f"{d}: {DECL_FILE} {key}: {t!r} is not a C# class in Foundry.Common.Models or this project"
                 for key in ("in", "out") for t in types_of(decl[key]) if t not in known]
        errors += found
        if not found:
            print(f"ok: {d}: {decl['name']}: {', '.join(decl['in'])} -> {', '.join(decl['out'])}")
    if errors:
        raise ConfigError(errors)


if __name__ == "__main__":
    sys.exit(run(main))
