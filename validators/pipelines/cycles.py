#!/usr/bin/env python3
"""Fails when a pipeline's Flow has a cycle, e.g. A -> K1 -> B -> K2 -> A.

    python validators/pipelines/cycles.py PipelineManifest.yaml...
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from pipeline import ConfigError, flow, load_manifest, run  # noqa: E402


def cycles(edges: list[tuple[str, str]]) -> list[list[str]]:
    """Each cycle once, as the path around it."""
    graph: dict[str, list[str]] = {}
    for a, b in edges:
        graph.setdefault(a, []).append(b)
    found, seen, stack = [], set(), []

    def visit(node):
        if node in stack:
            loop = stack[stack.index(node):]
            if set(loop) not in [set(c) for c in found]:
                found.append(loop + [node])
            return
        if node in seen:
            return
        seen.add(node)
        stack.append(node)
        for nxt in graph.get(node, []):
            visit(nxt)
        stack.pop()

    for node in list(graph):
        visit(node)
    return found


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("manifests", type=Path, nargs="+")
    args = p.parse_args()
    errors = []
    for path in args.manifests:
        edges, bad = flow(load_manifest(path))
        found = [f"{path}: {m}" for m in bad] + [f"{path}: Flow has a cycle: {' -> '.join(c)}" for c in cycles(edges)]
        errors += found
        if not found:
            print(f"ok: {path}: no cycles")
    if errors:
        raise ConfigError(errors)


if __name__ == "__main__":
    sys.exit(run(main))
