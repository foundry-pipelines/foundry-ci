"""Shared by the validators and deploy/render.py: reading PipelineManifest.yaml and operator.yaml files,
fetching repos, the connection profiles in configRegistry, and the type schemas in schemaRegistry.

A manifest:

    Name: EnrichmentPipeline
    ConfigRegistry: {Repo: ..., Ref: main}          # both required
    Kafkas:                                         # Input and Output are required
      Input: {Config: kafka/prod, AllowedTypes: [XmlPackets], Topic: raw.xml}
    Processors:
      XmlToJsonConverter: {Repo: ..., Ref: main}    # Ref defaults to main; Path: the processor's folder
    Flow:
      - Input -> XmlToJsonConverter                 # Kafka -> processor, or processor -> Kafka

A operator.yaml (at the processor's Path, the repo root by default):

    name: Decode
    description: Decodes the base64 payload.
    Runtime: dotnet                                 # dotnet | flink
    in: [Packets]                                   # type names: types in schemaRegistry
    out: [DecodeEnrichment]                         # classes of this operator, which registers them
"""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path

import yaml

DEFAULT_REF = "main"
MANIFEST_KEYS = {"Name", "ConfigRegistry", "Kafkas", "Processors", "Flow"}
KAFKA_KEYS = {"Config", "AllowedTypes", "Topic", "ConnectionSettings"}
MANDATORY_KAFKAS = ("Input", "Output")
PROCESSOR_KEYS = {"Repo", "Ref", "Path"}
DECL_FILE = "operator.yaml"
DECL_KEYS = {"name", "description", "Runtime", "in", "out"}
RUNTIMES = ("dotnet", "flink")
CONNECTION_KEYS = {"Brokers", "SecurityProtocol", "SaslMechanism", "SecretRef"}
PROFILE_RE = re.compile(r"^kafka/[a-z0-9][a-z0-9._-]*$")
SCHEMA_REGISTRY_REPO = "https://gitlab.com/foundry-platform/common/schemaRegistry.git"
MODELS_REPO = "https://gitlab.com/foundry-platform/common/foundry-common.git"
MODELS_DIR = "src/Foundry.Common.Models"
CLASS_RE = re.compile(r"^\s*(?:(?:public|internal|sealed|abstract|static|partial|final)\s+)*(?:class|record|struct)\s+([A-Za-z_]\w*)", re.M)


class ConfigError(Exception):
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("\n".join(errors))


def run(main) -> int:
    """Run a validator's main(), printing ConfigError messages."""
    try:
        main()
    except ConfigError as e:
        for msg in e.errors:
            print(f"error: {msg}")
        return 1
    return 0


# --- manifests ------------------------------------------------------------------------------

def load_manifest(path: Path) -> dict:
    try:
        manifest = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        raise ConfigError([f"{path}: not YAML ({e})"])
    if not isinstance(manifest, dict):
        raise ConfigError([f"{path}: not a mapping"])
    return manifest


def section(manifest: dict, key: str) -> dict:
    value = manifest.get(key)
    return value if isinstance(value, dict) else {}


def flow(manifest: dict) -> tuple[list[tuple[str, str]], list[str]]:
    """Flow entries as (from, to) pairs, and the entries that aren't `A -> B`."""
    edges, errors = [], []
    entries = manifest.get("Flow")
    if not isinstance(entries, list):
        return [], ["Flow: required, a list of `A -> B` entries"]
    for i, entry in enumerate(entries):
        parts = [p.strip() for p in str(entry).split("->")] if isinstance(entry, str) else []
        if len(parts) != 2 or not all(parts):
            errors.append(f"Flow[{i}]: {entry!r} is not `A -> B`")
        else:
            edges.append((parts[0], parts[1]))
    return edges, errors


def types_of(value) -> list[str]:
    return [str(v) for v in value] if isinstance(value, list) else []


# --- repos ----------------------------------------------------------------------------------

def fetch(repo: str, ref: str, dest: Path) -> str:
    """Check out repo at ref (branch, tag or commit) into dest; return the commit SHA."""
    def git(*args):
        return subprocess.run(["git", "-C", str(dest), *args], check=True,
                              capture_output=True, text=True).stdout.strip()
    dest.mkdir(parents=True, exist_ok=True)
    try:
        git("init", "-q")
        git("fetch", "-q", "--depth", "1", repo, ref)
        git("checkout", "-q", "FETCH_HEAD")
    except subprocess.CalledProcessError as e:
        raise ConfigError([f"cannot fetch {repo}@{ref}: {e.stderr.strip()}"]) from None
    return git("rev-parse", "HEAD")


@contextmanager
def checkout(repo: str, ref: str):
    """Yield (directory, commit SHA) of repo at ref, removed afterwards."""
    with tempfile.TemporaryDirectory(prefix="foundry-") as tmp:
        yield Path(tmp), fetch(repo, ref, Path(tmp))


# --- operator.yaml -------------------------------------------------------------------------

def read_decl(path: Path) -> dict:
    """A operator.yaml, checked: name, description, Runtime, in and out (non-empty lists of type names)."""
    if not path.is_file():
        raise ConfigError([f"no {DECL_FILE}"])
    try:
        decl = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        raise ConfigError([f"{DECL_FILE}: not YAML ({e})"])
    if not isinstance(decl, dict):
        raise ConfigError([f"{DECL_FILE}: not a mapping"])
    errors = [f"{DECL_FILE}: unknown key {k!r}" for k in decl if k not in DECL_KEYS]
    errors += [f"{DECL_FILE}: missing {k}" for k in sorted(DECL_KEYS) if not decl.get(k)]
    if decl.get("Runtime") and decl["Runtime"] not in RUNTIMES:
        errors.append(f"{DECL_FILE}: Runtime {decl['Runtime']!r} is not one of {', '.join(RUNTIMES)}")
    for key in ("in", "out"):
        if decl.get(key) and not (isinstance(decl[key], list) and all(isinstance(t, str) for t in decl[key])):
            errors.append(f"{DECL_FILE}: {key} must be a list of type names, e.g. [Packets]")
    if errors:
        raise ConfigError(errors)
    return decl


def processor_decls(manifest: dict) -> tuple[dict[str, dict], dict[str, str], list[str]]:
    """Each processor's operator.yaml and commit, from its Repo at Ref."""
    decls, commits, errors = {}, {}, []
    for name, spec in section(manifest, "Processors").items():
        if not isinstance(spec, dict) or not spec.get("Repo"):
            continue  # reported by the types validator
        try:
            with checkout(spec["Repo"], str(spec.get("Ref") or DEFAULT_REF)) as (root, commit):
                decls[name] = read_decl(root / str(spec.get("Path") or "").strip("/") / DECL_FILE)
                commits[name] = commit
        except ConfigError as e:
            errors += [f"Processors.{name}: {m}" for m in e.errors]
    return decls, commits, errors


# --- types ----------------------------------------------------------------------------------

def declared_classes(root: Path, patterns: tuple[str, ...] = ("*.cs", "*.java")) -> set[str]:
    """Class, record and struct names declared in the C# and Java files under root."""
    names = set()
    for f in (f for pattern in patterns for f in root.rglob(pattern)):
        if not {"bin", "obj", "build", "target", ".git"} & set(f.relative_to(root).parts):
            names |= set(CLASS_RE.findall(f.read_text(errors="replace")))
    return names


def model_types(models: Path | None = None) -> set[str]:
    """The classes in Foundry.Common.Models: from a local foundry-common checkout, or cloned."""
    if models is not None:
        return declared_classes(models / MODELS_DIR) - {"IpAddressJsonConverter"}
    with checkout(MODELS_REPO, DEFAULT_REF) as (root, _):
        return declared_classes(root / MODELS_DIR) - {"IpAddressJsonConverter"}


class SchemaRegistry:
    """The schemaRegistry repo: <TypeName>/v<N>.json, the type's schema versions, and <TypeName>/owner.txt,
    the operator that owns and alone publishes it (its project path, plus /<PROCESSOR_PATH> if set)."""

    def __init__(self, root: Path):
        self.root = root

    def types(self) -> set[str]:
        return {d.name for d in self.root.iterdir() if d.is_dir() and self.versions(d.name)}

    def versions(self, name: str) -> list[int]:
        return sorted(int(m.group(1)) for f in (self.root / name).glob("v*.json")
                      if (m := re.fullmatch(r"v(\d+)\.json", f.name)))

    def latest(self, name: str) -> dict | None:
        versions = self.versions(name) if (self.root / name).is_dir() else []
        return json.loads((self.root / name / f"v{versions[-1]}.json").read_text()) if versions else None

    def owner(self, name: str) -> str | None:
        path = self.root / name / "owner.txt"
        return path.read_text().strip() if path.is_file() else None

    def check(self, name: str, schema: dict, owner: str) -> list[str]:
        """Why owner can't publish schema as name's next version: not its owner, or a breaking change."""
        if (current := self.owner(name)) and current != owner:
            return [f"{name}: owned by {current}, not {owner}"]
        latest = self.latest(name)
        return [] if latest is None else [f"{name}: breaking change: {m}" for m in breaking_changes(latest, schema)]

    def publish(self, name: str, schema: dict, owner: str) -> int | None:
        """Write schema as name's next version (and owner.txt for a new type); None if it equals the latest."""
        if self.latest(name) == schema:
            return None
        version = (self.versions(name) or [0])[-1] + 1 if (self.root / name).is_dir() else 1
        (self.root / name).mkdir(exist_ok=True)
        (self.root / name / "owner.txt").write_text(owner + "\n")
        (self.root / name / f"v{version}.json").write_text(json.dumps(schema, indent=2) + "\n")
        return version


@contextmanager
def schema_registry(local: Path | None = None):
    """A SchemaRegistry: a local checkout, or SCHEMA_REGISTRY_REPO at main, cloned."""
    if local is not None:
        yield SchemaRegistry(local)
        return
    with checkout(SCHEMA_REGISTRY_REPO, DEFAULT_REF) as (root, _):
        yield SchemaRegistry(root)


def breaking_changes(old: dict, new: dict) -> list[str]:
    """What new breaks for consumers reading data that old described: a property removed or no longer
    required, a type, format or enum value it now allows. Added optional properties don't break."""
    def resolve(node, root):
        for _ in range(20):  # $ref -> $ref chains, within the schema
            if not (isinstance(node, dict) and str(node.get("$ref", "")).startswith("#/")):
                break
            target = root
            for part in node["$ref"][2:].split("/"):
                target = target.get(part, {}) if isinstance(target, dict) else {}
            node = target
        return node if isinstance(node, dict) else {}

    def types(node):
        t = node.get("type")
        return set(t) if isinstance(t, list) else {t} if t else set()

    def walk(o, n, where, depth=0):
        o, n = resolve(o, old), resolve(n, new)
        found = []
        if depth > 30:  # a recursive type: what's above is enough
            return found
        if types(o) and (not types(n) or types(n) - types(o)):
            found.append(f"{where}: type {sorted(types(o))} -> {sorted(types(n)) or 'any'}")
        if "format" in o and n.get("format") != o["format"]:
            found.append(f"{where}: format {o['format']} -> {n.get('format', 'none')}")
        if "enum" in o and "enum" not in n:
            found.append(f"{where}: no longer an enum")
        elif "enum" in o and (extra := [v for v in n["enum"] if v not in o["enum"]]):
            found.append(f"{where}: enum allows {extra}")
        for name, prop in (o.get("properties") or {}).items():
            if name not in (n.get("properties") or {}):
                found.append(f"{where}.{name}: removed")
            else:
                found += walk(prop, n["properties"][name], f"{where}.{name}", depth + 1)
        found += [f"{where}.{r}: no longer required" for r in o.get("required", []) if r not in n.get("required", [])]
        if isinstance(o.get("items"), dict):
            found += walk(o["items"], n.get("items") or {}, f"{where}[]", depth + 1)
        return found

    return walk(old, new, "$")


# --- profiles -------------------------------------------------------------------------------

def load_profile(root: Path, name: str) -> dict:
    if not PROFILE_RE.match(str(name)):
        raise ConfigError([f"Config {name!r} is not kafka/<name>"])
    path = root / f"{name}.json"
    if not path.is_file():
        known = ", ".join(sorted(f"kafka/{p.stem}" for p in (root / "kafka").glob("*.json"))) or "none"
        raise ConfigError([f"Config {name!r} not found (known: {known})"])
    data = json.loads(path.read_text())
    settings = data.get("ConnectionSettings") or {}
    if unknown := set(settings) - CONNECTION_KEYS:
        raise ConfigError([f"{name}: unknown ConnectionSettings {', '.join(sorted(unknown))}"])
    return data
