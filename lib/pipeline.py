"""Shared by the validators and deploy/render.py: reading PipelineManifest.yaml and processor.yaml files,
fetching repos, the connection profiles in configRegistry, and the type names in Foundry.Common.Models.

A manifest:

    Name: EnrichmentPipeline
    ConfigRegistry: {Repo: ..., Ref: main}          # both required
    Kafkas:                                         # Input and Output are required
      Input: {Config: kafka/prod, AllowedTypes: [XmlPackets], Topic: raw.xml}
    Processors:
      XmlToJsonConverter: {Repo: ..., Ref: main}    # Ref defaults to main; Path: the processor's folder
    Flow:
      - Input -> XmlToJsonConverter                 # Kafka -> processor, or processor -> Kafka

A processor.yaml (at the processor's Path, the repo root by default):

    name: Decode
    description: Decodes the base64 payload.
    Runtime: dotnet                                 # dotnet | flink
    in: [Packets]                                   # type names: C# classes
    out: [DecodeEnrichment]
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
DECL_FILE = "processor.yaml"
DECL_KEYS = {"name", "description", "Runtime", "in", "out"}
RUNTIMES = ("dotnet", "flink")
CONNECTION_KEYS = {"Brokers", "SecurityProtocol", "SaslMechanism", "SecretRef"}
PROFILE_RE = re.compile(r"^kafka/[a-z0-9][a-z0-9._-]*$")
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


# --- processor.yaml -------------------------------------------------------------------------

def read_decl(path: Path) -> dict:
    """A processor.yaml, checked: name, description, Runtime, in and out (non-empty lists of type names)."""
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
    """Each processor's processor.yaml and commit, from its Repo at Ref."""
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

def declared_classes(root: Path, pattern: str = "*.cs") -> set[str]:
    """Class, record and struct names declared in the files under root."""
    names = set()
    for f in root.rglob(pattern):
        if not {"bin", "obj", "build", ".git"} & set(f.relative_to(root).parts):
            names |= set(CLASS_RE.findall(f.read_text(errors="replace")))
    return names


def model_types(models: Path | None = None) -> set[str]:
    """The classes in Foundry.Common.Models: from a local foundry-common checkout, or cloned."""
    if models is not None:
        return declared_classes(models / MODELS_DIR) - {"IpAddressJsonConverter"}
    with checkout(MODELS_REPO, DEFAULT_REF) as (root, _):
        return declared_classes(root / MODELS_DIR) - {"IpAddressJsonConverter"}


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
