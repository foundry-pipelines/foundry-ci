#!/usr/bin/env python3
"""Tooling for Foundry PipelineManifest.yaml files and the configs repo's connection profiles.

    manifest.py schema --configs CONFIGS_DIR [-o F]  # generate the manifest JSON Schema for IDEs
    manifest.py render PipelineManifest.yaml         # print the manifest with profiles merged in
    manifest.py validate PipelineManifest.yaml       # check datasets, schemas and processor wiring
    manifest.py appsettings PipelineManifest.yaml --processor T [-o F]  # appsettings.json for one processor
    manifest.py appsettings PipelineManifest.yaml -o DIR               # DIR/<processor>/appsettings.json for all
    manifest.py k8s PipelineManifest.yaml -o DIR    # DIR/Processors/<processor>/{deployment,configmap}.yaml

render, validate and appsettings clone Configs.Repo at Configs.Ref; pass --configs DIR to use a
local checkout. validate also reads the schemas (schemas/*.yaml in the same configs repo) and
each processor's processor.yaml (its in/out schemas) at the processor's Ref.

A profile is <kind>/<name>.yaml in the configs repo (e.g. kafka/prod.yaml) and a manifest dataset
refers to it as `Config: kafka/prod`. The manifest's top-level `Configs: {Repo, Ref}` says which
repo and ref to read profiles from; Ref is optional and defaults to main. Inline
ConnectionSettings on a dataset override the profile's, key by key. Profiles only say how to
connect: every topic already exists and is owned by another authority, so nothing here creates,
deletes or configures topics.

Deployers can import resolve() rather than shelling out to render; editors (Foundry Studio) import
check(), so they and this CLI agree on what a valid pipeline is.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

import yaml

DEFAULT_REF = "main"
SCHEMA_DIR = "schemas"  # in the configs repo
DECL_FILE = "processor.yaml"  # a processor's name, in and out schemas, description
TOP_LEVEL = {"Name", "Configs", "Schemas", "DataSets", "Processors"}  # Schemas: legacy, ignored
DATASET_REQUIRED = ("Type", "DataSchema", "Topic")
PROCESSOR_KEYS = {"Repo", "Ref", "Path", "In", "Out"}
PROCESSOR_REQUIRED = ("Repo", "In", "Out")
KINDS = {"kafka": "Kafka"}  # profile directory -> dataset Type it applies to
PROFILE_RE = re.compile(r"^([a-z0-9-]+)/([a-z0-9][a-z0-9._-]*)$")
CONNECTION_KEYS = {"Brokers": "string", "SecurityProtocol": "string", "SaslMechanism": "string",
                   "SecretRef": "string"}
SECTIONS = {"ConnectionSettings": CONNECTION_KEYS}
DATASET_KEYS = {"Type", "Config", "DataSchema", "Topic", *SECTIONS}
SECRET_KEY_RE = re.compile(r"pass(word|wd)?|secret|token|api_?key|credential", re.I)


class ConfigError(Exception):
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("\n".join(errors))


# --- profiles -------------------------------------------------------------------------------

def profile_names(root: Path) -> list[str]:
    return sorted(f"{kind}/{p.stem}" for kind in KINDS for p in (root / kind).glob("*.yaml"))


def check_settings(where: str, data: dict, errors: list[str]) -> None:
    for section, value in data.items():
        if section not in SECTIONS:
            errors.append(f"{where}: unknown section {section!r} (expected {', '.join(SECTIONS)})")
            continue
        if not isinstance(value, dict):
            errors.append(f"{where}: {section} must be a mapping")
            continue
        for key, v in value.items():
            if key not in SECTIONS[section]:
                hint = "; reference credentials with SecretRef" if SECRET_KEY_RE.search(key) else ""
                errors.append(f"{where}: {section}.{key} is not a known key{hint}")
            elif not isinstance(v, str):
                errors.append(f"{where}: {section}.{key} must be a string")


def load_profile(root: Path, name: str) -> dict:
    if not PROFILE_RE.match(name) or name.split("/")[0] not in KINDS:
        raise ConfigError([f"Config {name!r} is not <{'|'.join(KINDS)}>/<name>"])
    path = root / f"{name}.yaml"
    if not path.is_file():
        known = ", ".join(profile_names(root)) or "none"
        raise ConfigError([f"Config {name!r} not found (known: {known})"])
    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise ConfigError([f"{name}: must be a mapping"])
    errors: list[str] = []
    check_settings(name, data, errors)
    if "Brokers" not in data.get("ConnectionSettings", {}):
        errors.append(f"{name}: ConnectionSettings.Brokers is required")
    if errors:
        raise ConfigError(errors)
    return data


# --- IDE schema -----------------------------------------------------------------------------

def manifest_schema(root: Path) -> dict:
    def settings(keys):
        return {"type": "object", "additionalProperties": False,
                "properties": {k: {"type": t} for k, t in keys.items()}}

    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Foundry PipelineManifest",
        "description": "Generated by foundry-platform/common/scripts manifest/manifest.py schema.",
        "type": "object",
        "required": ["Name", "DataSets", "Processors"],
        "additionalProperties": False,
        "properties": {
            "Name": {"type": "string"},
            "Configs": {
                "type": "object", "required": ["Repo"], "additionalProperties": False,
                "properties": {"Repo": {"type": "string"},
                               "Ref": {"type": "string", "description": f"Defaults to {DEFAULT_REF}"}},
            },
            "Schemas": {"type": "object", "additionalProperties": {"type": "string"}},
            "DataSets": {"type": "object", "additionalProperties": {
                "type": "object", "required": ["Type", "DataSchema", "Topic"],
                "additionalProperties": False,
                "properties": {
                    "Type": {"enum": sorted(KINDS.values())},
                    "Config": {"enum": profile_names(root),
                               "description": "Profile from the configs repo"},
                    "DataSchema": {"type": "string"},
                    "Topic": {"type": "string"},
                    "ConnectionSettings": settings(CONNECTION_KEYS),
                },
            }},
            "Processors": {"type": "object", "additionalProperties": {
                "type": "object", "required": ["Repo", "In", "Out"], "additionalProperties": False,
                "properties": {"Repo": {"type": "string"}, "Ref": {"type": "string"},
                               "Path": {"type": "string"},
                               "In": {"type": "string", "description": "Input dataset"},
                               "Out": {"type": "string", "description": "Output dataset"}},
            }},
        },
    }


def schema_text(root: Path) -> str:
    return json.dumps(manifest_schema(root), indent=2) + "\n"


# --- resolving a manifest -------------------------------------------------------------------

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
def configs_checkout(manifest: dict, configs_dir: Path | None):
    """Yield (configs root, commit SHA or None) for the manifest's Configs."""
    if configs_dir is not None:
        yield configs_dir, None
        return
    cfg = manifest.get("Configs") or {}
    if "Repo" not in cfg:
        raise ConfigError(["the manifest has no Configs.Repo (or pass --configs)"])
    with tempfile.TemporaryDirectory(prefix="foundry-configs-") as tmp:
        yield Path(tmp), fetch(cfg["Repo"], cfg.get("Ref", DEFAULT_REF), Path(tmp))


def merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        out[k] = merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def apply_profiles(manifest: dict, root: Path) -> dict:
    out = dict(manifest)
    datasets, errors = {}, []
    for name, ds in (manifest.get("DataSets") or {}).items():
        ds = dict(ds or {})
        inline = {s: ds.pop(s) for s in SECTIONS if s in ds}
        for key in ds.keys() - DATASET_KEYS:
            errors.append(f"DataSets.{name}: unknown key {key!r}")
        check_settings(f"DataSets.{name}", inline, errors)
        profile = {}
        if "Config" in ds:
            try:
                profile = load_profile(root, ds["Config"])
            except ConfigError as e:
                errors += [f"DataSets.{name}: {m}" for m in e.errors]
                continue
            kind = KINDS[ds["Config"].split("/")[0]]
            if ds.get("Type") and ds["Type"] != kind:
                errors.append(f"DataSets.{name}: Config {ds['Config']} is for Type {kind}, "
                              f"not {ds.get('Type')}")
        settings = merge(profile, inline)
        if "Brokers" not in settings.get("ConnectionSettings", {}):
            errors.append(f"DataSets.{name}: no Brokers (set Config or ConnectionSettings)")
        datasets[name] = {**ds, **settings}
    if errors:
        raise ConfigError(errors)
    out["DataSets"] = datasets
    return out


def resolve(manifest_path: Path, configs_dir: Path | None = None) -> dict:
    """Return the manifest with every dataset's Config merged in.

    Configs.Commit records the configs commit used (absent with a local configs_dir), so a
    watcher can tell which pipelines a configs change affects and redeploy only those.
    """
    manifest = yaml.safe_load(manifest_path.read_text()) or {}
    with configs_checkout(manifest, configs_dir) as (root, commit):
        out = apply_profiles(manifest, root)
    if commit:
        cfg = manifest["Configs"]
        out["Configs"] = {**cfg, "Ref": cfg.get("Ref", DEFAULT_REF), "Commit": commit}
    return out


# --- validating a manifest -----------------------------------------------------------------

def schema_name(stem: str) -> str:
    """Schema file stem -> schema name, as foundry-common's generator names the type: xml_packets -> XmlPackets."""
    name = "".join(part[:1].upper() + part[1:] for part in re.split(r"[^A-Za-z0-9]+", stem) if part)
    return name if name and not name[0].isdigit() else f"_{name}"


def schema_names(configs_root: Path) -> set[str]:
    return {schema_name(p.stem) for p in (configs_root / SCHEMA_DIR).glob("*.yaml")}


def read_decl(text: str) -> tuple[str | None, str | None]:
    """processor.yaml -> (in, out) schema names."""
    spec = yaml.safe_load(text) or {}
    if not isinstance(spec, dict):
        raise ValueError("not a mapping")
    return (str(spec["in"]) if spec.get("in") else None, str(spec["out"]) if spec.get("out") else None)


def fetch_decls(manifest: dict) -> tuple[dict[str, tuple[str | None, str | None]], list[str]]:
    """Each processor's (in, out) from processor.yaml in its Repo at Ref (default branch without one)."""
    decls, errors, checkouts = {}, [], {}
    with tempfile.TemporaryDirectory(prefix="foundry-processors-") as tmp:
        for name, spec in (manifest.get("Processors") or {}).items():
            if not isinstance(spec, dict) or not spec.get("Repo"):
                continue
            key = (spec["Repo"], spec.get("Ref") or "HEAD")
            try:
                if key not in checkouts:
                    checkouts[key] = Path(tmp) / str(len(checkouts))
                    fetch(*key, checkouts[key])
                path = checkouts[key] / str(spec.get("Path") or "").strip("/") / DECL_FILE
                decls[name] = read_decl(path.read_text())
            except ConfigError as e:
                errors += [f"Processors.{name}: {m}" for m in e.errors]
            except FileNotFoundError:
                errors.append(f"Processors.{name}: no {DECL_FILE} in {key[0]}@{key[1]}")
            except (yaml.YAMLError, ValueError) as e:
                errors.append(f"Processors.{name}: {DECL_FILE} unreadable ({e})")
    return decls, errors


def cycle(processors: dict) -> list[str]:
    """The processors on a cycle (one feeds another through its Out dataset), sorted."""
    feeds = {t: {r for r, rs in processors.items()
                 if r != t and (s or {}).get("Out") and (s or {}).get("Out") == (rs or {}).get("In")}
             for t, s in processors.items()}

    def reaches(start, goal):
        seen, stack = set(), list(feeds[start])
        while stack:
            n = stack.pop()
            if n == goal:
                return True
            if n not in seen:
                seen.add(n)
                stack += feeds[n]
        return False

    return sorted(t for t in processors if reaches(t, t))


def check(manifest: dict, configs_root: Path, schemas: set[str] | None = None,
          declared: dict[str, tuple[str | None, str | None]] | None = None) -> dict:
    """Validate a manifest; return it with profiles merged in, or raise ConfigError with every problem.

    schemas: the schema names DataSchema may use (None: not checked). declared: each processor's
    (in, out) schemas from its processor.yaml (a processor missing here isn't schema-checked).
    """
    if not isinstance(manifest, dict):
        raise ConfigError(["manifest: not a mapping"])
    errors = [f"{k}: unknown top-level key" for k in manifest if k not in TOP_LEVEL]
    if not manifest.get("Name"):
        errors.append("Name: required")
    datasets = manifest.get("DataSets") or {}
    processors = manifest.get("Processors") or {}
    resolved = manifest
    try:
        resolved = apply_profiles(manifest, configs_root)
    except ConfigError as e:
        errors += e.errors

    def carries(ds):
        # An unknown schema is reported on the dataset, not again on each processor using it.
        schema = (datasets.get(ds) or {}).get("DataSchema")
        return schema if schemas is None or schema in schemas else None

    for name, ds in datasets.items():
        ds = ds or {}
        errors += [f"DataSets.{name}: missing {k}" for k in DATASET_REQUIRED if not ds.get(k)]
        if schemas is not None and ds.get("DataSchema") and ds["DataSchema"] not in schemas:
            errors.append(f"DataSets.{name}.DataSchema: unknown schema {ds['DataSchema']!r} "
                          f"(known: {', '.join(sorted(schemas)) or 'none'})")
    for name, spec in processors.items():
        spec = spec if isinstance(spec, dict) else {}
        errors += [f"Processors.{name}: unknown key {k!r}" for k in spec if k not in PROCESSOR_KEYS]
        errors += [f"Processors.{name}: missing {k}" for k in PROCESSOR_REQUIRED if not spec.get(k)]
        ins, out = spec.get("In"), spec.get("Out")
        errors += [f"Processors.{name}.{role}: no dataset {ds!r}"
                   for role, ds in (("In", ins), ("Out", out)) if ds and ds not in datasets]
        if ins and ins == out:
            errors.append(f"Processors.{name}: reads and writes {ins}")
        want_in, want_out = (declared or {}).get(name) or (None, None)
        if want_in and carries(ins) and carries(ins) != want_in:
            errors.append(f"Processors.{name}.In: {ins} carries {carries(ins)} but {name} reads {want_in}")
        if want_out and carries(out) and carries(out) != want_out:
            errors.append(f"Processors.{name}.Out: {name} writes {want_out} but {out} carries {carries(out)}")
    if looped := cycle(processors):
        errors.append(f"Processors: cycle among {', '.join(looped)}")
    if errors:
        raise ConfigError(errors)
    return resolved


def validate(manifest_path: Path, configs_dir: Path | None = None, processors: bool = True) -> dict:
    """check() a manifest file, fetching what it needs: configs (profiles and schemas), processor.yaml files."""
    manifest = yaml.safe_load(manifest_path.read_text()) or {}
    decls, errors = fetch_decls(manifest) if processors else ({}, [])
    with configs_checkout(manifest, configs_dir) as (root, _):
        try:
            resolved = check(manifest, root, schema_names(root), decls)
        except ConfigError as e:
            errors = e.errors + errors
    if errors:
        raise ConfigError(errors)
    return resolved


# --- appsettings ----------------------------------------------------------------------------

def appsettings(resolved: dict, processor: str) -> dict:
    """The appsettings.json for one processor of a resolve()d manifest.

    Its "Foundry" section binds to Foundry.Common.Models.Configuration.FoundryOptions (foundry-common):
    the pipeline and processor names plus the In and Out datasets, each with its connection
    profile merged in.
    """
    processors = resolved.get("Processors") or {}
    if processor not in processors:
        raise ConfigError([f"no processor {processor!r} (known: {', '.join(processors) or 'none'})"])
    t, datasets = processors[processor], resolved["DataSets"]

    def dataset(role: str) -> dict:
        name = t.get(role)
        if name not in datasets:
            raise ConfigError([f"Processors.{processor}.{role}: no dataset {name!r}"])
        return {"DataSet": name, **datasets[name]}

    foundry = {"Pipeline": resolved.get("Name"), "Processor": processor}
    if commit := (resolved.get("Configs") or {}).get("Commit"):
        foundry["ConfigsCommit"] = commit
    foundry["Input"], foundry["Output"] = dataset("In"), dataset("Out")
    return {"Foundry": foundry}


def json_text(data: dict) -> str:
    return json.dumps(data, indent=2) + "\n"


# --- kubernetes -----------------------------------------------------------------------------

class K8sDumper(yaml.SafeDumper):
    """Plain YAML for humans: multi-line strings as | blocks, no &anchors."""
    def ignore_aliases(self, data):
        return True


K8sDumper.add_representer(str, lambda d, s: d.represent_scalar(
    "tag:yaml.org,2002:str", s, style="|" if "\n" in s else None))


def dns_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def commit_of(repo: str, ref: str) -> str:
    """The commit ref points at in repo (tags peeled)."""
    out = subprocess.run(["git", "ls-remote", repo, ref, f"{ref}^{{}}"], capture_output=True, text=True)
    lines = [l.split("\t") for l in out.stdout.splitlines() if l]
    if out.returncode or not lines:
        raise ConfigError([f"{repo}: no ref {ref!r}"])
    peeled = [sha for sha, name in lines if name.endswith("^{}")]
    return (peeled or [lines[0][0]])[0]


def image_of(spec: dict, commit: str) -> str:
    """registry.gitlab.com/<project>[/<path>]:<short sha>, as the processor CI template pushes it."""
    m = re.match(r"^https://gitlab\.com/(.+?)(\.git)?$", spec["Repo"])
    if not m:
        raise ConfigError([f"{spec['Repo']}: can only derive images for gitlab.com repos"])
    path = "/".join(dns_name(p) for p in (m.group(1) + "/" + spec.get("Path", "")).strip("/").split("/"))
    return f"registry.gitlab.com/{path}:{commit[:8]}"


def k8s(resolved: dict, processor: str, commit: str) -> dict[str, dict]:
    """deployment.yaml and configmap.yaml for one processor. No namespace: the deployer sets it."""
    spec = resolved["Processors"][processor]
    name, settings = dns_name(processor), json_text(appsettings(resolved, processor))
    labels = {"app.kubernetes.io/name": name, "app.kubernetes.io/part-of": dns_name(resolved["Name"])}
    env = [{"name": "DOTNET_ENVIRONMENT", "value": "Production"}]
    secrets = {(resolved["DataSets"][spec[r]].get("ConnectionSettings") or {}).get("SecretRef") for r in ("In", "Out")}
    for ref in sorted(s for s in secrets if s):
        env += [{"name": f"Secrets__{ref}__{k.capitalize()}",
                 "valueFrom": {"secretKeyRef": {"name": ref, "key": k}}} for k in ("username", "password")]
    configmap = {"apiVersion": "v1", "kind": "ConfigMap",
                 "metadata": {"name": f"{name}-appsettings", "labels": labels},
                 "data": {"appsettings.json": settings}}
    deployment = {
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name, "labels": labels},
        "spec": {
            "replicas": 1, "revisionHistoryLimit": 3,
            "selector": {"matchLabels": {"app.kubernetes.io/name": name}},
            "template": {
                "metadata": {"labels": labels, "annotations": {
                    # A config change alters the pod template, so it rolls out like an image change.
                    "foundry/config-hash": hashlib.sha256(settings.encode()).hexdigest()[:16],
                    "foundry/commit": commit}},
                "spec": {
                    "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                    "containers": [{
                        "name": name, "image": image_of(spec, commit), "env": env,
                        "resources": {"requests": {"cpu": "50m", "memory": "128Mi"}, "limits": {"memory": "256Mi"}},
                        "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                            "capabilities": {"drop": ["ALL"]}},
                        "volumeMounts": [{"name": "appsettings", "mountPath": "/app/appsettings.json",
                                          "subPath": "appsettings.json", "readOnly": True},
                                         {"name": "tmp", "mountPath": "/tmp"}]}],
                    "volumes": [{"name": "appsettings", "configMap": {"name": f"{name}-appsettings"}},
                                {"name": "tmp", "emptyDir": {}}]}}}}
    return {"deployment.yaml": deployment, "configmap.yaml": configmap}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("schema")
    s.add_argument("--configs", type=Path, required=True, help="configs checkout")
    s.add_argument("-o", "--output", type=Path, help="write here instead of stdout")
    r = sub.add_parser("render")
    r.add_argument("manifest", type=Path)
    r.add_argument("--configs", type=Path, help="local configs checkout (skips cloning)")
    v = sub.add_parser("validate")
    v.add_argument("manifest", type=Path)
    v.add_argument("--configs", type=Path, help="local configs checkout (skips cloning)")
    v.add_argument("--no-processors", action="store_true", help="don't fetch processor.yaml files")
    a = sub.add_parser("appsettings")
    a.add_argument("manifest", type=Path)
    a.add_argument("--processor", help="one processor; omit to write all of them under -o DIR")
    a.add_argument("-o", "--output", type=Path, help="file (with --processor) or directory (without)")
    a.add_argument("--configs", type=Path, help="local configs checkout (skips cloning)")
    k = sub.add_parser("k8s")
    k.add_argument("manifest", type=Path)
    k.add_argument("-o", "--output", type=Path, required=True, help="directory")
    k.add_argument("--configs", type=Path, help="local configs checkout (skips cloning)")
    args = p.parse_args(argv)
    if args.cmd == "appsettings" and not args.processor and not args.output:
        p.error("appsettings: pass --processor, or -o DIR to write every processor")
    try:
        if args.cmd == "schema":
            text = schema_text(args.configs)
            if args.output:
                args.output.write_text(text)
            else:
                sys.stdout.write(text)
        elif args.cmd == "render":
            yaml.safe_dump(resolve(args.manifest, args.configs), sys.stdout, sort_keys=False)
        elif args.cmd == "validate":
            m = validate(args.manifest, args.configs, not args.no_processors)
            print(f"ok: {m.get('Name')}: {len(m.get('Processors') or {})} processors, "
                  f"{len(m.get('DataSets') or {})} datasets")
        elif args.cmd == "appsettings":
            resolved = resolve(args.manifest, args.configs)
            if args.processor:
                text = json_text(appsettings(resolved, args.processor))
                if args.output:
                    args.output.write_text(text)
                else:
                    sys.stdout.write(text)
            else:
                # Build them all before writing any, so an error leaves no partial output.
                files = {t: json_text(appsettings(resolved, t)) for t in resolved.get("Processors") or {}}
                for t, text in files.items():
                    (args.output / t).mkdir(parents=True, exist_ok=True)
                    (args.output / t / "appsettings.json").write_text(text)
                    print(f"wrote {args.output / t / 'appsettings.json'}", file=sys.stderr)
        elif args.cmd == "k8s":
            resolved = validate(args.manifest, args.configs)
            files = {}
            for t, spec in (resolved.get("Processors") or {}).items():
                commit = commit_of(spec["Repo"], spec.get("Ref", DEFAULT_REF))
                for f, obj in k8s(resolved, t, commit).items():
                    files[args.output / "Processors" / t / f] = yaml.dump(obj, Dumper=K8sDumper, sort_keys=False)
            for path, text in files.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
                print(f"wrote {path}", file=sys.stderr)
    except ConfigError as e:
        for msg in e.errors:
            print(f"error: {msg}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
