#!/usr/bin/env python3
"""Renders a pipeline's Kubernetes files: one folder per processor under DIR/Processors/.

    python deploy/render.py PipelineManifest.yaml -o DIR [--registry CONFIG_REGISTRY_DIR] [--image-registry HOST]

Run the validators first: this only reports what stops it from rendering. Each processor gets a
ConfigMap with its appsettings.json (a Foundry section: the pipeline and processor names and the
Kafkas it reads and writes, profiles from ConfigRegistry merged in) and, by the Runtime in its
operator.yaml, a Deployment (dotnet) or a FlinkDeployment (flink). Images are
<image registry>/<project>[/<path>]:<short sha of Ref>, as ci/processor.yml and ci/flink.yml push them; the
image registry is the GitLab's container registry ($CI_REGISTRY in CI).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))
from pipeline import (ConfigError, checkout, flow, load_manifest, load_profile, processor_decls,  # noqa: E402
                      repo_url, run, section)

FLINK_VERSION = "v2_2"  # the operator's name for the Flink version ci/flink.yml builds on
FLINK_UID = 9999  # the flink user of the official Flink image


class K8sDumper(yaml.SafeDumper):
    """Plain YAML for humans: multi-line strings as | blocks, no &anchors."""
    def ignore_aliases(self, data):
        return True


K8sDumper.add_representer(str, lambda d, s: d.represent_scalar(
    "tag:yaml.org,2002:str", s, style="|" if "\n" in s else None))


def merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        out[k] = merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def dns_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def kafkas(manifest: dict, registry: Path) -> dict[str, dict]:
    """Each Kafka with its Config profile merged in (inline ConnectionSettings win, key by key)."""
    out, errors = {}, []
    for name, spec in section(manifest, "Kafkas").items():
        try:
            profile = load_profile(registry, spec.get("Config"))
        except ConfigError as e:
            errors += [f"Kafkas.{name}: {m}" for m in e.errors]
            continue
        out[name] = merge(profile, {k: v for k, v in spec.items() if k != "Config"})
    if errors:
        raise ConfigError(errors)
    return out


def appsettings(manifest: dict, kafkas: dict, processor: str, runtime: str, edges: list, registry_commit: str) -> dict:
    def kafka(name: str) -> dict:
        return {"Kafka": name, **kafkas[name]}

    ins = [a for a, b in edges if b == processor]
    outs = [b for a, b in edges if a == processor]
    foundry = {"Pipeline": manifest["Name"], "Processor": processor, "ConfigRegistryCommit": registry_commit}
    if runtime == "flink":
        foundry["Inputs"] = [kafka(k) for k in ins]
    else:
        foundry["Input"] = kafka(ins[0])
    foundry["Output"] = kafka(outs[0])
    return {"Foundry": foundry}


def image_of(spec: dict, commit: str, image_registry: str) -> str:
    m = re.match(r"^https?://[^/]+/(.+?)(\.git)?/?$", repo_url(spec["Repo"]))
    if not m:
        raise ConfigError([f"{spec['Repo']}: can only derive images for http(s) repo URLs"])
    path = "/".join(dns_name(p) for p in (m.group(1) + "/" + spec.get("Path", "")).strip("/").split("/"))
    return f"{image_registry}/{path}:{commit[:8]}"


def k8s(manifest: dict, kafkas: dict, processor: str, runtime: str, settings: dict, commit: str,
        image_registry: str) -> dict[str, dict]:
    """The Kubernetes files for one processor. No namespace: deploy.sh sets it."""
    spec = manifest["Processors"][processor]
    name, text = dns_name(processor), json.dumps(settings, indent=2) + "\n"
    labels = {"app.kubernetes.io/name": name, "app.kubernetes.io/part-of": dns_name(manifest["Name"])}
    foundry = settings["Foundry"]
    used = [*(foundry.get("Inputs") or [foundry.get("Input")]), foundry["Output"]]
    secrets = {(k.get("ConnectionSettings") or {}).get("SecretRef") for k in used}
    env = [{"name": f"Secrets__{ref}__{k.capitalize()}", "valueFrom": {"secretKeyRef": {"name": ref, "key": k}}}
           for ref in sorted(s for s in secrets if s) for k in ("username", "password")]
    configmap = {"apiVersion": "v1", "kind": "ConfigMap",
                 "metadata": {"name": f"{name}-appsettings", "labels": labels},
                 "data": {"appsettings.json": text}}
    # A config change alters the pod template, so it rolls out like an image change.
    annotations = {"foundry/config-hash": hashlib.sha256(text.encode()).hexdigest()[:16], "foundry/commit": commit}
    container_security = {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}
    if runtime == "flink":
        return {"configmap.yaml": configmap, **flink(name, labels, annotations, image_of(spec, commit, image_registry), env, container_security)}
    deployment = {
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name, "labels": labels},
        "spec": {
            "replicas": 1, "revisionHistoryLimit": 3,
            "selector": {"matchLabels": {"app.kubernetes.io/name": name}},
            "template": {
                "metadata": {"labels": labels, "annotations": annotations},
                "spec": {
                    "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                    "containers": [{
                        "name": name, "image": image_of(spec, commit, image_registry),
                        "env": [{"name": "DOTNET_ENVIRONMENT", "value": "Production"}, *env],
                        "resources": {"requests": {"cpu": "50m", "memory": "128Mi"}, "limits": {"memory": "256Mi"}},
                        "securityContext": {**container_security, "readOnlyRootFilesystem": True},
                        # Layered over the image's appsettings.json, so the processor's own settings stay.
                        "volumeMounts": [{"name": "appsettings", "mountPath": "/app/appsettings.Production.json",
                                          "subPath": "appsettings.json", "readOnly": True},
                                         {"name": "tmp", "mountPath": "/tmp"}]}],
                    "volumes": [{"name": "appsettings", "configMap": {"name": f"{name}-appsettings"}},
                                {"name": "tmp", "emptyDir": {}}]}}}}
    return {"deployment.yaml": deployment, "configmap.yaml": configmap}


def flink(name: str, labels: dict, annotations: dict, image: str, env: list, container_security: dict) -> dict[str, dict]:
    """A FlinkDeployment (Flink Kubernetes Operator) running the job in the image, with its state on a volume.

    Kubernetes HA plus upgradeMode last-state: a new image or config restarts the job from its latest
    checkpoint, so windows and other state survive deploys. The job reads its Kafkas from
    /opt/flink/foundry/appsettings.json, the same Foundry section the .NET processors get.
    """
    meta = {"labels": labels}
    data = f"{name}-state"
    deployment = {
        "apiVersion": "flink.apache.org/v1beta1", "kind": "FlinkDeployment",
        "metadata": {"name": name, **meta, "annotations": annotations},
        "spec": {
            "image": image, "flinkVersion": FLINK_VERSION, "serviceAccount": "flink",
            "flinkConfiguration": {
                "taskmanager.numberOfTaskSlots": "1",
                "state.backend.type": "hashmap",
                "execution.checkpointing.interval": "30s",
                "execution.checkpointing.dir": "file:///flink-data/checkpoints",
                "execution.checkpointing.savepoint-dir": "file:///flink-data/savepoints",
                "high-availability.type": "kubernetes",
                "high-availability.storageDir": "file:///flink-data/ha",
            },
            "podTemplate": {"metadata": {"annotations": annotations}, "spec": {
                "securityContext": {"runAsNonRoot": True, "runAsUser": FLINK_UID, "runAsGroup": FLINK_UID,
                                    "fsGroup": FLINK_UID, "seccompProfile": {"type": "RuntimeDefault"}},
                "containers": [{
                    "name": "flink-main-container", "env": env, "securityContext": container_security,
                    "volumeMounts": [{"name": "state", "mountPath": "/flink-data"},
                                     {"name": "appsettings", "mountPath": "/opt/flink/foundry", "readOnly": True}]}],
                "volumes": [{"name": "state", "persistentVolumeClaim": {"claimName": data}},
                            {"name": "appsettings", "configMap": {"name": f"{name}-appsettings"}}]}},
            "jobManager": {"resource": {"memory": "1024m", "cpu": 0.5}},
            "taskManager": {"resource": {"memory": "1536m", "cpu": 0.5}},
            "job": {"jarURI": "local:///opt/flink/usrlib/job.jar", "parallelism": 1, "upgradeMode": "last-state"},
        }}
    pvc = {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": data, **meta},
           "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}}
    # The JobManager starts its TaskManagers and keeps HA metadata in ConfigMaps.
    account = {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "flink", **meta}}
    role = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": "flink", **meta},
            "rules": [{"apiGroups": [""], "resources": ["pods", "configmaps"], "verbs": ["*"]},
                      {"apiGroups": ["apps"], "resources": ["deployments", "deployments/finalizers"], "verbs": ["*"]}]}
    binding = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": "flink", **meta},
               "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "flink"},
               "subjects": [{"kind": "ServiceAccount", "name": "flink"}]}
    return {"flinkdeployment.yaml": deployment, "pvc.yaml": pvc,
            "serviceaccount.yaml": account, "role.yaml": role, "rolebinding.yaml": binding}


def render(manifest: dict, registry: Path, registry_commit: str, image_registry: str) -> dict[str, dict[str, dict]]:
    decls, commits, errors = processor_decls(manifest)
    if errors:
        raise ConfigError(errors)
    resolved = kafkas(manifest, registry)
    edges, _ = flow(manifest)
    out = {}
    for processor in section(manifest, "Processors"):
        runtime = decls[processor]["Runtime"]
        settings = appsettings(manifest, resolved, processor, runtime, edges, registry_commit)
        out[processor] = k8s(manifest, resolved, processor, runtime, settings, commits[processor], image_registry)
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("manifest", type=Path)
    p.add_argument("-o", "--output", type=Path, required=True, help="directory")
    p.add_argument("--registry", type=Path, help="local configRegistry checkout (default: clone ConfigRegistry.Repo at Ref)")
    p.add_argument("--image-registry", default=os.environ.get("CI_REGISTRY"),
                   help="container registry the images are in (default: $CI_REGISTRY)")
    args = p.parse_args()
    if not args.image_registry:
        p.error("--image-registry or CI_REGISTRY required")
    manifest = load_manifest(args.manifest)
    if args.registry:
        rendered = render(manifest, args.registry, "local", args.image_registry)
    else:
        reg = section(manifest, "ConfigRegistry")
        with checkout(reg["Repo"], reg["Ref"]) as (root, commit):
            rendered = render(manifest, root, commit, args.image_registry)
    # Everything is built before anything is written, so an error leaves no partial output.
    for processor, files in rendered.items():
        for f, obj in files.items():
            path = args.output / "Processors" / processor / f
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(yaml.dump(obj, Dumper=K8sDumper, sort_keys=False))
            print(f"wrote {path}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(run(main))
