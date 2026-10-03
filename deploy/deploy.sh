#!/usr/bin/env bash
# Applies rendered pipelines (deploy/render.py output) to the current kubectl context:
#
#   deploy.sh DIR...    e.g. deploy.sh out/PacketPipeline
#
# Each DIR is one pipeline, deployed into its own namespace named after the folder in lowercase
# (PacketPipeline -> packetpipeline) with the restricted pod security level enforced. Deployments and
# ConfigMaps (and Flink jobs) the pipeline no longer renders are deleted. Waits for the rollout.
set -euo pipefail

for dir in "$@"; do
  ns=$(basename "$dir" | tr 'A-Z' 'a-z' | sed -E 's/[^a-z0-9]+/-/g')
  echo "== $dir -> namespace $ns"
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
  kubectl label namespace "$ns" pod-security.kubernetes.io/enforce=restricted --overwrite
  kubectl apply -n "$ns" -R -f "$dir" --prune -l app.kubernetes.io/part-of \
    --prune-allowlist=core/v1/ConfigMap --prune-allowlist=apps/v1/Deployment \
    --prune-allowlist=core/v1/PersistentVolumeClaim --prune-allowlist=core/v1/ServiceAccount \
    --prune-allowlist=rbac.authorization.k8s.io/v1/Role --prune-allowlist=rbac.authorization.k8s.io/v1/RoleBinding \
    --prune-allowlist=flink.apache.org/v1beta1/FlinkDeployment
  kubectl rollout status -n "$ns" deployment -l app.kubernetes.io/part-of --timeout=5m
  # Flink jobs: the operator reports when the (re)deployed job is running.
  for fd in $(kubectl get -n "$ns" flinkdeployments -l app.kubernetes.io/part-of -o name); do
    kubectl wait -n "$ns" "$fd" --for=jsonpath='{.status.jobStatus.state}'=RUNNING --timeout=10m
  done
done
