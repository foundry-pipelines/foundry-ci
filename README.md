# foundry-ci

Shared tooling for Foundry pipelines. Python 3 with `pyyaml`; everything that needs a repo clones it.

## operator/validate, pipeline/validate

```sh
python pipeline/validate/cycles.py PipelineManifest.yaml   # Flow has no cycle
python pipeline/validate/typecheck.py PipelineManifest.yaml    # Kafkas, Processors and Flow line up by type
python operator/validate/typecheck.py [OPERATOR_DIR]          # operator.yaml's types: own classes or registered
```

Types are folders of [schema-registry](https://gitlab.com/foundry-platform/registries/schema-registry) (each with a `<Type>/metadata.yaml`: `owner`, and `ignoreSchema: true` for a type without a schema; otherwise `<Type>/v<N>.json`; `--schema-registry DIR` for a local checkout). Each operator owns the classes of its out types (and of in types arriving via Input), and gets classes of the others it reads with `dotnet models` (foundry-packages) into `Models/In/`; `operator/ci/dotnet.yml` / `operator/ci/flink.yml` generate their JSON Schemas (`operator/schema/SchemaGen`, or the pom's jsonschema-maven-plugin), fail merge requests on breaking changes or a type another operator owns, and commit new versions to schema-registry on main (`operator/schema/registry.py`). A pipeline's Configs must be profiles in its config-registry (`--config-registry DIR` for a local checkout), and every type a processor reads must be written by one or come via Input. `pipeline-manifests` CI runs the pipeline validators; `operator/ci/dotnet.yml` and `operator/ci/flink.yml` run the processor one.

A processor reads every type its input Kafkas allow, and each type it writes must be allowed by its output Kafka. dotnet processors read one Kafka, flink ones any number; every processor writes one.

```yaml
# operator.yaml, at the repo root (or the processor's Path)
name: Decode
description: Decodes the base64 payload.
Runtime: dotnet   # dotnet | flink

in: [Packets]
out: [DecodeEnrichment]
```

## pipeline/deploy

`render.py PipelineManifest.yaml -o DIR` writes each processor's Kubernetes files (a ConfigMap with its `appsettings.json`, and a Deployment or a FlinkDeployment by `Runtime`); `deploy.sh DIR...` applies them, one namespace per pipeline folder. Processors read `Foundry:Input` (flink: `Foundry:Inputs`) and `Foundry:Output`, each with `Topic`, `AllowedTypes` and `ConnectionSettings` (the `Config` profile from [config-registry](https://gitlab.com/foundry-platform/registries/config-registry) merged in).

## shared/

`pipeline.py`: what the scripts above share.
