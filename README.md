# scripts

Shared tooling for Foundry pipelines. Python 3 with `pyyaml`; everything that needs a repo clones it.

## validators/

```sh
python validators/pipelines/cycles.py PipelineManifest.yaml   # Flow has no cycle
python validators/pipelines/typecheck.py PipelineManifest.yaml    # Kafkas, Processors and Flow line up by type
python validators/processors/typecheck.py [OPERATOR_DIR]          # operator.yaml's types: own classes or registered
```

Types are folders of [schemaRegistry](https://gitlab.com/foundry-platform/common/schemaRegistry) (each with a `<Type>/metadata.yaml`: `owner`, and `ignoreSchema: true` for a type without a schema; otherwise `<Type>/v<N>.json`; `--schema-registry DIR` for a local checkout). Each operator owns the classes of its out types (and of in types arriving via Input), and gets classes of the others it reads with `dotnet gen-types` (foundry-common) into `Contracts/In/`; `ci/processor.yml` / `ci/flink.yml` generate their JSON Schemas (`schema/SchemaGen`, or the pom's jsonschema-maven-plugin), fail merge requests on breaking changes or a type another operator owns, and commit new versions to schemaRegistry on main (`schema/registry.py`). A pipeline's Configs must be profiles in its configRegistry (`--config-registry DIR` for a local checkout), and every type a processor reads must be written by one or come via Input. `pipelines` CI runs the pipeline validators; `ci/processor.yml` and `ci/flink.yml` run the processor one.

A processor reads every type its input Kafkas allow, and each type it writes must be allowed by its output Kafka. dotnet processors read one Kafka, flink ones any number; every processor writes one.

```yaml
# operator.yaml, at the repo root (or the processor's Path)
name: Decode
description: Decodes the base64 payload.
Runtime: dotnet   # dotnet | flink

in: [Packets]
out: [DecodeEnrichment]
```

## deploy/

`render.py PipelineManifest.yaml -o DIR` writes each processor's Kubernetes files (a ConfigMap with its `appsettings.json`, and a Deployment or a FlinkDeployment by `Runtime`); `deploy.sh DIR...` applies them, one namespace per pipeline folder. Processors read `Foundry:Input` (flink: `Foundry:Inputs`) and `Foundry:Output`, each with `Topic`, `AllowedTypes` and `ConnectionSettings` (the `Config` profile from [configRegistry](https://gitlab.com/foundry-platform/common/configRegistry) merged in).

## lib/

`pipeline.py`: what the scripts above share.
