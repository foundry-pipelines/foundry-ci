# scripts

Shared tooling for Foundry pipelines. Python 3 with `pyyaml`; everything that needs a repo clones it.

## validators/

```sh
python validators/pipelines/cycles.py PipelineManifest.yaml   # Flow has no cycle
python validators/pipelines/types.py PipelineManifest.yaml    # Kafkas, Processors and Flow line up by type
python validators/processors/types.py [PROCESSOR_DIR]         # processor.yaml's types are C# classes
```

Types are names of C# classes: the ones in [Foundry.Common.Models](https://gitlab.com/foundry-platform/common/foundry-common) (or, for a processor, its own project). Nothing checks the shape of the data. `--models DIR` uses a local foundry-common checkout instead of cloning it. `pipelines` CI runs the pipeline validators; `ci/processor.yml` and `ci/flink.yml` run the processor one.

A processor reads every type its input Kafkas allow, and each type it writes must be allowed by its output Kafka. dotnet processors read one Kafka, flink ones any number; every processor writes one.

```yaml
# processor.yaml, at the repo root (or the processor's Path)
name: Decode
description: Decodes the base64 payload.
Runtime: dotnet   # dotnet | flink

in: [Packets]
out: [DecodeEnrichment]
```

## deploy/

`render.py PipelineManifest.yaml -o DIR` writes each processor's Kubernetes files (a ConfigMap with its `appsettings.json`, and a Deployment or a FlinkDeployment by `Runtime`); `deploy.sh DIR...` applies them, one namespace per pipeline folder. Processors read `Foundry:Input` (flink: `Foundry:Inputs`) and `Foundry:Output`, each with `Topic`, `AllowedTypes` and `ConnectionSettings` (the `Config` profile from [configRegistry](https://gitlab.com/foundry-platform/common/configRegistry) merged in).

## ide/

`manifest_schema.py --registry DIR` writes the JSON Schema IDEs autocomplete manifests with. CI publishes it on every push to `main` here, in configRegistry and in foundry-common:

```yaml
# yaml-language-server: $schema=https://gitlab.com/foundry-platform/common/scripts/-/jobs/artifacts/main/raw/manifest.schema.json?job=schema
```

## lib/

`pipeline.py`: what the scripts above share.
