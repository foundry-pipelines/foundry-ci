# scripts

Shared tooling for Foundry pipelines.

## manifest/

`manifest.py` works with `PipelineManifest.yaml` files and the connection profiles in [configs](https://gitlab.com/foundry-platform/common/configs). Needs `pyyaml`.

```sh
python manifest/manifest.py schema --configs ../configs    # print the IDE schema
python manifest/manifest.py render PipelineManifest.yaml   # print a manifest with profiles merged in
```

`render` clones `Configs.Repo` at `Configs.Ref`; add `--configs DIR` to use a local checkout.

### Validating a manifest

```sh
python manifest/manifest.py validate PipelineManifest.yaml
```

Checks that the datasets have a `Type`, `DataSchema` and `Topic`, that every profile exists, and that every `DataSchema` is a schema in the configs repo's `schemas/`. Each processor must have a `Repo` and read (`In`) and write (`Out`) existing datasets, and those datasets must carry the schemas declared in the processor's `processor.yaml` at its `Ref`. Processors may not form a cycle. It clones what it needs; `--configs DIR` uses a local checkout instead, and `--no-processors` skips the `processor.yaml` checks. Foundry Studio imports the same `check()`.

A processor declares its schemas in a `processor.yaml` at its `Path` (the repo root by default):

```yaml
name: Isp
in: Packets
out: EnrichedPackets
description: Adds the ISP of host_ip.
```

### appsettings.json for processors

```sh
python manifest/manifest.py appsettings PipelineManifest.yaml --processor Isp -o appsettings.json
python manifest/manifest.py appsettings PipelineManifest.yaml -o out/   # out/<processor>/appsettings.json, one per processor
```

Writes a `Foundry` section with the pipeline and processor names and the processor's `In` and `Out` datasets (topic, schema and connection settings, profiles merged in). It binds to `FoundryOptions` from the [Foundry.Common.Models](https://gitlab.com/foundry-platform/common/foundry-common) package, whose `EnsureSchemas<TIn, TOut>()` checks the datasets carry the schemas the processor was built for. Accepts `--configs DIR` like `render`.

### IDE autocomplete

Every push to `main` here or in `configs` regenerates the schema, so `Config:` autocompletes the current profiles. Put this at the top of a `PipelineManifest.yaml` (YAML extension in VS Code, or JetBrains):

```yaml
# yaml-language-server: $schema=https://gitlab.com/foundry-platform/common/scripts/-/jobs/artifacts/main/raw/manifest.schema.json?job=schema
```
