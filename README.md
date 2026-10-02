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

Checks that the datasets have a `Type`, `DataSchema` and `Topic`, that every profile exists, and that every `DataSchema` is a schema in the configs repo's `schemas/`. Each transform must have a `Repo` and read (`In`) and write (`Out`) existing datasets, and those datasets must carry the schemas declared in the transformer's `transformer.yaml` at its `Ref`. Transforms may not form a cycle. It clones what it needs; `--configs DIR` uses a local checkout instead, and `--no-transformers` skips the `transformer.yaml` checks. Foundry Studio imports the same `check()`.

A transformer declares its schemas in a `transformer.yaml` at its `Path` (the repo root by default):

```yaml
name: Isp
in: Packets
out: EnrichedPackets
description: Adds the ISP of host_ip.
```

### appsettings.json for transformers

```sh
python manifest/manifest.py appsettings PipelineManifest.yaml --transform Isp -o appsettings.json
python manifest/manifest.py appsettings PipelineManifest.yaml -o out/   # out/<transform>/appsettings.json, one per transform
```

Writes a `Foundry` section with the pipeline and transform names and the transform's `In` and `Out` datasets (topic, schema and connection settings, profiles merged in). It binds to `FoundryOptions` from the [Foundry.Common.Models](https://gitlab.com/foundry-platform/common/foundry-common) package, whose `EnsureSchemas<TIn, TOut>()` checks the datasets carry the schemas the transformer was built for. Accepts `--configs DIR` like `render`.

### IDE autocomplete

Every push to `main` here or in `configs` regenerates the schema, so `Config:` autocompletes the current profiles. Put this at the top of a `PipelineManifest.yaml` (YAML extension in VS Code, or JetBrains):

```yaml
# yaml-language-server: $schema=https://gitlab.com/foundry-platform/common/scripts/-/jobs/artifacts/main/raw/manifest.schema.json?job=schema
```
