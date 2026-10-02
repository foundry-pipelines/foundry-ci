# scripts

Shared tooling for Foundry pipelines.

## manifest/

`manifest.py` works with `PipelineManifest.yaml` files and the connection profiles in [configs](https://gitlab.com/foundry-common/configs). Needs `pyyaml`.

```sh
python manifest/manifest.py schema --configs ../configs    # print the IDE schema
python manifest/manifest.py render PipelineManifest.yaml   # print a manifest with profiles merged in
```

`render` clones `Configs.Repo` at `Configs.Ref`; add `--configs DIR` to use a local checkout.

### IDE autocomplete

Every push to `main` here or in `configs` regenerates the schema, so `Config:` autocompletes the current profiles. Put this at the top of a `PipelineManifest.yaml` (YAML extension in VS Code, or JetBrains):

```yaml
# yaml-language-server: $schema=https://gitlab.com/foundry-common/scripts/-/jobs/artifacts/main/raw/manifest.schema.json?job=schema
```
