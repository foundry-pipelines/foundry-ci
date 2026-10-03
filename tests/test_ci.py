from pathlib import Path

import pytest
import yaml

CI = Path(__file__).resolve().parents[1] / "ci"


@pytest.mark.parametrize("name", ["processor.yml", "flink.yml"])
def test_scripts_are_strings(name):
    """A `: ` in an unquoted script line makes it a mapping, which GitLab rejects."""
    for job, spec in yaml.safe_load((CI / name).read_text()).items():
        for key in ("before_script", "script"):
            if isinstance(spec, dict) and key in spec:
                assert all(isinstance(line, str) for line in flatten(spec[key])), f"{name}: {job}.{key}"


def flatten(lines):
    for line in lines:
        yield from flatten(line) if isinstance(line, list) else [line]
