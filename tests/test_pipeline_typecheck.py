import json

import pytest
from conftest import load

typecheck = load("pipeline/validate/typecheck.py")

REGISTERED = {"XmlPackets", "Packets", "DecodeEnrichment", "EnrichedPackets"}


def manifest(**kafkas):
    return {
        "Name": "P",
        "ConfigRegistry": {"Repo": "r", "Ref": "main"},
        "Kafkas": {
            "Input": {"Config": "kafka/prod", "AllowedTypes": ["XmlPackets"], "Topic": "in"},
            "Mid": {"Config": "kafka/prod", "AllowedTypes": ["Packets"], "Topic": "mid"},
            "Output": {"Config": "kafka/prod", "AllowedTypes": ["DecodeEnrichment"], "Topic": "out"},
            **kafkas,
        },
        "Processors": {"Convert": {"Repo": "a"}, "Decode": {"Repo": "b"}},
        "Flow": ["Input -> Convert", "Convert -> Mid", "Mid -> Decode", "Decode -> Output"],
    }


DECLS = {
    "Convert": {"Runtime": "dotnet", "in": ["XmlPackets"], "out": ["Packets"]},
    "Decode": {"Runtime": "dotnet", "in": ["Packets"], "out": ["DecodeEnrichment"]},
}


@pytest.fixture
def profiles(tmp_path):
    (tmp_path / "kafka").mkdir()
    (tmp_path / "kafka/prod.json").write_text(json.dumps({"ConnectionSettings": {"Brokers": "k:9092"}}))
    return tmp_path


def test_valid(profiles):
    assert typecheck.check(manifest(), DECLS, REGISTERED, profiles) == []


def test_config_missing_from_config_registry(profiles):
    m = manifest(Mid={"Config": "kafka/staging", "AllowedTypes": ["Packets"], "Topic": "mid"})
    errors = typecheck.check(m, DECLS, REGISTERED, profiles)
    assert any(e.startswith("Kafkas.Mid.Config: Config 'kafka/staging' not found") for e in errors)


def test_allowed_type_not_registered(profiles):
    errors = typecheck.check(manifest(), DECLS, REGISTERED - {"Packets"}, profiles)
    assert "Kafkas.Mid.AllowedTypes: 'Packets' is not in schema-registry" in errors
    assert "Convert: 'Packets' is not in schema-registry" in errors


def test_output_type_not_allowed_by_its_kafka(profiles):
    decls = {**DECLS, "Convert": {"Runtime": "dotnet", "in": ["XmlPackets"], "out": ["Packets", "EnrichedPackets"]}}
    errors = typecheck.check(manifest(), decls, REGISTERED, profiles)
    assert any(e.startswith("Flow: Convert -> Mid: Convert writes EnrichedPackets") for e in errors)


def test_input_type_not_allowed_by_its_kafka(profiles):
    decls = {**DECLS, "Decode": {"Runtime": "dotnet", "in": ["Packets", "EnrichedPackets"], "out": ["DecodeEnrichment"]}}
    errors = typecheck.check(manifest(), decls, REGISTERED, profiles)
    assert "Decode: reads EnrichedPackets but none of Mid allows it" in errors


def test_input_type_without_producer(profiles):
    decls = {**DECLS, "Decode": {"Runtime": "dotnet", "in": ["Packets", "EnrichedPackets"], "out": ["DecodeEnrichment"]}}
    errors = typecheck.check(manifest(), decls, REGISTERED, profiles)
    assert "Decode: reads EnrichedPackets but no processor writes it and Input doesn't allow it" in errors


def test_input_type_via_input_needs_no_producer(profiles):
    errors = typecheck.check(manifest(), DECLS, REGISTERED, profiles)
    assert not any("no processor writes it" in e for e in errors)
