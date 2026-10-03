import json
import subprocess
import sys
from pathlib import Path

from pipeline import SchemaRegistry, breaking_changes

PACKETS = {"type": "object", "required": ["guid"],
           "properties": {"guid": {"type": "string", "format": "guid"}, "data": {"type": "string"}}}
OWNER = "foundry-platform/operators/xmltojsonprocessor"
SCRIPT = Path(__file__).resolve().parents[1] / "schema/registry.py"


def with_props(**props):
    return {**PACKETS, "properties": {**PACKETS["properties"], **props}}


def test_compatible_changes():
    assert breaking_changes(PACKETS, PACKETS) == []
    assert breaking_changes(PACKETS, with_props(isp={"type": "string"})) == []  # added optional property
    assert breaking_changes(PACKETS, {**PACKETS, "required": ["guid", "data"]}) == []  # newly required


def test_breaking_changes():
    assert breaking_changes(PACKETS, {**PACKETS, "properties": {"guid": PACKETS["properties"]["guid"]}}) == [
        "$.data: removed"]
    assert breaking_changes(PACKETS, with_props(data={"type": ["string", "null"]})) == [
        "$.data: type ['string'] -> ['null', 'string']"]
    assert breaking_changes(PACKETS, with_props(guid={"type": "string"})) == ["$.guid: format guid -> none"]
    assert breaking_changes(PACKETS, {**PACKETS, "required": []}) == ["$.guid: no longer required"]


def test_refs_and_enums():
    old = {"type": "object", "properties": {"kind": {"$ref": "#/definitions/Kind"}},
           "definitions": {"Kind": {"type": "string", "enum": ["a", "b"]}}}
    new = json.loads(json.dumps(old))
    new["definitions"]["Kind"]["enum"].append("c")
    assert breaking_changes(old, new) == ["$.kind: enum allows ['c']"]


def test_publish_versions_and_owner(tmp_path):
    registry = SchemaRegistry(tmp_path)
    assert registry.types() == set()
    assert registry.publish("Packets", PACKETS, OWNER) == 1
    assert registry.publish("Packets", PACKETS, OWNER) is None  # unchanged
    assert registry.publish("Packets", with_props(isp={"type": "string"}), OWNER) == 2
    assert registry.types() == {"Packets"}
    assert registry.owner("Packets") == OWNER
    assert sorted(f.name for f in (tmp_path / "Packets").iterdir()) == ["owner.txt", "v1.json", "v2.json"]


def test_check_owner_and_breaking(tmp_path):
    registry = SchemaRegistry(tmp_path)
    registry.publish("Packets", PACKETS, OWNER)
    assert registry.check("Packets", PACKETS, "someone/else") == [f"Packets: owned by {OWNER}, not someone/else"]
    assert registry.check("Packets", {**PACKETS, "required": []}, OWNER) == [
        "Packets: breaking change: $.guid: no longer required"]
    assert registry.check("NewType", PACKETS, "someone/else") == []


def test_cli(tmp_path):
    schemas, registry = tmp_path / "schemas", tmp_path / "registry"
    schemas.mkdir(), registry.mkdir()
    (schemas / "Packets.json").write_text(json.dumps(PACKETS))

    def cli(action, owner=OWNER):
        return subprocess.run([sys.executable, str(SCRIPT), action, str(schemas), "--owner", owner,
                               "--registry", str(registry)], capture_output=True, text=True)

    assert cli("check").returncode == 0
    assert not (registry / "Packets").exists()  # check writes nothing
    assert "v1" in cli("publish").stdout
    assert "unchanged" in cli("publish").stdout
    result = cli("publish", "someone/else")
    assert result.returncode == 1 and "owned by" in result.stdout
    (schemas / "Packets.json").write_text(json.dumps({**PACKETS, "required": []}))
    assert cli("check").returncode == 1
