import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pipeline
import pytest
from conftest import load

manifest_schema = load("ide/manifest_schema.py")

TREE = [  # two pages
    [{"path": "Packets", "type": "tree"}, {"path": "Packets/metadata.yaml", "type": "blob"},
     {"path": "Packets/v1.json", "type": "blob"}, {"path": ".gitlab-ci.yml", "type": "blob"}],
    [{"path": "Raw", "type": "tree"}, {"path": "Raw/metadata.yaml", "type": "blob"},
     {"path": "Stray/v1.json", "type": "blob"}, {"path": "Deep/x/metadata.yaml", "type": "blob"}],
]


class FakeGitLab(BaseHTTPRequestHandler):
    def do_GET(self):
        page = int(self.path.rsplit("page=", 1)[1])
        self.send_response(200)
        self.send_header("X-Next-Page", "2" if page == 1 else "")
        self.end_headers()
        self.wfile.write(json.dumps(TREE[page - 1]).encode())

    def log_message(self, *args):
        pass


@pytest.fixture
def api(monkeypatch):
    server = HTTPServer(("127.0.0.1", 0), FakeGitLab)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(pipeline, "SCHEMA_REGISTRY_API", f"http://127.0.0.1:{server.server_port}")
    yield
    server.shutdown()


def test_type_names_from_api(api):
    assert pipeline.registry_type_names() == {"Packets", "Raw"}


def test_type_names_from_clone(tmp_path):
    for name, files in {"Packets": ["metadata.yaml", "v1.json"], "Raw": ["metadata.yaml"], "Stray": ["v1.json"]}.items():
        (tmp_path / name).mkdir()
        for f in files:
            (tmp_path / name / f).write_text("owner: x\n" if f == "metadata.yaml" else "{}")
    assert pipeline.registry_type_names(tmp_path) == {"Packets", "Raw"}


def test_schemas_list_the_types(tmp_path):
    (tmp_path / "kafka").mkdir()
    (tmp_path / "kafka/prod.json").write_text("{}")
    kafka = manifest_schema.schema(tmp_path, {"Raw", "Packets"})["properties"]["Kafkas"]["additionalProperties"]
    assert kafka["properties"]["AllowedTypes"]["items"] == {"enum": ["Packets", "Raw"]}
    assert kafka["properties"]["Config"] == {"enum": ["kafka/prod"]}
    operator = manifest_schema.operator_schema({"Raw", "Packets"})
    assert operator["properties"]["in"]["items"] == operator["properties"]["out"]["items"] == {"enum": ["Packets", "Raw"]}
    assert operator["properties"]["Runtime"] == {"enum": ["dotnet", "flink"]}
    assert operator["required"] == ["Runtime", "description", "in", "name", "out"]
