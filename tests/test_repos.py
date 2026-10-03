from conftest import load

pipeline = load("lib/pipeline.py")
render = load("deploy/render.py")


def test_project_path_is_on_this_gitlab(monkeypatch):
    monkeypatch.setattr(pipeline, "GITLAB_URL", "https://git.x")
    assert pipeline.repo_url("group/sub/project") == "https://git.x/group/sub/project.git"
    assert pipeline.repo_url("https://other/group/project.git") == "https://other/group/project.git"
    assert pipeline.repo_url("/tmp/local/repo") == "/tmp/local/repo"


def test_image_of_path_and_url():
    for repo in ("group/IspEnricher", "https://git.x/group/IspEnricher.git"):
        assert render.image_of({"Repo": repo}, "0123456789", "reg.x") == "reg.x/group/ispenricher:01234567"
