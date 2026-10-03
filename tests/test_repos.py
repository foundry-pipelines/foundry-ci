from conftest import load

pipeline = load("shared/pipeline.py")
render = load("pipeline/deploy/render.py")


def test_project_path_is_on_this_gitlab(monkeypatch):
    monkeypatch.setenv("CI_SERVER_URL", "https://git.x")
    assert pipeline.repo_url("group/sub/project") == "https://git.x/group/sub/project.git"
    assert pipeline.repo_url("https://other/group/project.git") == "https://other/group/project.git"
    assert pipeline.repo_url("/tmp/local/repo") == "/tmp/local/repo"


def test_gitlab_from_origin():
    assert pipeline.gitlab_prefix(None, "https://git.x/group/project.git") == "https://git.x/"
    assert pipeline.gitlab_prefix(None, "git@git.x:group/project.git") == "git@git.x:"
    assert pipeline.gitlab_prefix("https://git.y/", "https://git.x/group/project.git") == "https://git.y/"
    assert pipeline.gitlab_prefix(None, None) is None


def test_image_of_path_and_url(monkeypatch):
    monkeypatch.setenv("CI_SERVER_URL", "https://git.x")
    for repo in ("group/IspEnricher", "https://git.x/group/IspEnricher.git"):
        assert render.image_of({"Repo": repo}, "0123456789", "reg.x") == "reg.x/group/ispenricher:01234567"
