import logging
from types import SimpleNamespace
from pathlib import Path
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
import scheduled_input_engine.ScheduledInputEngine as sie


@pytest.mark.asyncio
async def test_execute_repo_script_allows_valid_path(tmp_path, monkeypatch, caplog):
    input_repos = tmp_path / "input_repos"
    repo = input_repos / "r1"
    repo.mkdir(parents=True)
    script = repo / "script.py"
    script.write_text("print('hi')")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sie, "INPUT_REPOS_ROOT", input_repos.resolve())

    async def fake_run(path, env=None):
        return SimpleNamespace(returncode=0)

    async def fake_store(*args, **kwargs):
        return None

    monkeypatch.setattr(sie, "run_in_subprocess", fake_run)
    monkeypatch.setattr(sie, "store_execution_telemetry", fake_store)

    with caplog.at_level(logging.INFO):
        await sie.execute_repo_script(1, str(repo), "script.py", "* * * * *", None, True)

    assert "outside" not in caplog.text


@pytest.mark.asyncio
async def test_execute_repo_script_rejects_repo_outside_root(tmp_path, monkeypatch, caplog):
    input_repos = tmp_path / "input_repos"
    input_repos.mkdir()
    repo = tmp_path / "r_out"
    repo.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sie, "INPUT_REPOS_ROOT", input_repos.resolve())

    called = False

    async def fake_run(path, env=None):
        nonlocal called
        called = True
        return SimpleNamespace(returncode=0)

    async def fake_store(*args, **kwargs):
        return None

    monkeypatch.setattr(sie, "run_in_subprocess", fake_run)
    monkeypatch.setattr(sie, "store_execution_telemetry", fake_store)

    with caplog.at_level(logging.ERROR):
        await sie.execute_repo_script(1, str(repo), "script.py", "* * * * *", None, True)

    assert not called
    assert "outside allowed root" in caplog.text


@pytest.mark.asyncio
async def test_execute_repo_script_rejects_script_outside_repo(tmp_path, monkeypatch, caplog):
    input_repos = tmp_path / "input_repos"
    repo = input_repos / "r1"
    repo.mkdir(parents=True)
    evil_script = input_repos / "evil.py"
    evil_script.write_text("print('bad')")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sie, "INPUT_REPOS_ROOT", input_repos.resolve())

    called = False

    async def fake_run(path, env=None):
        nonlocal called
        called = True
        return SimpleNamespace(returncode=0)

    async def fake_store(*args, **kwargs):
        return None

    monkeypatch.setattr(sie, "run_in_subprocess", fake_run)
    monkeypatch.setattr(sie, "store_execution_telemetry", fake_store)

    with caplog.at_level(logging.ERROR):
        await sie.execute_repo_script(1, str(repo), "../evil.py", "* * * * *", None, True)

    assert not called
    assert "escapes repo root" in caplog.text
