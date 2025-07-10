import os
import sys
import types
import subprocess
from pathlib import Path
from cryptography.fernet import Fernet

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.env_crypto import encrypt_env
from utils import docker_runner

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _mock_run(cmd, capture_output, text, timeout):
    result = types.SimpleNamespace(returncode=0, stdout="ok", stderr="")
    result.cmd = cmd
    result.timeout = timeout
    _mock_run.last = result
    return result


def test_network_disabled_when_no_fetch(monkeypatch, tmp_path):
    script = PROJECT_ROOT / "tmp_script.py"
    script.write_text("print('hi')\n")
    monkeypatch.setattr(subprocess, "run", _mock_run)
    try:
        docker_runner.run_script_in_container(script)
        assert "--network" in _mock_run.last.cmd
        assert "none" in _mock_run.last.cmd
    finally:
        if script.exists():
            script.unlink()


def test_network_enabled_with_fetch(monkeypatch, tmp_path):
    script = PROJECT_ROOT / "tmp_script_fetch.py"
    script.write_text("data = FETCH_API_DATA('https://example.com')\n")
    monkeypatch.setattr(subprocess, "run", _mock_run)
    try:
        docker_runner.run_script_in_container(script)
        assert "--network" not in _mock_run.last.cmd
    finally:
        if script.exists():
            script.unlink()


def test_env_file_decrypted(monkeypatch, tmp_path):
    key = Fernet.generate_key()
    monkeypatch.setenv("MASTER_KEY", key.decode())
    plain = tmp_path / ".env"
    enc = tmp_path / ".env.enc"
    plain.write_text("SECRET=ok\n")
    encrypt_env(str(plain), str(enc))

    script = PROJECT_ROOT / "tmp_script_env.py"
    script.write_text("print('done')\n")

    monkeypatch.setattr(subprocess, "run", _mock_run)
    try:
        docker_runner.run_script_in_container(script, env_enc=enc)
        assert "--env-file" in _mock_run.last.cmd
        env_index = _mock_run.last.cmd.index("--env-file") + 1
        env_path = _mock_run.last.cmd[env_index]
        assert not os.path.exists(env_path)
    finally:
        if script.exists():
            script.unlink()
