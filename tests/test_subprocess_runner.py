import asyncio
from pathlib import Path
from scheduled_input_engine.subprocess_runner import run_in_subprocess


def test_run_script_success(tmp_path):
    script = tmp_path / "hello.py"
    script.write_text('print("hello")')
    result = asyncio.run(run_in_subprocess(script, timeout=5))
    assert result.returncode == 0
    assert "hello" in result.stdout


def test_run_script_timeout(tmp_path):
    script = tmp_path / "sleep.py"
    script.write_text('import time; time.sleep(2)')
    result = asyncio.run(run_in_subprocess(script, timeout=0.5))
    assert result.returncode == -1
    assert "Timeout" in result.stderr


def test_run_script_failure(tmp_path):
    script = tmp_path / "fail.py"
    script.write_text('raise Exception("boom")')
    result = asyncio.run(run_in_subprocess(script, timeout=5))
    assert result.returncode != 0
    assert "boom" in result.stderr


def test_run_script_with_env(tmp_path):
    script = tmp_path / "env.py"
    script.write_text('import os; print(os.environ.get("FOO"))')
    result = asyncio.run(
        run_in_subprocess(script, timeout=5, env={"FOO": "bar"})
    )
    assert result.returncode == 0
    assert "bar" in result.stdout
