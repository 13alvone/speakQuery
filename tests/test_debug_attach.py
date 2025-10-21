import importlib
import sys
from types import SimpleNamespace

import pytest


@pytest.fixture
def reload_debug_attach():
    """Reload the debug_attach module with clean state for each test."""

    import pathlib

    project_root = pathlib.Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    import devtools.debug_attach as debug_attach_module

    loaded_modules = []

    def _reload():
        module = importlib.reload(debug_attach_module)
        module._reset_for_testing()
        loaded_modules.append(module)
        return module

    yield _reload

    for module in loaded_modules:
        module._reset_for_testing()


@pytest.mark.parametrize("value, expected_attempt", [(None, False), ("0", False), ("1", True)])
def test_auto_attach_toggle(monkeypatch, reload_debug_attach, value, expected_attempt):
    if value is None:
        monkeypatch.delenv("PYCHARM_ATTACH", raising=False)
    else:
        monkeypatch.setenv("PYCHARM_ATTACH", value)

    module = reload_debug_attach()
    result = module.auto_attach_if_enabled()
    status = module.attach_status()
    assert status["attempted"] is expected_attempt
    if expected_attempt:
        assert result is False
        assert status["attached"] is False
    else:
        assert result is False
        assert status == {"attempted": False, "attached": False}


def test_auto_attach_logs_when_pydevd_missing(monkeypatch, reload_debug_attach):
    monkeypatch.setenv("PYCHARM_ATTACH", "1")
    module = reload_debug_attach()
    def raise_missing(name):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(module.importlib, "import_module", raise_missing)
    monkeypatch.setattr(module.sys, "gettrace", lambda: None)

    assert module.auto_attach_if_enabled() is False
    status = module.attach_status()
    assert status["attempted"] is True
    assert status["attached"] is False


def test_successful_attach_invokes_pydevd(monkeypatch, reload_debug_attach):
    monkeypatch.setenv("PYCHARM_ATTACH", "1")
    monkeypatch.setenv("PYCHARM_DEBUG_HOST", "debug-host")
    monkeypatch.setenv("PYCHARM_DEBUG_PORT", "6000")
    monkeypatch.setenv("PYCHARM_DEBUG_REDIRECT_OUTPUT", "1")
    monkeypatch.setenv("PYCHARM_DEBUG_TRACE_CURRENT_THREAD", "1")
    monkeypatch.setenv("PYCHARM_DEBUG_PATCH_MULTIPROCESSING", "0")
    monkeypatch.setenv("PYCHARM_DEBUG_SUSPEND", "1")

    module = reload_debug_attach()
    monkeypatch.setattr(module.sys, "gettrace", lambda: None)

    calls = []

    def fake_settrace(**kwargs):
        calls.append(kwargs)

    dummy_module = SimpleNamespace(settrace=fake_settrace)
    monkeypatch.setitem(sys.modules, "pydevd_pycharm", dummy_module)

    assert module.auto_attach_if_enabled() is True
    assert len(calls) == 1

    kwargs = calls[0]
    assert kwargs["host"] == "debug-host"
    assert kwargs["port"] == 6000
    assert kwargs["suspend"] is True
    assert kwargs["stdoutToServer"] is True
    assert kwargs["stderrToServer"] is True
    assert kwargs["trace_only_current_thread"] is True
    assert kwargs["patch_multiprocessing"] is False

    status = module.attach_status()
    assert status["attempted"] is True
    assert status["attached"] is True
