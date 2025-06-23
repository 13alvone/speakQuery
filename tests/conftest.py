import sys
import types
import subprocess
from pathlib import Path
import logging
import pytest


# Build shared objects as soon as this file is imported so that test modules
# importing them during collection succeed.
project_root = Path(__file__).resolve().parents[1]
_script = project_root / "build_custom_components.py"
_cmd = [sys.executable, str(_script)]
logging.info("[i] Building custom components via %s", _cmd)
try:
    subprocess.run(_cmd, check=True)
except subprocess.CalledProcessError as exc:  # pragma: no cover - only hits on failure
    raise RuntimeError(
        "[x] Failed to build custom components. Run build_custom_components.py manually"
    ) from exc

_index_dir = project_root / "functionality" / "cpp_index_call" / "build"
_datetime_dir = project_root / "functionality" / "cpp_datetime_parser" / "build"
if not any(_index_dir.glob("cpp_index_call*.so")) or not any(
    _datetime_dir.glob("cpp_datetime_parser*.so")
):
    raise RuntimeError(
        "[x] Required .so files not found. Run 'python build_custom_components.py'"
    )


@pytest.fixture
def mock_heavy_modules():
    """Provide lightweight stand-ins for heavy imports used by app.py."""
    mock_modules = {
        'lexers.antlr4_active.speakQueryLexer': types.SimpleNamespace(speakQueryLexer=object),
        'lexers.antlr4_active.speakQueryParser': types.SimpleNamespace(speakQueryParser=object),
        'lexers.speakQueryListener': types.SimpleNamespace(speakQueryListener=object),
        'handlers.JavaHandler': types.SimpleNamespace(JavaHandler=lambda *a, **k: None),
        'validation.SavedSearchValidation': types.SimpleNamespace(
            SavedSearchValidation=lambda *a, **k: None
        ),
        'functionality.FindNextCron': types.SimpleNamespace(
            suggest_next_cron_runtime=lambda *a, **k: None
        ),
        'scheduled_input_engine.ScheduledInputEngine': types.SimpleNamespace(
            ScheduledInputBackend=lambda *a, **k: None,
            crank_scheduled_input_engine=lambda: None,
        ),
        'query_engine.QueryEngine': types.SimpleNamespace(crank_query_engine=lambda *a, **k: None),
        'scheduled_input_engine.SIExecution': types.SimpleNamespace(
            SIExecution=lambda *a, **k: None
        ),
    }
    originals = {name: sys.modules.get(name) for name in mock_modules}
    sys.modules.update(mock_modules)
    try:
        yield
    finally:
        for name, original in originals.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
