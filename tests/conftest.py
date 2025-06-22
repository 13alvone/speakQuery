import sys
import types
import pytest


@pytest.fixture
def mock_heavy_modules():
    """Provide lightweight stand-ins for heavy imports used by app.py."""
    mock_modules = {
        'lexers.antlr4_active.speakQueryLexer': types.SimpleNamespace(speakQueryLexer=object),
        'lexers.antlr4_active.speakQueryParser': types.SimpleNamespace(speakQueryParser=object),
        'lexers.speakQueryListener': types.SimpleNamespace(speakQueryListener=object),
        'handlers.JavaHandler': types.SimpleNamespace(JavaHandler=lambda *a, **k: None),
        'validation.SavedSearchValidation': types.SimpleNamespace(SavedSearchValidation=lambda *a, **k: None),
        'functionality.FindNextCron': types.SimpleNamespace(suggest_next_cron_runtime=lambda *a, **k: None),
        'scheduled_input_engine.ScheduledInputEngine': types.SimpleNamespace(
            ScheduledInputBackend=lambda *a, **k: None,
            crank_scheduled_input_engine=lambda: None,
        ),
        'query_engine.QueryEngine': types.SimpleNamespace(crank_query_engine=lambda *a, **k: None),
        'scheduled_input_engine.SIExecution': types.SimpleNamespace(SIExecution=lambda *a, **k: None),
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
