import os
import sys
import types

# Mock heavy modules before importing app
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
sys.modules.update(mock_modules)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import allowed_file


def test_multi_dot_extension_allowed():
    assert allowed_file('example.system4.system4.parquet')

