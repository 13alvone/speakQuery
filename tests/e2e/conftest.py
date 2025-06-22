import os
import sys
import types
import threading
import time
import pandas as pd
import pytest
from flask import jsonify
from werkzeug.serving import make_server

@pytest.fixture(scope="session")
def server():
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

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    sys.path.insert(0, root)

    import app
    app.app.config['WTF_CSRF_ENABLED'] = False

    def fake_execute_speakQuery(query):
        return pd.DataFrame([{"col1": 1, "col2": "a"}])

    app.execute_speakQuery = fake_execute_speakQuery

    def fake_save_dataframe(request_id, df, query):
        pass
    app.save_dataframe = fake_save_dataframe

    def fake_get_saved_searches():
        return jsonify({
            'status': 'success',
            'searches': [{
                'id': 1,
                'title': 'Test Search',
                'description': 'desc',
                'cron_schedule': '* * * * *',
                'trigger': 'manual',
                'lookback': '1h',
                'owner': 'tester',
                'execution_count': 0,
                'disabled': False,
                'send_email': False,
                'next_scheduled_time': 'N/A'
            }]
        })
    app.app.view_functions['get_saved_searches'] = fake_get_saved_searches

    def fake_get_scheduled_inputs():
        return jsonify({
            'status': 'success',
            'inputs': [{
                'id': 1,
                'title': 'Test Input',
                'description': 'desc',
                'code': 'print(1)',
                'cron_schedule': '* * * * *',
                'overwrite': False,
                'subdirectory': '',
                'created_at': '2024-01-01',
                'disabled': 0,
                'status': 'Enabled'
            }]
        })
    app.app.view_functions['get_scheduled_inputs'] = fake_get_scheduled_inputs

    def fake_get_lookup_files():
        return jsonify({
            'status': 'success',
            'files': [{
                'filename': 'sample.csv',
                'filepath': '/tmp/sample.csv',
                'created_at': '2024-01-01 00:00:00',
                'updated_at': '2024-01-01 00:00:00',
                'filesize': 10,
                'permissions': '644',
                'row_count': 1
            }]
        })
    app.app.view_functions['get_lookup_files'] = fake_get_lookup_files

    server = make_server('127.0.0.1', 5001, app.app)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    time.sleep(1)
    yield 'http://127.0.0.1:5001'
    server.shutdown()
    thread.join()
