import os
import sys
import sqlite3
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_backend_initialize_adds_api_url(tmp_path, monkeypatch):
    # Stub dependencies so ScheduledInputBackend can be imported
    stub_mod = types.ModuleType('SIExecution')
    stub_mod.SIExecution = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, 'SIExecution', stub_mod)
    cleanup_mod = types.ModuleType('SICleanup')
    cleanup_mod.cleanup_indexes = lambda: []
    monkeypatch.setitem(sys.modules, 'SICleanup', cleanup_mod)
    monkeypatch.setitem(sys.modules, 'pandas', types.ModuleType('pandas'))
    monkeypatch.setitem(sys.modules, 'RestrictedPython', types.ModuleType('RestrictedPython'))
    monkeypatch.setitem(sys.modules, 'RestrictedPython.Guards', types.ModuleType('RestrictedPython.Guards'))
    monkeypatch.setitem(sys.modules, 'RestrictedPython.utility_builtins', types.ModuleType('RestrictedPython.utility_builtins'))

    from scheduled_input_engine.ScheduledInputBackend import ScheduledInputBackend

    db_path = tmp_path / "scheduled_inputs.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE scheduled_inputs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT UNIQUE
            )"""
        )
        conn.commit()

    backend = ScheduledInputBackend.__new__(ScheduledInputBackend)
    backend.SCHEDULED_INPUTS_DB = db_path
    backend.initialize_database()

    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("PRAGMA table_info(scheduled_inputs)")
        cols = [r[1] for r in cur.fetchall()]
    assert 'api_url' in cols
