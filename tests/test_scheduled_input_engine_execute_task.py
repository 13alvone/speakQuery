import os
import sys
import types
import asyncio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_execute_task_runs_without_attribute_error(tmp_path):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        backend_mod = types.ModuleType('ScheduledInputBackend')
        class _Backend:
            def __init__(self):
                pass
            def execute_scheduled_code(self, *a, **k):
                pass
        backend_mod.ScheduledInputBackend = _Backend
        sys.modules['ScheduledInputBackend'] = backend_mod

        cleanup_mod = types.ModuleType('SICleanup')
        cleanup_mod.cleanup_indexes = lambda: []
        sys.modules['SICleanup'] = cleanup_mod

        from scheduled_input_engine import ScheduledInputEngine as sie

        sie.HISTORY_DB = tmp_path / "history.db"
        async def _store(*a, **k):
            pass
        sie.store_execution_telemetry = _store

        loop.run_until_complete(
            sie.execute_task(1, "t", "print(1)", "* * * * *", "false", "", None)
        )
    finally:
        asyncio.set_event_loop(None)
        loop.close()
