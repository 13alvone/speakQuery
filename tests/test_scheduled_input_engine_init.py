import os
import sys
import types
import asyncio
import aiosqlite

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_initialize_scheduled_inputs_db_creates_table(tmp_path):
    # Provide lightweight stand-ins for dependencies required during import
    backend_mod = types.ModuleType('ScheduledInputBackend')
    class _Backend:
        def __init__(self):
            pass
        def execute_task(self, *a, **k):
            pass
    backend_mod.ScheduledInputBackend = _Backend
    sys.modules['ScheduledInputBackend'] = backend_mod

    cleanup_mod = types.ModuleType('SICleanup')
    def _cleanup_indexes():
        return []
    cleanup_mod.cleanup_indexes = _cleanup_indexes
    sys.modules['SICleanup'] = cleanup_mod

    from scheduled_input_engine import ScheduledInputEngine as sie

    db_path = tmp_path / "scheduled_inputs.db"
    sie.SCHEDULED_INPUTS_DB = db_path

    asyncio.run(sie.initialize_scheduled_inputs_db())

    async def check():
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='scheduled_inputs'"
            ) as cursor:
                row = await cursor.fetchone()
                assert row is not None
    asyncio.run(check())
