import os
import sys
import types
import asyncio
import aiosqlite
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_initialize_saved_searches_db_creates_table(tmp_path):
    # Provide lightweight stand-ins for heavy dependencies
    cmd_mod = types.ModuleType('CmdExecutionBackend')
    cmd_mod.process_query = lambda *a, **k: pd.DataFrame()
    sys.modules['CmdExecutionBackend'] = cmd_mod

    alert_mod = types.ModuleType('Alert')
    alert_mod.email_results = lambda *a, **k: None
    sys.modules['Alert'] = alert_mod

    pe_mod = types.ModuleType('functionality.ParquetEpochAdder')
    class _PE:
        def __init__(self, *a, **k):
            pass
        def process(self, *a, **k):
            pass
    pe_mod.ParquetEpochAdder = _PE
    sys.modules['functionality.ParquetEpochAdder'] = pe_mod

    from query_engine import QueryEngine as qe

    db_path = tmp_path / "saved_searches.db"
    qe.SEARCHES_DB = str(db_path)

    asyncio.run(qe.initialize_saved_searches_db())

    async def check():
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='saved_searches'"
            ) as cursor:
                row = await cursor.fetchone()
                assert row is not None
    asyncio.run(check())
