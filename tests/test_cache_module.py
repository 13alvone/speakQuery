import os
import sys
import types
import sqlite3

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scheduled_input_engine import cache


def test_get_cached_or_fetch(monkeypatch, tmp_path):
    db = tmp_path / "cache.db"
    monkeypatch.setattr(cache, "CACHE_DB", db)
    calls = {"n": 0}

    def mock_get(url, timeout=10):
        calls["n"] += 1
        return types.SimpleNamespace(content=b"data", raise_for_status=lambda: None)

    monkeypatch.setattr(cache.requests, "get", mock_get)

    times = [0, 0, 61]

    def fake_time():
        return times.pop(0)

    monkeypatch.setattr(cache, "time", types.SimpleNamespace(time=fake_time))

    assert cache.get_cached_or_fetch("http://x", 60) == b"data"
    assert calls["n"] == 1
    assert cache.get_cached_or_fetch("http://x", 60) == b"data"
    assert calls["n"] == 1
    cache.get_cached_or_fetch("http://x", 60)
    assert calls["n"] == 2
    with sqlite3.connect(db) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM api_cache")
        assert cur.fetchone()[0] == 1
