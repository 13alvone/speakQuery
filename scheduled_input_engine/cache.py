#!/usr/bin/env python3
"""Simple SQLite cache for HTTP GET requests."""
import logging
import sqlite3
import time
from pathlib import Path
import requests

logger = logging.getLogger(__name__)

# Default cache database path next to scheduled_inputs.db
CACHE_DB = Path(__file__).parent.parent / 'scheduled_inputs.db'


def get_cached_or_fetch(url: str, ttl: int) -> bytes:
    """Return cached response content or fetch ``url`` if expired.

    Parameters
    ----------
    url : str
        The URL to request.
    ttl : int
        Time to live for the cached response in seconds.

    Returns
    -------
    bytes
        Response body.
    """
    now = time.time()
    with sqlite3.connect(CACHE_DB) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS api_cache (
                   key TEXT PRIMARY KEY,
                   data BLOB,
                   expires REAL
               )"""
        )
        cur = conn.execute(
            "SELECT data, expires FROM api_cache WHERE key=?", (url,)
        )
        row = cur.fetchone()
        if row and row[1] > now:
            logger.info("[i] Cache hit for %s", url)
            return row[0]
    logger.info("[i] Fetching %s", url)
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        logger.error("[x] Request failed: %s", exc)
        raise
    data = resp.content
    expires = now + ttl
    with sqlite3.connect(CACHE_DB) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO api_cache (key, data, expires) VALUES (?, ?, ?)",
            (url, data, expires),
        )
        conn.commit()
    return data
