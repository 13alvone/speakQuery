"""Utilities for attaching SpeakQuery processes to a PyCharm debugger.

This module is designed to be safe to import from anywhere in the codebase.
All logic is controlled by environment variables so that production or CI
executions remain unaffected.  When :envvar:`PYCHARM_ATTACH` is set to a truthy
value, :func:`auto_attach_if_enabled` will attempt to connect the running
process to a PyCharm Debug Server using :mod:`pydevd_pycharm`.

Example usage for ad-hoc debugging::

    PYCHARM_ATTACH=1 PYCHARM_DEBUG_HOST=host.docker.internal \
    PYCHARM_DEBUG_PORT=5678 python app.py

When imported from :mod:`sitecustomize`, :func:`auto_attach_if_enabled` runs
for every interpreter start so no application code needs to change.
"""
from __future__ import annotations

import importlib
import logging
import os
import sys
import threading
from typing import Any, Dict, Optional

_LOGGER = logging.getLogger("speakquery.devtools.debug_attach")
_LOGGER.addHandler(logging.NullHandler())

# Synchronises updates to the module level state so that multi-threaded imports
# or repeated calls are safe.
_STATE_LOCK = threading.Lock()
_ATTEMPTED_ATTACH = False
_ATTACHED = False

# Normalised boolean strings recognised by :func:`_env_as_bool`.
_TRUTHY = {"1", "true", "t", "yes", "y", "on"}
_FALSY = {"0", "false", "f", "no", "n", "off"}

_DEFAULT_PORT = 5678


def _env_as_bool(name: str, *, default: Optional[bool] = None) -> Optional[bool]:
    """Return a boolean value for *name* if it contains a recognised literal.

    The conversion understands a handful of common truthy/falsy strings.  If the
    variable is present but unrecognised we log a warning and fall back to the
    provided *default* so that an invalid configuration never crashes the
    interpreter.
    """

    value = os.getenv(name)
    if value is None:
        return default

    normalised = value.strip().lower()
    if normalised in _TRUTHY:
        return True
    if normalised in _FALSY:
        return False

    _LOGGER.warning(
        "[!] Environment variable %s=%r is not a recognised boolean literal; "
        "using %r",
        name,
        value,
        default,
    )
    return default


def _env_as_int(name: str, *, default: int) -> int:
    """Return an integer value for *name* or *default* if parsing fails."""

    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        _LOGGER.warning(
            "[!] Environment variable %s=%r is not an integer; falling back to %d",
            name,
            value,
            default,
        )
        return default
    if not (0 < parsed < 65536):
        _LOGGER.warning(
            "[!] Environment variable %s=%r is outside the valid TCP range; "
            "falling back to %d",
            name,
            value,
            default,
        )
        return default
    return parsed


def _detect_default_host() -> str:
    """Best-effort detection of the host running the PyCharm Debug Server."""

    # Inside Docker the debugger usually runs on the host machine and
    # ``host.docker.internal`` resolves correctly on Windows/Mac.  On Linux the
    # developer is expected to override :envvar:`PYCHARM_DEBUG_HOST`.
    if os.path.exists("/.dockerenv"):
        return "host.docker.internal"
    # On bare-metal hosts we default to localhost which keeps the behaviour
    # unchanged for traditional debugging sessions.
    return "127.0.0.1"


def _resolve_debug_settings() -> Dict[str, Any]:
    """Collect runtime configuration sourced from environment variables."""

    host = os.getenv("PYCHARM_DEBUG_HOST", _detect_default_host())
    port = _env_as_int("PYCHARM_DEBUG_PORT", default=_DEFAULT_PORT)
    suspend = _env_as_bool("PYCHARM_DEBUG_SUSPEND", default=False)
    redirect = _env_as_bool("PYCHARM_DEBUG_REDIRECT_OUTPUT", default=False)
    trace_current_thread = _env_as_bool(
        "PYCHARM_DEBUG_TRACE_CURRENT_THREAD", default=False
    )
    patch_multiprocessing = _env_as_bool(
        "PYCHARM_DEBUG_PATCH_MULTIPROCESSING", default=True
    )

    return {
        "host": host,
        "port": port,
        "suspend": bool(suspend),
        "redirect_output": bool(redirect),
        "trace_only_current_thread": bool(trace_current_thread),
        "patch_multiprocessing": bool(patch_multiprocessing),
    }


def should_auto_attach() -> bool:
    """Return ``True`` when :envvar:`PYCHARM_ATTACH` requests a debugger."""

    return bool(_env_as_bool("PYCHARM_ATTACH", default=False))


def _import_pydevd() -> Optional[Any]:
    """Import :mod:`pydevd_pycharm` if available, returning ``None`` otherwise."""

    try:
        return importlib.import_module("pydevd_pycharm")
    except ModuleNotFoundError:
        _LOGGER.warning(
            "[!] PYCHARM_ATTACH is enabled but pydevd_pycharm is missing. "
            "Install it with 'pip install pydevd-pycharm' or configure the "
            "PyCharm helpers on the target interpreter."
        )
        return None
    except Exception as exc:  # Defensive: unexpected import errors should not kill us.
        _LOGGER.warning(
            "[!] Unable to import pydevd_pycharm: %s", exc, exc_info=True
        )
        return None


def attach_to_pycharm(*, force: bool = False) -> bool:
    """Attach the current process to a listening PyCharm debugger.

    Parameters
    ----------
    force:
        When ``True`` the attach attempt runs even if a previous successful
        attach already occurred.  This is primarily intended for tests.

    Returns
    -------
    bool
        ``True`` when the debugger successfully attaches, ``False`` otherwise.
    """

    global _ATTACHED, _ATTEMPTED_ATTACH

    with _STATE_LOCK:
        if _ATTACHED and not force:
            _LOGGER.debug(
                "[DEBUG] (PID %s) Debugger already attached; skipping re-attach.",
                os.getpid(),
            )
            return True
        if _ATTEMPTED_ATTACH and not force:
            # A previous attempt already failed.  Avoid spamming connection logs
            # unless :arg:`force` is explicitly requested.
            _LOGGER.debug(
                "[DEBUG] (PID %s) Debugger attach already attempted; skipping.",
                os.getpid(),
            )
            return False
        _ATTEMPTED_ATTACH = True

    if sys.gettrace() is not None and not force:
        _LOGGER.info(
            "[i] (PID %s) Debugger is already active according to sys.gettrace(); "
            "no additional attach needed.",
            os.getpid(),
        )
        with _STATE_LOCK:
            _ATTACHED = True
        return True

    pydevd = _import_pydevd()
    if pydevd is None:
        return False

    settings = _resolve_debug_settings()
    host = settings["host"]
    port = settings["port"]

    _LOGGER.info(
        "[i] (PID %s) Attempting PyCharm debugger attach to %s:%s (suspend=%s, "
        "redirect_output=%s)",
        os.getpid(),
        host,
        port,
        settings["suspend"],
        settings["redirect_output"],
    )

    kwargs: Dict[str, Any] = {
        "host": host,
        "port": port,
        "suspend": settings["suspend"],
        "stdoutToServer": settings["redirect_output"],
        "stderrToServer": settings["redirect_output"],
        "trace_only_current_thread": settings["trace_only_current_thread"],
    }

    patch_multiprocessing = settings["patch_multiprocessing"]
    if patch_multiprocessing is not None:
        kwargs["patch_multiprocessing"] = patch_multiprocessing

    try:
        pydevd.settrace(**kwargs)
    except TypeError as exc:
        # Older helper versions may not understand ``patch_multiprocessing``; try
        # again without it so that we remain backwards compatible.
        if "patch_multiprocessing" in kwargs:
            _LOGGER.warning(
                "[!] pydevd_pycharm.settrace rejected 'patch_multiprocessing': %s; "
                "retrying without it",
                exc,
            )
            kwargs.pop("patch_multiprocessing", None)
            try:
                pydevd.settrace(**kwargs)
            except Exception as inner_exc:  # pragma: no cover - defensive fallback
                _LOGGER.warning(
                    "[!] Second attempt to attach debugger failed: %s",
                    inner_exc,
                    exc_info=True,
                )
                return False
        else:  # pragma: no cover - unexpected API mismatch
            _LOGGER.warning(
                "[!] pydevd_pycharm.settrace raised unexpected TypeError: %s",
                exc,
                exc_info=True,
            )
            return False
    except Exception as exc:
        _LOGGER.warning(
            "[!] Failed to attach PyCharm debugger at %s:%s: %s",
            host,
            port,
            exc,
        )
        return False

    with _STATE_LOCK:
        _ATTACHED = True
    _LOGGER.info("[i] (PID %s) PyCharm debugger attached successfully.", os.getpid())
    return True


def auto_attach_if_enabled() -> bool:
    """Attempt to attach when :envvar:`PYCHARM_ATTACH` is enabled."""

    if not should_auto_attach():
        _LOGGER.debug(
            "[DEBUG] (PID %s) PYCHARM_ATTACH not set; skipping PyCharm attach.",
            os.getpid(),
        )
        return False

    return attach_to_pycharm()


def attach_status() -> Dict[str, bool]:
    """Return a snapshot of the module's attach bookkeeping state."""

    with _STATE_LOCK:
        return {"attempted": _ATTEMPTED_ATTACH, "attached": _ATTACHED}


def _reset_for_testing() -> None:
    """Reset module state so unit tests can run in isolation."""

    global _ATTACHED, _ATTEMPTED_ATTACH
    with _STATE_LOCK:
        _ATTACHED = False
        _ATTEMPTED_ATTACH = False

