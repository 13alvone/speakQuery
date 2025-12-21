#!/usr/bin/env python3
"""
query_engine/Alert.py

Design goals:
- NEVER fail at import-time due to missing SMTP env vars (setup / db init imports must be safe).
- Validate SMTP configuration only at send-time.
- Provide async + safe sync wrappers.
- Maintain backwards compatibility for legacy callers expecting email_results().
- Use logging (no print) with required prefixes.
"""

from __future__ import annotations

import os
import ssl
import asyncio
import logging
from dataclasses import dataclass
from email.message import EmailMessage
from typing import List, Optional, Sequence, Any

import aiosmtplib

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SMTPConfig:
    server: str
    port: int
    user: str
    password: str
    start_tls: bool
    from_addr: str


def _env_bool(val: Optional[str], default: bool) -> bool:
    if val is None:
        return default
    v = val.strip().lower()
    if v in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _env_int(val: Optional[str], default: int) -> int:
    if val is None:
        return default
    try:
        return int(val.strip())
    except Exception:
        return default


def _normalize_recipients(to_addrs: Sequence[str] | str) -> List[str]:
    if isinstance(to_addrs, str):
        raw = [to_addrs]
    else:
        raw = list(to_addrs)

    cleaned: List[str] = []
    for item in raw:
        if item is None:
            continue
        s = str(item).strip()
        if not s:
            continue
        cleaned.append(s)

    # De-dupe while preserving order
    seen = set()
    out: List[str] = []
    for addr in cleaned:
        if addr not in seen:
            seen.add(addr)
            out.append(addr)
    return out


def load_smtp_config_from_env() -> SMTPConfig:
    """
    Loads SMTP config from environment variables.

    Required at send-time:
      - SMTP_USER
      - SMTP_PASSWORD

    Optional:
      - SMTP_SERVER (default: smtp.gmail.com)
      - SMTP_PORT (default: 587)
      - SMTP_STARTTLS (default: true)
      - SMTP_FROM (default: SMTP_USER)
    """
    server = os.environ.get("SMTP_SERVER", "smtp.gmail.com").strip()
    port = _env_int(os.environ.get("SMTP_PORT"), 587)
    start_tls = _env_bool(os.environ.get("SMTP_STARTTLS"), True)

    user = (os.environ.get("SMTP_USER") or "").strip()
    password = (os.environ.get("SMTP_PASSWORD") or "").strip()
    from_addr = (os.environ.get("SMTP_FROM") or user).strip()

    if not user or not password:
        logger.error("[x] SMTP_USER and SMTP_PASSWORD environment variables must be set to send email.")
        raise RuntimeError("SMTP_USER and SMTP_PASSWORD environment variables must be set to send email.")

    if not server:
        logger.error("[x] SMTP_SERVER resolved to empty string.")
        raise RuntimeError("SMTP_SERVER must be set (or left unset to use the default).")

    if port <= 0 or port > 65535:
        logger.error("[x] SMTP_PORT is invalid: %s", port)
        raise RuntimeError("SMTP_PORT is invalid.")

    if not from_addr:
        logger.error("[x] SMTP_FROM resolved to empty string.")
        raise RuntimeError("SMTP_FROM must be set (or SMTP_USER must be set).")

    return SMTPConfig(
        server=server,
        port=port,
        user=user,
        password=password,
        start_tls=start_tls,
        from_addr=from_addr,
    )


def build_email_message(
    *,
    subject: str,
    body: str,
    to_addrs: Sequence[str] | str,
    from_addr: str,
) -> EmailMessage:
    recipients = _normalize_recipients(to_addrs)
    if not recipients:
        logger.error("[x] No valid recipient addresses provided.")
        raise ValueError("No valid recipient addresses provided.")

    msg = EmailMessage()
    msg["Subject"] = subject.strip()
    msg["From"] = from_addr.strip()
    msg["To"] = ", ".join(recipients)

    msg.set_content(body if body is not None else "")

    return msg


async def send_email_async(
    *,
    subject: str,
    body: str,
    to_addrs: Sequence[str] | str,
    smtp_config: Optional[SMTPConfig] = None,
    timeout_seconds: int = 30,
) -> None:
    """
    Async email sender. Validates config at call time.
    """
    cfg = smtp_config or load_smtp_config_from_env()
    msg = build_email_message(subject=subject, body=body, to_addrs=to_addrs, from_addr=cfg.from_addr)

    logger.info("[i] Sending email to %s via %s:%s (start_tls=%s)", msg["To"], cfg.server, cfg.port, cfg.start_tls)

    tls_context = ssl.create_default_context()

    try:
        await aiosmtplib.send(
            message=msg,
            hostname=cfg.server,
            port=cfg.port,
            username=cfg.user,
            password=cfg.password,
            start_tls=cfg.start_tls,
            tls_context=tls_context if cfg.start_tls else None,
            timeout=timeout_seconds,
        )
    except Exception as exc:
        logger.error("[x] Failed to send email: %s", exc)
        raise

    logger.info("[i] Email sent successfully.")


def send_email(
    *,
    subject: str,
    body: str,
    to_addrs: Sequence[str] | str,
    smtp_config: Optional[SMTPConfig] = None,
    timeout_seconds: int = 30,
) -> None:
    """
    Safe synchronous wrapper around send_email_async.
    """
    try:
        loop = asyncio.get_running_loop()
        if loop and loop.is_running():
            logger.error("[x] send_email() called while an event loop is running; use send_email_async() instead.")
            raise RuntimeError("send_email() cannot be called from a running event loop. Use send_email_async().")
    except RuntimeError:
        # No running loop; proceed.
        pass

    asyncio.run(
        send_email_async(
            subject=subject,
            body=body,
            to_addrs=to_addrs,
            smtp_config=smtp_config,
            timeout_seconds=timeout_seconds,
        )
    )


# ---------------------------------------------------------------------
# Backwards compatibility layer
# ---------------------------------------------------------------------
def _format_results_for_email(results: Any, max_rows: int = 200) -> str:
    """
    Best-effort formatting to keep legacy integrations stable.

    - If results looks like a pandas DataFrame, render a truncated plain-text table.
    - Otherwise stringify.
    """
    if results is None:
        return ""

    # Lazy pandas detection to avoid unnecessary import cost unless needed.
    try:
        import pandas as pd  # type: ignore
        if isinstance(results, pd.DataFrame):
            df = results
            if len(df) > max_rows:
                logger.warning("[!] Results exceed max_rows=%s; truncating for email body.", max_rows)
                df = df.head(max_rows)

            # Use a stable, readable format.
            try:
                return df.to_string(index=False)
            except Exception:
                # Fallback if df contains odd types
                return str(df.head(max_rows).to_dict(orient="records"))
    except Exception:
        # If pandas isn't available or detection fails, fall through.
        pass

    return str(results)


def email_results(*args: Any, **kwargs: Any) -> None:
    """
    Legacy entrypoint expected by QueryEngine.py: `email_results(...)`.

    Since we don't have your historical signature in this thread, this function is intentionally
    flexible and supports common calling patterns:

    Supported keyword arguments (preferred):
      - to_addrs / recipients / email_to
      - subject
      - body (optional)
      - results / df / dataframe (optional; will be formatted if provided and body missing)

    If callers pass positional args, we attempt:
      - email_results(to_addrs, subject, body_or_results)

    This function only validates SMTP config when attempting to send.
    """
    to_addrs = kwargs.get("to_addrs") or kwargs.get("recipients") or kwargs.get("email_to")
    subject = kwargs.get("subject")
    body = kwargs.get("body")

    results = kwargs.get("results")
    if results is None:
        results = kwargs.get("df")
    if results is None:
        results = kwargs.get("dataframe")

    # Positional fallback: (to_addrs, subject, body_or_results)
    if to_addrs is None and len(args) >= 1:
        to_addrs = args[0]
    if subject is None and len(args) >= 2:
        subject = args[1]
    if body is None and len(args) >= 3:
        body = args[2]

    if body is None and results is not None:
        body = _format_results_for_email(results)

    if body is None:
        body = ""

    if not to_addrs or not subject:
        logger.error("[x] email_results() missing required fields (to_addrs and subject).")
        raise ValueError("email_results() requires at least to_addrs and subject.")

    logger.info("[i] email_results() invoked (legacy shim).")
    send_email(subject=str(subject), body=str(body), to_addrs=to_addrs)

