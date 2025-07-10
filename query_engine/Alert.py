#!/usr/bin/env python3

import os
import aiosmtplib
import logging
from email.message import EmailMessage
from pathlib import Path
from dotenv import load_dotenv

# Reuse the logger configuration
logger = logging.getLogger(__name__)

env_path = os.environ.get('ENV_PATH')
if env_path and Path(env_path).exists():
    load_dotenv(env_path)
    logger.info("[i] Loaded environment variables from %s", env_path)
else:
    dotenv_path = Path(__file__).resolve().parent.parent / '.env'
    if dotenv_path.exists():
        load_dotenv(dotenv_path)
        logger.info("[i] Loaded environment variables from .env")
    else:
        logger.debug("[DEBUG] No .env file found for Alert module")

# Gmail SMTP configuration
SMTP_SERVER = os.environ.get('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 587))
SMTP_USER = os.environ.get('SMTP_USER')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')

if not SMTP_USER or not SMTP_PASSWORD:
    logger.error("SMTP_USER and SMTP_PASSWORD environment variables must be set.")
RESULTS_DIR = Path(f"{os.path.dirname(os.path.abspath(__file__))}/../executed_scheduled_searches")


async def send_email(subject, body, to, attachments=None):
    """Asynchronously sends an email with optional attachments."""
    try:
        # Create the email message
        message = EmailMessage()
        message['From'] = SMTP_USER
        message['To'] = to
        message['Subject'] = subject
        message.set_content(body)

        # Attach files if provided
        if attachments:
            for attachment in attachments:
                with open(attachment, 'rb') as f:
                    file_data = f.read()
                    file_name = attachment.name
                    message.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

        # Send the email
        await aiosmtplib.send(message, hostname=SMTP_SERVER, port=SMTP_PORT, username=SMTP_USER, password=SMTP_PASSWORD, start_tls=True)
        logger.info(f"[i] Email sent to {to} with subject: {subject}")

    except Exception as e:
        logger.error(f"[x] Failed to send email to {to}: {str(e)}")


async def email_results(task_id, recipient_email, filename):
    """Sends the results of a completed search to the specified recipient."""
    try:
        # Find the latest result file for the given task_id
        result_files = sorted(RESULTS_DIR.glob(f"{filename}"), key=lambda p: p.stat().st_mtime, reverse=True)
        if result_files:
            latest_result = result_files[0]
            logger.info(f"[i] Found result file for task {task_id}: {latest_result.name}")

            # Compose the email and send it
            subject = f"Results for Task {task_id}"
            body = f"Attached are the results for task {task_id}."
            await send_email(subject, body, recipient_email, attachments=[latest_result])

        else:
            logger.warning(f"[!] No result files found for task {task_id}.")

    except Exception as e:
        logger.error(f"[x] Error emailing results for task {task_id}: {str(e)}")
