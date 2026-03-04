#!/usr/bin/env python3
"""
poller.py — Inbox polling daemon.

Connects to the POP3 server on a configurable interval, downloads new
messages, and for each one:
  - Checks whether the sender is a known recipient.
  - Checks whether the email has an .xlsx attachment.
  - Runs the validator script against the attachment.
  - Stores the attachment (timestamped) in the per-sender directory.
  - Enqueues an acknowledgement or error-report email.
  - Saves the raw .eml file to data/inbox/.
  - Records the Message-ID so the same email is never processed twice.

Usage (run in a terminal or as a systemd service):
    python poller.py

The process loops forever; kill with Ctrl-C.
"""

import os
import sys
import time
import poplib
import email as email_lib
import subprocess
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config_loader import load_config
from db import (
    init_db, is_seen, mark_seen,
    get_recipient, insert_submission, update_recipient_status,
    enqueue_email,
)

# ---------------------------------------------------------------------------
# Logging setup — writes to stdout and to data/poller.log
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [poller] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/poller.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("poller")


# ---------------------------------------------------------------------------
# POP3 connection helper
# ---------------------------------------------------------------------------

def pop3_connect():
    """
    Open and authenticate a POP3 connection.
    Returns an authenticated poplib.POP3 (or POP3_SSL) object.
    """
    cfg = load_config()
    pop_cfg = cfg["email"]["pop3"]
    host = pop_cfg["host"]
    port = pop_cfg["port"]
    use_tls = pop_cfg.get("use_tls", True)
    username = cfg["email"]["username"]
    password = cfg["email"]["password"]

    if use_tls:
        conn = poplib.POP3_SSL(host, port)
    else:
        conn = poplib.POP3(host, port)
        # Optionally issue STLS here if the server supports it and you need it

    conn.user(username)
    conn.pass_(password)
    return conn


# ---------------------------------------------------------------------------
# Email parsing helpers
# ---------------------------------------------------------------------------

def parse_raw_email(raw_lines):
    """
    Convert a list of byte lines (as returned by POP3 RETR) into an
    email.message.Message object.
    """
    raw_bytes = b"\r\n".join(raw_lines)
    return email_lib.message_from_bytes(raw_bytes)


def extract_xlsx_attachment(msg):
    """
    Walk the MIME parts of a message and return (filename, bytes) for the
    first .xlsx attachment found, or (None, None) if none exists.
    """
    for part in msg.walk():
        content_disposition = part.get("Content-Disposition", "")
        content_type = part.get_content_type()

        # Match on disposition 'attachment' OR on xlsx MIME type
        is_attachment = "attachment" in content_disposition.lower()
        is_xlsx_mime = content_type in (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/octet-stream",
            "application/zip",   # xlsx are zip files; some clients mis-type them
        )

        filename = part.get_filename()
        if filename and filename.lower().endswith(".xlsx") and (is_attachment or is_xlsx_mime):
            return filename, part.get_payload(decode=True)

    return None, None


def get_sender_address(msg) -> str:
    """
    Extract a clean sender email address from the From header.
    e.g. 'John Doe <john@example.com>' → 'john@example.com'
    """
    from email.utils import parseaddr
    _, addr = parseaddr(msg.get("From", ""))
    return addr.strip().lower()


def get_message_id(msg) -> str:
    """Return the Message-ID header value, stripped of whitespace."""
    return (msg.get("Message-ID") or "").strip()


# ---------------------------------------------------------------------------
# Validator runner
# ---------------------------------------------------------------------------

def run_validator(xlsx_path: str):
    """
    Run the validator script against the given xlsx file.

    Returns:
        (success: bool, stdout: str, stderr: str)

    The validator is expected to:
      - Exit 0 on success, printing a summary to stdout.
      - Exit non-zero on failure, printing errors to stderr (and/or stdout).
    """
    cfg = load_config()
    validator = cfg["paths"]["validator_script"]

    try:
        result = subprocess.run(
            [sys.executable, validator, xlsx_path],
            capture_output=True,
            text=True,
            timeout=120,   # 2-minute timeout — adjust if your validator is slow
        )
        success = result.returncode == 0
        return success, result.stdout, result.stderr
    except FileNotFoundError:
        msg = f"Validator script not found: {validator}"
        log.error(msg)
        return False, "", msg
    except subprocess.TimeoutExpired:
        msg = "Validator timed out after 120 seconds."
        log.error(msg)
        return False, "", msg


# ---------------------------------------------------------------------------
# File storage helpers
# ---------------------------------------------------------------------------

def store_attachment(sender_email: str, original_filename: str,
                     file_bytes: bytes, received_at: datetime) -> str:
    """
    Save the xlsx bytes into data/attachments/<sender_email>/<stem>_<ts>.xlsx.
    Returns the absolute path of the stored file.
    """
    cfg = load_config()
    base_dir = cfg["paths"]["attachments_dir"]

    # Sanitise the email address for use as a directory name
    safe_email = sender_email.replace("@", "_at_").replace("/", "_")
    dest_dir = os.path.join(base_dir, safe_email)
    os.makedirs(dest_dir, exist_ok=True)

    # Build timestamped filename
    stem = os.path.splitext(original_filename)[0]
    ts = received_at.strftime("%Y%m%dT%H%M%S")
    dest_filename = f"{stem}_{ts}.xlsx"
    dest_path = os.path.join(dest_dir, dest_filename)

    with open(dest_path, "wb") as f:
        f.write(file_bytes)

    return os.path.abspath(dest_path)


def save_raw_eml(raw_lines, message_id: str, received_at: datetime):
    """
    Save the raw email bytes to data/inbox/<date>/<sanitised_msgid>.eml
    for archival and Thunderbird access.
    """
    cfg = load_config()
    inbox_dir = cfg["paths"]["inbox_dir"]
    date_dir = os.path.join(inbox_dir, received_at.strftime("%Y-%m-%d"))
    os.makedirs(date_dir, exist_ok=True)

    # Sanitise message-id for use as filename
    safe_id = message_id.replace("<", "").replace(">", "").replace("/", "_").replace(":", "_")
    eml_path = os.path.join(date_dir, f"{safe_id}.eml")

    raw_bytes = b"\r\n".join(raw_lines)
    with open(eml_path, "wb") as f:
        f.write(raw_bytes)


# ---------------------------------------------------------------------------
# Response email builder
# ---------------------------------------------------------------------------

def build_response_email(cfg, sender: str, status: bool,
                          stdout: str, stderr: str,
                          stored_filename: str, received_at: str):
    """
    Build (subject, body) for the response email using templates from config.
    """
    if status:
        tmpl = cfg["templates"]["validation_success"]
        body = tmpl["body"].format(
            sender=sender,
            summary=stdout.strip() or "(no output)",
            filename=os.path.basename(stored_filename),
            timestamp=received_at,
        )
        subject = tmpl["subject"]
    else:
        tmpl = cfg["templates"]["validation_failure"]
        # Combine stdout + stderr for the errors block (validator may use either)
        errors = "\n".join(filter(None, [stderr.strip(), stdout.strip()])) or "(no output)"
        body = tmpl["body"].format(
            sender=sender,
            errors=errors,
            timestamp=received_at,
        )
        subject = tmpl["subject"]

    return subject, body


# ---------------------------------------------------------------------------
# Core: process one email message
# ---------------------------------------------------------------------------

def process_message(raw_lines):
    """
    Parse and handle one downloaded POP3 message.
    This is the main business logic function.
    """
    cfg = load_config()
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    msg = parse_raw_email(raw_lines)
    message_id = get_message_id(msg) or f"no-id-{now_iso}"
    sender = get_sender_address(msg)

    # ------------------------------------------------------------------
    # Deduplication: skip if already processed
    # ------------------------------------------------------------------
    if is_seen(message_id):
        log.debug(f"Already processed: {message_id}")
        return

    # Always mark as seen so we never process twice, even if we skip below
    mark_seen(message_id, now_iso)

    # Always save the raw .eml
    save_raw_eml(raw_lines, message_id, now)

    log.info(f"Processing message from {sender} (ID: {message_id})")

    # ------------------------------------------------------------------
    # Check: is the sender a known recipient?
    # ------------------------------------------------------------------
    rec = get_recipient(sender)
    if rec is None:
        log.info(f"  Ignored: {sender} is not in recipients list.")
        return

    # ------------------------------------------------------------------
    # Check: optional subject keyword filter
    # ------------------------------------------------------------------
    keyword = cfg.get("activity", {}).get("subject_keyword", "")
    if keyword:
        subject_header = msg.get("Subject", "")
        if keyword.lower() not in subject_header.lower():
            log.info(f"  Ignored: subject '{subject_header}' does not match keyword '{keyword}'.")
            return

    # ------------------------------------------------------------------
    # Extract .xlsx attachment
    # ------------------------------------------------------------------
    original_filename, file_bytes = extract_xlsx_attachment(msg)
    if original_filename is None:
        log.info(f"  Ignored: no .xlsx attachment found in email from {sender}.")
        return

    log.info(f"  Found attachment: {original_filename} ({len(file_bytes)} bytes)")

    # ------------------------------------------------------------------
    # Store the attachment to disk
    # ------------------------------------------------------------------
    stored_path = store_attachment(sender, original_filename, file_bytes, now)
    log.info(f"  Stored at: {stored_path}")

    # ------------------------------------------------------------------
    # Run validator
    # ------------------------------------------------------------------
    log.info(f"  Running validator on {stored_path} ...")
    success, stdout, stderr = run_validator(stored_path)
    status_str = "success" if success else "failure"
    log.info(f"  Validation result: {status_str}")
    if stdout:
        log.debug(f"  stdout: {stdout[:200]}")
    if stderr:
        log.debug(f"  stderr: {stderr[:200]}")

    # ------------------------------------------------------------------
    # Record submission in DB
    # ------------------------------------------------------------------
    submission_id = insert_submission(
        sender_email=sender,
        received_at=now_iso,
        original_filename=original_filename,
        stored_path=stored_path,
        status=status_str,
        stdout=stdout,
        stderr=stderr,
    )

    # Update denormalised status on the recipient row
    update_recipient_status(sender, status_str, now_iso)

    # ------------------------------------------------------------------
    # Enqueue response email
    # ------------------------------------------------------------------
    subject, body = build_response_email(
        cfg=cfg,
        sender=sender,
        status=success,
        stdout=stdout,
        stderr=stderr,
        stored_filename=stored_path,
        received_at=now_iso,
    )

    email_type = "success_ack" if success else "failure_report"
    enqueue_email(
        recipient=sender,
        subject=subject,
        body=body,
        attachment_path=None,
        email_type=email_type,
        submission_id=submission_id,
        created_at=now_iso,
    )

    log.info(f"  Response email enqueued ({email_type}) for {sender}.")


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

def fetch_message_id_only(pop, index: int):
    """
    Use POP3 TOP to fetch only the headers of message at `index`
    (TOP <index> 0 means: headers + 0 body lines).
    Parse and return the Message-ID header string, or None if absent.

    This avoids downloading the full message body and attachments just
    to check whether we have already seen this message.
    """
    try:
        # TOP returns (response, ['header line', ...], octets)
        _, header_lines, _ = pop.top(index, 0)
        # Parse just the headers as an email message object
        raw_headers = b"\r\n".join(header_lines)
        msg = email_lib.message_from_bytes(raw_headers)
        return get_message_id(msg) or None
    except Exception as e:
        log.warning(f"  Could not fetch headers for message {index}: {e}")
        return None


def poll_once():
    """
    Connect to POP3, identify new (unseen) messages using headers only,
    then download and process only those. Disconnect when done.

    Flow per message:
      1. TOP  → fetch headers only (cheap)
      2. Check Message-ID against seen_messages table
      3. RETR → full download only if unseen
    """
    cfg = load_config()
    delete_after = cfg["polling"].get("delete_after_download", False)

    log.info("Connecting to POP3 server ...")
    try:
        pop = pop3_connect()
    except Exception as e:
        log.error(f"POP3 connection failed: {e}")
        return

    num_messages = len(pop.list()[1])
    log.info(f"  {num_messages} message(s) on server.")

    indices_to_delete = []

    for i in range(1, num_messages + 1):
        # --- Step 1: fetch headers only to get Message-ID ---
        message_id = fetch_message_id_only(pop, i)

        # If we cannot determine the Message-ID, we still want to
        # download the full message so process_message() can handle it
        # (it will generate a fallback ID and mark it seen).
        if message_id and is_seen(message_id):
            log.debug(f"  Message {i} already seen ({message_id}), skipping RETR.")
            continue

        # --- Step 2: full download only for unseen messages ---
        try:
            log.debug(f"  Fetching full message {i} ...")
            _, raw_lines, _ = pop.retr(i)
            process_message(raw_lines)
            if delete_after:
                indices_to_delete.append(i)
        except Exception as e:
            log.error(f"  Error processing message {i}: {e}", exc_info=True)

    # Mark for deletion on server (only if configured)
    for i in indices_to_delete:
        try:
            pop.dele(i)
        except Exception as e:
            log.warning(f"  Could not mark message {i} for deletion: {e}")

    pop.quit()
    log.info("POP3 session closed.")


def main():
    cfg = load_config()
    interval = cfg["polling"]["interval_seconds"]

    # Ensure DB and directories exist before first poll
    os.makedirs(cfg["paths"]["inbox_dir"], exist_ok=True)
    os.makedirs(cfg["paths"]["attachments_dir"], exist_ok=True)
    os.makedirs("data", exist_ok=True)
    init_db()

    log.info(f"Poller started. Polling every {interval} seconds.")
    while True:
        try:
            poll_once()
        except Exception as e:
            log.error(f"Unexpected error in poll_once: {e}", exc_info=True)
        log.info(f"Sleeping {interval}s until next poll ...")
        time.sleep(interval)


if __name__ == "__main__":
    main()
