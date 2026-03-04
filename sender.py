#!/usr/bin/env python3
"""
sender.py — Outgoing email sender daemon.

Runs in a loop, checking the email_queue table at a configurable interval.

Behaviour:
  - If autosend is ON  → sends ALL unsent queued emails.
  - If autosend is OFF → sends only emails that have been approved=1
                         (via the dashboard or approve_all_pending()).

Each email that is sent is marked with its sent_at timestamp.
If the sender is also an initial-email send, the recipient's initial_sent_at
is updated.

Usage:
    python sender.py
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config_loader import load_config, reload_config, is_autosend
from db import (
    init_db, get_pending_queue, get_approved_queue,
    mark_sent, mark_initial_sent, get_queue_item,
    approve_all_pending,
)
from mailer import send_email

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [sender] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/sender.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("sender")


def send_queued():
    """
    Fetch the appropriate queue (all pending if autosend, else approved only)
    and attempt to send each email via SMTP.
    """
    # Re-read config on each cycle so autosend toggle takes effect immediately
    reload_config()
    autosend = is_autosend()

    if autosend:
        # In autosend mode, approve everything that isn't yet approved,
        # then fetch all pending
        approve_all_pending()
        queue = get_pending_queue()
        log.info(f"Autosend ON — {len(queue)} email(s) pending.")
    else:
        queue = get_approved_queue()
        log.info(f"Autosend OFF — {len(queue)} approved email(s) to send.")

    if not queue:
        return

    sent_count = 0
    failed_count = 0

    for item in queue:
        recipient = item["recipient"]
        subject = item["subject"]
        body = item["body"]
        attachment_path = item["attachment_path"]
        email_type = item["email_type"]
        queue_id = item["id"]

        # If there's an attachment path, verify the file still exists
        if attachment_path and not os.path.exists(attachment_path):
            log.error(f"  Attachment not found, skipping: {attachment_path}")
            failed_count += 1
            continue

        try:
            log.info(f"  Sending [{email_type}] to {recipient} ...")
            send_email(
                recipient=recipient,
                subject=subject,
                body=body,
                attachment_path=attachment_path,
            )
            now_iso = datetime.now(timezone.utc).isoformat()
            mark_sent(queue_id, now_iso)

            # If this was an initial email, update the recipient's sent timestamp
            if email_type == "initial":
                mark_initial_sent(recipient, now_iso)

            log.info(f"    ✓ Sent to {recipient}")
            sent_count += 1

        except Exception as e:
            log.error(f"    ✗ Failed to send to {recipient}: {e}", exc_info=True)
            failed_count += 1
            # We do NOT mark as sent — it will be retried next cycle.
            # NOTE: if you want to add a retry limit, track attempts in the DB.

    log.info(f"Cycle complete: {sent_count} sent, {failed_count} failed.")


def main():
    cfg = load_config()
    interval = cfg["sending"]["interval_seconds"]

    os.makedirs("data", exist_ok=True)
    init_db()

    log.info(f"Sender daemon started. Checking queue every {interval} seconds.")

    while True:
        try:
            send_queued()
        except Exception as e:
            log.error(f"Unexpected error in send_queued: {e}", exc_info=True)
        log.info(f"Sleeping {interval}s ...")
        time.sleep(interval)


if __name__ == "__main__":
    main()
