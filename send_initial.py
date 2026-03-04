#!/usr/bin/env python3
"""
send_initial.py — Bootstrap script to enqueue the initial data-collection
                  emails to all recipients listed in recipients.txt.

Usage:
    python send_initial.py

This script:
  1. Reads recipients.txt and upserts every address into the DB.
  2. For each recipient who has NOT yet had an initial email queued,
     it enqueues an email (with the Excel template attached) in email_queue.
  3. Emails are NOT sent immediately — they sit in the queue until the
     sender daemon (sender.py) picks them up (respecting autosend / approval).

Run this once.  If you add new recipients later and re-run, only the newly
added addresses will be enqueued (existing rows are untouched).
"""

import os
import sys
from datetime import datetime, timezone

# Make sure we can import project modules regardless of CWD
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config_loader import load_config
from db import init_db, upsert_recipient, get_recipient, enqueue_email, mark_initial_queued


def main():
    cfg = load_config()
    paths = cfg["paths"]
    templates = cfg["templates"]["initial_email"]

    recipients_file = paths["recipients_file"]
    template_file = paths["template_file"]

    # ------------------------------------------------------------------
    # Validate that required files exist before doing anything
    # ------------------------------------------------------------------
    if not os.path.exists(recipients_file):
        print(f"[ERROR] Recipients file not found: {recipients_file}")
        sys.exit(1)

    if not os.path.exists(template_file):
        print(f"[ERROR] Excel template not found: {template_file}")
        sys.exit(1)

    # Initialise DB tables (safe to call multiple times)
    init_db()

    # ------------------------------------------------------------------
    # Load recipient list
    # ------------------------------------------------------------------
    with open(recipients_file, "r") as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]

    if not lines:
        print("[ERROR] recipients.txt is empty.")
        sys.exit(1)

    # Parse each line: first whitespace-delimited token is the email address;
    # everything after it (if present) is a free-text display name.
    # Examples:
    #   sales@acme.com
    #   sales@acme.com Sales dept of Acme
    #   john@acme.com  John Smith, Engineering
    recipients_parsed = []
    for line in lines:
        parts = line.split(None, 1)          # split on first whitespace only
        email = parts[0].lower()
        display_name = parts[1].strip() if len(parts) > 1 else None
        recipients_parsed.append((email, display_name))

    print(f"[init] Loaded {len(recipients_parsed)} recipient(s) from {recipients_file}.")

    now_iso = datetime.now(timezone.utc).isoformat()
    queued_count = 0
    skipped_count = 0

    for email, display_name in recipients_parsed:
        # Upsert the recipient into the DB (with display_name)
        upsert_recipient(email, display_name)

        # Check if an initial email has already been queued for this person
        rec = get_recipient(email)
        if rec and rec["initial_queued_at"]:
            print(f"  [skip] {email} — initial email already queued at {rec['initial_queued_at']}")
            skipped_count += 1
            continue

        # Build email content from template
        subject = templates["subject"]
        body = templates["body"].replace("{recipient}", email)

        # Enqueue with the template file as attachment
        enqueue_email(
            recipient=email,
            subject=subject,
            body=body,
            attachment_path=os.path.abspath(template_file),
            email_type="initial",
            submission_id=None,
            created_at=now_iso,
        )

        # Mark that we've queued the initial email
        mark_initial_queued(email, now_iso)

        print(f"  [queued] {email}")
        queued_count += 1

    print(f"\n[init] Done. {queued_count} email(s) queued, {skipped_count} skipped.")
    print("       Run sender.py (or approve via dashboard) to send them.")


if __name__ == "__main__":
    main()
