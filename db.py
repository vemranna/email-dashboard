#!/usr/bin/env python3
"""
db.py — Database layer for the data-collection automation system.

Uses SQLite via Python's built-in sqlite3 module. All schema creation,
insertion, and query helpers live here so every other module imports
from one place.

Tables:
  recipients    — everyone who should receive / has received the initial email
  submissions   — every incoming xlsx that was processed
  email_queue   — outgoing emails awaiting approval or sending
  seen_messages — POP3 Message-IDs already processed (deduplication)
"""

import sqlite3
import os
from contextlib import contextmanager


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def get_db_path():
    """Return the database path from config, defaulting to data/db.sqlite."""
    # We import config lazily so this module can be imported before config is
    # fully loaded in some test/init scenarios.
    from config_loader import load_config
    cfg = load_config()
    return cfg["paths"]["db_file"]


@contextmanager
def get_conn():
    """
    Context manager that yields an open sqlite3 connection with row_factory
    set to sqlite3.Row (so columns are accessible by name).
    Commits on clean exit, rolls back on exception.
    """
    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # allow concurrent readers
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

SCHEMA = """
-- One row per intended recipient (loaded from recipients.txt at boot).
CREATE TABLE IF NOT EXISTS recipients (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    email               TEXT    NOT NULL UNIQUE,
    -- Human-readable label from recipients.txt (everything after the email)
    display_name        TEXT,
    -- When the initial email was enqueued (NULL = not yet enqueued)
    initial_queued_at   TEXT,
    -- When the initial email was actually sent (NULL = not yet sent)
    initial_sent_at     TEXT,
    -- Convenience denorm: latest submission status for dashboard queries
    latest_status       TEXT    DEFAULT 'pending',   -- pending|success|failure
    latest_submission_at TEXT,
    submission_count    INTEGER DEFAULT 0
);

-- One row per processed incoming submission.
CREATE TABLE IF NOT EXISTS submissions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_email    TEXT    NOT NULL,
    received_at     TEXT    NOT NULL,               -- ISO-8601 UTC
    original_filename TEXT,
    stored_path     TEXT,                           -- path to timestamped file
    validation_status TEXT NOT NULL,                -- success|failure
    validator_stdout  TEXT,                         -- summary (success) output
    validator_stderr  TEXT,                         -- error output (failure)
    -- FK back to recipients (the sender should be a known recipient)
    FOREIGN KEY (sender_email) REFERENCES recipients(email)
);

-- One row per outgoing email (initial blasts + responses).
CREATE TABLE IF NOT EXISTS email_queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient       TEXT    NOT NULL,
    subject         TEXT    NOT NULL,
    body            TEXT    NOT NULL,
    -- Path to an attachment file, or NULL if no attachment.
    attachment_path TEXT,
    -- email_type: initial|success_ack|failure_report
    email_type      TEXT    NOT NULL DEFAULT 'initial',
    -- Foreign key to submissions table when this is a response email.
    submission_id   INTEGER,
    created_at      TEXT    NOT NULL,
    -- approved=1 means admin has approved this email for sending.
    -- When autosend is on, the sender treats all queued as approved.
    approved        INTEGER NOT NULL DEFAULT 0,
    sent_at         TEXT,                           -- NULL until actually sent
    FOREIGN KEY (submission_id) REFERENCES submissions(id)
);

-- POP3 deduplication: store Message-IDs we have already downloaded.
CREATE TABLE IF NOT EXISTS seen_messages (
    message_id  TEXT PRIMARY KEY,
    seen_at     TEXT NOT NULL
);
"""


def init_db():
    """Create all tables if they do not already exist, and run migrations."""
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        _migrate(conn)
    print("[db] Database initialised.")


def _migrate(conn):
    """
    Apply incremental schema migrations that are safe to run on an
    already-populated database.  Each ALTER is guarded so re-running
    init_db() on an up-to-date schema is a no-op.
    """
    existing_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(recipients)")
    }
    if "display_name" not in existing_cols:
        conn.execute("ALTER TABLE recipients ADD COLUMN display_name TEXT")
        print("[db] Migration applied: added recipients.display_name")


# ---------------------------------------------------------------------------
# Recipient helpers
# ---------------------------------------------------------------------------

def upsert_recipient(email: str, display_name: str = None):
    """
    Insert a recipient if not already present.
    If the row exists but display_name has changed, update it.
    Does NOT overwrite other existing fields (sent timestamps etc.).
    """
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO recipients (email, display_name) VALUES (?, ?)",
            (email.strip().lower(), display_name)
        )
        # Update display_name even if the row already existed, so re-running
        # send_initial.py after editing recipients.txt refreshes the labels.
        if display_name is not None:
            conn.execute(
                "UPDATE recipients SET display_name = ? WHERE email = ?",
                (display_name, email.strip().lower())
            )


def get_all_recipients(conn=None):
    """Return all recipient rows as a list of sqlite3.Row objects."""
    def _query(c):
        return c.execute("SELECT * FROM recipients ORDER BY email").fetchall()
    if conn:
        return _query(conn)
    with get_conn() as c:
        return _query(c)


def get_recipient(email: str, conn=None):
    """Return the recipient row for a given email, or None."""
    def _query(c):
        return c.execute(
            "SELECT * FROM recipients WHERE email = ?",
            (email.strip().lower(),)
        ).fetchone()
    if conn:
        return _query(conn)
    with get_conn() as c:
        return _query(c)


def mark_initial_queued(email: str, queued_at: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE recipients SET initial_queued_at = ? WHERE email = ?",
            (queued_at, email.strip().lower())
        )


def mark_initial_sent(email: str, sent_at: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE recipients SET initial_sent_at = ? WHERE email = ?",
            (sent_at, email.strip().lower())
        )


def update_recipient_status(email: str, status: str, submission_at: str):
    """Update the denormalised latest_status fields on the recipient row."""
    with get_conn() as conn:
        conn.execute(
            """UPDATE recipients
               SET latest_status = ?,
                   latest_submission_at = ?,
                   submission_count = submission_count + 1
               WHERE email = ?""",
            (status, submission_at, email.strip().lower())
        )


# ---------------------------------------------------------------------------
# Submission helpers
# ---------------------------------------------------------------------------

def insert_submission(sender_email, received_at, original_filename,
                       stored_path, status, stdout, stderr):
    """Insert a new submission record and return its row id."""
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO submissions
               (sender_email, received_at, original_filename, stored_path,
                validation_status, validator_stdout, validator_stderr)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (sender_email.strip().lower(), received_at, original_filename,
             stored_path, status, stdout, stderr)
        )
        return cur.lastrowid


def get_submissions_for(email: str):
    """Return all submissions from a given sender, newest first."""
    with get_conn() as conn:
        return conn.execute(
            """SELECT * FROM submissions
               WHERE sender_email = ?
               ORDER BY received_at DESC""",
            (email.strip().lower(),)
        ).fetchall()


def get_all_submissions():
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM submissions ORDER BY received_at DESC"
        ).fetchall()


# ---------------------------------------------------------------------------
# Email queue helpers
# ---------------------------------------------------------------------------

def enqueue_email(recipient, subject, body, attachment_path=None,
                  email_type="initial", submission_id=None, created_at=None):
    """Add an email to the outgoing queue. Returns the new row id."""
    from datetime import datetime, timezone
    if created_at is None:
        created_at = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO email_queue
               (recipient, subject, body, attachment_path, email_type,
                submission_id, created_at, approved)
               VALUES (?, ?, ?, ?, ?, ?, ?, 0)""",
            (recipient, subject, body, attachment_path,
             email_type, submission_id, created_at)
        )
        return cur.lastrowid


def get_pending_queue():
    """Return all unsent emails in the queue, ordered by creation time."""
    with get_conn() as conn:
        return conn.execute(
            """SELECT * FROM email_queue
               WHERE sent_at IS NULL
               ORDER BY created_at ASC"""
        ).fetchall()


def get_approved_queue():
    """Return unsent emails that have been approved (or all if autosend)."""
    with get_conn() as conn:
        return conn.execute(
            """SELECT * FROM email_queue
               WHERE sent_at IS NULL AND approved = 1
               ORDER BY created_at ASC"""
        ).fetchall()


def approve_email(queue_id: int):
    with get_conn() as conn:
        conn.execute(
            "UPDATE email_queue SET approved = 1 WHERE id = ?",
            (queue_id,)
        )


def approve_all_pending():
    with get_conn() as conn:
        conn.execute(
            "UPDATE email_queue SET approved = 1 WHERE sent_at IS NULL"
        )


def mark_sent(queue_id: int, sent_at: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE email_queue SET sent_at = ? WHERE id = ?",
            (sent_at, queue_id)
        )


def get_queue_item(queue_id: int):
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM email_queue WHERE id = ?", (queue_id,)
        ).fetchone()


# ---------------------------------------------------------------------------
# Seen-message deduplication helpers
# ---------------------------------------------------------------------------

def is_seen(message_id: str) -> bool:
    """Return True if this POP3 Message-ID has already been processed."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM seen_messages WHERE message_id = ?",
            (message_id,)
        ).fetchone()
        return row is not None


def mark_seen(message_id: str, seen_at: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO seen_messages (message_id, seen_at) VALUES (?, ?)",
            (message_id, seen_at)
        )
