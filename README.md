# Data Collection Automation System — Setup & Operations Guide

## Overview

Three long-running processes make up the system:

| Process | File | Purpose |
|---------|------|---------|
| Poller | `poller.py` | Checks inbox via POP3, processes submissions |
| Sender | `sender.py` | Sends queued outgoing emails via SMTP |
| Dashboard | `app.py` | Flask admin web UI |

A one-shot bootstrap script (`send_initial.py`) enqueues the initial emails.

---

## Directory layout

```
project/
├── config.yaml          ← edit this first
├── recipients.txt       ← one email per line
├── template.xlsx        ← your Excel template
├── validator.py         ← your existing validator (not included)
├── users.txt            ← username:password lines for dashboard auth
├── requirements.txt
├── db.py
├── config_loader.py
├── mailer.py
├── auth.py
├── poller.py
├── sender.py
├── send_initial.py
└── app.py
data/                    ← created automatically
├── db.sqlite
├── poller.log
├── sender.log
├── inbox/               ← raw .eml files, organised by date
├── attachments/         ← validated xlsx files, organised by sender
└── queue/               ← (reserved for future attachment staging)
```

---

## Installation (Debian Bookworm, airgapped LAN)

```bash
# 1. Install Python and virtualenv (from your local Debian mirror)
sudo apt install python3 python3-venv python3-pip

# 2. Create a virtualenv
cd /path/to/project
python3 -m venv venv
source venv/bin/activate

# 3. Install Python dependencies
pip install -r requirements.txt
# If your pip mirror is internal, add: --index-url http://your-pypi-mirror/simple/

# 4. Edit config.yaml with your real credentials and paths.

# 5. Create users.txt
echo "admin:yourpassword" > users.txt
chmod 600 users.txt   # keep credentials private
```

---

## Initial run (step by step)

```bash
source venv/bin/activate

# Step 1 — Initialise DB and enqueue initial emails (run once)
python send_initial.py

# Step 2 — Start the dashboard and review the queued emails
python app.py &
# Open http://localhost:5000 in your browser.
# Go to "Email Queue", review the initial emails, approve them.

# Step 3 — Start the sender daemon (it will send approved emails)
python sender.py &

# Step 4 — Start the poller daemon (it will check inbox every N seconds)
python poller.py &
```

---

## Running as systemd services (recommended for unattended operation)

Copy the three `.service` files from the `systemd/` subdirectory
(see below) to `/etc/systemd/system/`, then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now datacollect-poller
sudo systemctl enable --now datacollect-sender
sudo systemctl enable --now datacollect-dashboard
```

Logs:
```bash
journalctl -u datacollect-poller -f
journalctl -u datacollect-sender -f
# Also: data/poller.log and data/sender.log
```

---

## Autosend

Initially `autosend: false` in config.yaml. Every outgoing email requires
manual approval in the dashboard queue before the sender daemon sends it.

Once you are confident the system is working correctly, click **"Turn ON"**
in the dashboard sidebar. From that point, the sender daemon will send all
queued emails without waiting for approval.

You can also toggle it directly in `config.yaml`:
```yaml
sending:
  autosend: true
```

---

## Validator contract

The system calls your validator as:
```
python validator.py <path-to-xlsx-file>
```
- Exit code `0` → validation passed; summary goes to **stdout**.
- Exit code non-zero → validation failed; error details go to **stderr**
  (stdout is also captured and appended if present).

---

## POP3 notes

- The poller tracks processed messages by their `Message-ID` header in
  the `seen_messages` DB table. This means it is safe to leave messages
  on the POP3 server (`delete_after_download: false`), and Thunderbird
  will continue to download them normally.
- If `delete_after_download: true`, the poller marks messages for deletion
  during each session. Thunderbird will no longer see those messages.

---

## Adding recipients later

Add the new email addresses to `recipients.txt`, then re-run:
```bash
python send_initial.py
```
Only the new addresses will be enqueued; existing rows are untouched.
