#!/usr/bin/env python3
"""
app.py — Flask admin dashboard for the data-collection automation system.

Routes:
  GET  /                    — Overview summary statistics
  GET  /recipients          — Per-recipient status table
  GET  /submissions         — Full submission log
  GET  /queue               — Outgoing email queue
  POST /queue/<id>/approve  — Approve a single queued email
  POST /queue/approve-all   — Approve all pending queued emails
  POST /queue/<id>/send-now — Approve + immediately trigger send for one email
  POST /autosend/on         — Enable autosend
  POST /autosend/off        — Disable autosend
  GET  /submission/<id>     — Detail view for a single submission

Run with:
    python app.py
    # or for production-like use:
    gunicorn -w 1 app:app -b 0.0.0.0:5000
"""

import os
import sys
import threading
from datetime import datetime, timezone

from flask import Flask, render_template_string, redirect, url_for, abort, request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config_loader import load_config, set_autosend, is_autosend
from db import (
    init_db, get_all_recipients, get_all_submissions,
    get_pending_queue, approve_email, approve_all_pending,
    mark_sent, get_queue_item, get_submissions_for,
)
from mailer import send_email
from auth import require_auth

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Jinja2 template — single-file approach (no templates/ directory needed)
# All HTML lives in this string constant.
# ---------------------------------------------------------------------------

BASE_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Data Collection Admin</title>
  <style>
    /* ---- Reset & base ---- */
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Segoe UI', sans-serif; background: #f4f6f9; color: #333; }
    a { color: #2563eb; text-decoration: none; }
    a:hover { text-decoration: underline; }

    /* ---- Layout ---- */
    .sidebar {
      position: fixed; top: 0; left: 0; width: 200px; height: 100%;
      background: #1e293b; color: #cbd5e1; padding: 24px 0;
      display: flex; flex-direction: column;
    }
    .sidebar h1 { font-size: 14px; font-weight: 700; padding: 0 20px 20px;
      color: #f1f5f9; border-bottom: 1px solid #334155; }
    .sidebar nav a {
      display: block; padding: 10px 20px; color: #94a3b8; font-size: 14px;
      transition: background .15s;
    }
    .sidebar nav a:hover, .sidebar nav a.active {
      background: #2d3f55; color: #f1f5f9; text-decoration: none;
    }
    .sidebar .autosend-box {
      margin-top: auto; padding: 16px 20px; border-top: 1px solid #334155;
      font-size: 13px;
    }
    .main { margin-left: 200px; padding: 32px; }

    /* ---- Cards ---- */
    .card { background: #fff; border-radius: 8px; padding: 20px 24px;
      box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 20px; }
    .card h2 { font-size: 16px; font-weight: 600; margin-bottom: 12px; color: #1e293b; }

    /* ---- Stats grid ---- */
    .stats { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: 16px; }
    .stat { background: #fff; border-radius: 8px; padding: 16px;
      box-shadow: 0 1px 3px rgba(0,0,0,.08); text-align: center; }
    .stat .num { font-size: 32px; font-weight: 700; color: #1e293b; }
    .stat .lbl { font-size: 12px; color: #64748b; margin-top: 4px; }

    /* ---- Tables ---- */
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th { background: #f8fafc; text-align: left; padding: 8px 12px;
      font-weight: 600; color: #475569; border-bottom: 2px solid #e2e8f0; }
    td { padding: 8px 12px; border-bottom: 1px solid #f1f5f9; vertical-align: top; }
    tr:hover td { background: #f8fafc; }

    /* ---- Badges ---- */
    .badge {
      display: inline-block; padding: 2px 8px; border-radius: 9999px;
      font-size: 11px; font-weight: 600; text-transform: uppercase;
    }
    .badge-success { background: #dcfce7; color: #166534; }
    .badge-failure { background: #fee2e2; color: #991b1b; }
    .badge-pending { background: #fef9c3; color: #854d0e; }
    .badge-sent    { background: #dbeafe; color: #1e40af; }
    .badge-queued  { background: #f3e8ff; color: #6b21a8; }

    /* ---- Buttons ---- */
    .btn {
      display: inline-block; padding: 5px 12px; border-radius: 4px;
      font-size: 12px; font-weight: 600; cursor: pointer;
      border: none; text-align: center;
    }
    .btn-primary  { background: #2563eb; color: #fff; }
    .btn-primary:hover { background: #1d4ed8; }
    .btn-success  { background: #16a34a; color: #fff; }
    .btn-success:hover { background: #15803d; }
    .btn-danger   { background: #dc2626; color: #fff; }
    .btn-danger:hover { background: #b91c1c; }
    .btn-sm { padding: 3px 8px; font-size: 11px; }

    /* ---- Pre (validator output) ---- */
    pre { background: #1e293b; color: #e2e8f0; padding: 16px; border-radius: 6px;
      font-size: 12px; overflow-x: auto; white-space: pre-wrap; word-break: break-word; }

    /* ---- Page header ---- */
    .page-header { margin-bottom: 24px; }
    .page-header h1 { font-size: 22px; font-weight: 700; color: #1e293b; }
    .page-header p { font-size: 14px; color: #64748b; margin-top: 4px; }

    /* ---- Alert ---- */
    .alert { padding: 12px 16px; border-radius: 6px; margin-bottom: 16px;
      font-size: 13px; }
    .alert-info    { background: #eff6ff; color: #1e40af; border-left: 4px solid #3b82f6; }
    .alert-success { background: #f0fdf4; color: #166534; border-left: 4px solid #22c55e; }
    .alert-warning { background: #fffbeb; color: #92400e; border-left: 4px solid #f59e0b; }
  </style>
</head>
<body>

<div class="sidebar">
  <h1>Data Collection</h1>
  <nav>
    <a href="{{ url_for('index') }}"       class="{{ 'active' if active=='overview'    else '' }}">Overview</a>
    <a href="{{ url_for('recipients') }}"  class="{{ 'active' if active=='recipients'  else '' }}">Recipients</a>
    <a href="{{ url_for('submissions') }}" class="{{ 'active' if active=='submissions' else '' }}">Submissions</a>
    <a href="{{ url_for('queue') }}"       class="{{ 'active' if active=='queue'       else '' }}">
      Email Queue {% if queue_count %}<span style="background:#ef4444;color:#fff;border-radius:9999px;padding:1px 6px;font-size:11px;">{{ queue_count }}</span>{% endif %}
    </a>
  </nav>
  <div class="autosend-box">
    <div style="margin-bottom:8px; color:#94a3b8;">
      Autosend:
      <strong style="color:{{ '#4ade80' if autosend else '#f87171' }}">
        {{ 'ON' if autosend else 'OFF' }}
      </strong>
    </div>
    {% if autosend %}
      <form method="post" action="{{ url_for('autosend_off') }}">
        <button class="btn btn-danger btn-sm" style="width:100%">Turn OFF</button>
      </form>
    {% else %}
      <form method="post" action="{{ url_for('autosend_on') }}">
        <button class="btn btn-success btn-sm" style="width:100%">Turn ON</button>
      </form>
    {% endif %}
  </div>
</div>

<div class="main">
  {% block content %}{% endblock %}
</div>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Individual page templates (extend BASE_TEMPLATE via Jinja2 include trick)
# We render these as full strings using render_template_string with extends.
# ---------------------------------------------------------------------------

OVERVIEW_TEMPLATE = BASE_TEMPLATE.replace(
    "{% block content %}{% endblock %}", """
<div class="page-header">
  <h1>Overview</h1>
  <p>Activity summary as of {{ now }}</p>
</div>

<div class="stats">
  <div class="stat"><div class="num">{{ stats.total }}</div><div class="lbl">Total Recipients</div></div>
  <div class="stat"><div class="num">{{ stats.initial_sent }}</div><div class="lbl">Initial Email Sent</div></div>
  <div class="stat"><div class="num">{{ stats.responded }}</div><div class="lbl">Responded</div></div>
  <div class="stat"><div class="num">{{ stats.no_response }}</div><div class="lbl">No Response Yet</div></div>
  <div class="stat"><div class="num" style="color:#16a34a">{{ stats.passed }}</div><div class="lbl">Validation Passed</div></div>
  <div class="stat"><div class="num" style="color:#dc2626">{{ stats.failed }}</div><div class="lbl">Validation Failed</div></div>
  <div class="stat"><div class="num">{{ stats.pending_queue }}</div><div class="lbl">Queued (unsent)</div></div>
</div>

{% if stats.initial_sent < stats.total %}
<div class="alert alert-warning" style="margin-top:20px">
  ⚠ {{ stats.total - stats.initial_sent }} recipient(s) have not yet been sent the initial email.
  Check the <a href="{{ url_for('queue') }}">Email Queue</a>.
</div>
{% endif %}
""")

RECIPIENTS_TEMPLATE = BASE_TEMPLATE.replace(
    "{% block content %}{% endblock %}", """
<div class="page-header">
  <h1>Recipients</h1>
  <p>{{ recipients|length }} total recipients.</p>
</div>
<div class="card">
  <table>
    <thead>
      <tr>
        <th>Email / Name</th>
        <th>Initial Email Sent</th>
        <th>Submissions</th>
        <th>Last Submission</th>
        <th>Latest Status</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody>
    {% for r in recipients %}
      <tr>
        <td>
          {{ r['email'] }}
          {% if r['display_name'] %}
            <br><span style="font-size:11px;color:#64748b">{{ r['display_name'] }}</span>
          {% endif %}
        </td>
        <td>
          {% if r['initial_sent_at'] %}
            <span title="{{ r['initial_sent_at'] }}">✓ {{ r['initial_sent_at'][:10] }}</span>
          {% elif r['initial_queued_at'] %}
            <span class="badge badge-queued">Queued</span>
          {% else %}
            <span class="badge badge-pending">Not queued</span>
          {% endif %}
        </td>
        <td>{{ r['submission_count'] }}</td>
        <td>{{ r['latest_submission_at'][:16].replace('T',' ') if r['latest_submission_at'] else '—' }}</td>
        <td>
          {% if r['latest_status'] == 'success' %}
            <span class="badge badge-success">Passed</span>
          {% elif r['latest_status'] == 'failure' %}
            <span class="badge badge-failure">Failed</span>
          {% else %}
            <span class="badge badge-pending">Pending</span>
          {% endif %}
        </td>
        <td>
          <a href="{{ url_for('recipient_detail', email=r['email']) }}"
             class="btn btn-primary btn-sm">History</a>
        </td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
</div>
""")

SUBMISSIONS_TEMPLATE = BASE_TEMPLATE.replace(
    "{% block content %}{% endblock %}", """
<div class="page-header">
  <h1>All Submissions</h1>
  <p>{{ submissions|length }} total submissions processed.</p>
</div>
<div class="card">
  <table>
    <thead>
      <tr>
        <th>Received</th>
        <th>Sender</th>
        <th>Original Filename</th>
        <th>Status</th>
        <th>Detail</th>
      </tr>
    </thead>
    <tbody>
    {% for s in submissions %}
      <tr>
        <td>{{ s['received_at'][:16].replace('T',' ') }}</td>
        <td>{{ s['sender_email'] }}</td>
        <td>{{ s['original_filename'] }}</td>
        <td>
          {% if s['validation_status'] == 'success' %}
            <span class="badge badge-success">Passed</span>
          {% else %}
            <span class="badge badge-failure">Failed</span>
          {% endif %}
        </td>
        <td><a href="{{ url_for('submission_detail', sub_id=s['id']) }}"
               class="btn btn-primary btn-sm">View</a></td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
</div>
""")

QUEUE_TEMPLATE = BASE_TEMPLATE.replace(
    "{% block content %}{% endblock %}", """
<div class="page-header">
  <h1>Email Queue</h1>
  <p>{{ pending|length }} unsent email(s) in queue.</p>
</div>

{% if pending %}
<div class="card">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
    <h2>Pending Emails</h2>
    <form method="post" action="{{ url_for('approve_all') }}">
      <button class="btn btn-success">Approve All ({{ pending|length }})</button>
    </form>
  </div>
  <table>
    <thead>
      <tr>
        <th>Created</th>
        <th>To</th>
        <th>Type</th>
        <th>Subject</th>
        <th>Approved</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody>
    {% for item in pending %}
      <tr>
        <td>{{ item['created_at'][:16].replace('T',' ') }}</td>
        <td>{{ item['recipient'] }}</td>
        <td>
          {% if item['email_type'] == 'initial' %}
            <span class="badge badge-queued">Initial</span>
          {% elif item['email_type'] == 'success_ack' %}
            <span class="badge badge-success">Ack ✓</span>
          {% else %}
            <span class="badge badge-failure">Error ✗</span>
          {% endif %}
        </td>
        <td>{{ item['subject'] }}</td>
        <td>
          {% if item['approved'] %}
            <span class="badge badge-success">Yes</span>
          {% else %}
            <span class="badge badge-pending">No</span>
          {% endif %}
        </td>
        <td style="white-space:nowrap">
          {% if not item['approved'] %}
          <form method="post" action="{{ url_for('approve_one', q_id=item['id']) }}"
                style="display:inline">
            <button class="btn btn-primary btn-sm">Approve</button>
          </form>
          {% endif %}
          <form method="post" action="{{ url_for('send_now', q_id=item['id']) }}"
                style="display:inline">
            <button class="btn btn-success btn-sm">Send Now</button>
          </form>
          <a href="{{ url_for('queue_detail', q_id=item['id']) }}"
             class="btn btn-sm" style="background:#e2e8f0;color:#334155">Preview</a>
        </td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
</div>
{% else %}
<div class="alert alert-success">✓ No emails pending in queue.</div>
{% endif %}

{% if sent_recent %}
<div class="card">
  <h2>Recently Sent (last 20)</h2>
  <table>
    <thead>
      <tr><th>Sent At</th><th>To</th><th>Type</th><th>Subject</th></tr>
    </thead>
    <tbody>
    {% for item in sent_recent %}
      <tr>
        <td>{{ item['sent_at'][:16].replace('T',' ') }}</td>
        <td>{{ item['recipient'] }}</td>
        <td>{{ item['email_type'] }}</td>
        <td>{{ item['subject'] }}</td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
</div>
{% endif %}
""")

QUEUE_DETAIL_TEMPLATE = BASE_TEMPLATE.replace(
    "{% block content %}{% endblock %}", """
<div class="page-header">
  <h1>Email Preview</h1>
  <a href="{{ url_for('queue') }}" style="font-size:13px">← Back to Queue</a>
</div>
<div class="card">
  <table style="margin-bottom:16px">
    <tr><th style="width:120px">To</th><td>{{ item['recipient'] }}</td></tr>
    <tr><th>Subject</th><td>{{ item['subject'] }}</td></tr>
    <tr><th>Type</th><td>{{ item['email_type'] }}</td></tr>
    <tr><th>Created</th><td>{{ item['created_at'] }}</td></tr>
    <tr><th>Approved</th><td>{{ 'Yes' if item['approved'] else 'No' }}</td></tr>
    {% if item['attachment_path'] %}
    <tr><th>Attachment</th><td>{{ item['attachment_path'] }}</td></tr>
    {% endif %}
  </table>
  <h2 style="margin-bottom:8px">Body</h2>
  <pre>{{ item['body'] }}</pre>
  <div style="margin-top:16px; display:flex; gap:8px">
    {% if not item['approved'] and not item['sent_at'] %}
    <form method="post" action="{{ url_for('approve_one', q_id=item['id']) }}">
      <button class="btn btn-primary">Approve</button>
    </form>
    {% endif %}
    {% if not item['sent_at'] %}
    <form method="post" action="{{ url_for('send_now', q_id=item['id']) }}">
      <button class="btn btn-success">Send Now</button>
    </form>
    {% endif %}
  </div>
</div>
""")

SUBMISSION_DETAIL_TEMPLATE = BASE_TEMPLATE.replace(
    "{% block content %}{% endblock %}", """
<div class="page-header">
  <h1>Submission Detail</h1>
  <a href="{{ url_for('submissions') }}" style="font-size:13px">← Back to Submissions</a>
</div>
<div class="card">
  <table style="margin-bottom:16px">
    <tr><th style="width:160px">Sender</th><td>{{ sub['sender_email'] }}</td></tr>
    <tr><th>Received At</th><td>{{ sub['received_at'] }}</td></tr>
    <tr><th>Original Filename</th><td>{{ sub['original_filename'] }}</td></tr>
    <tr><th>Stored Path</th><td>{{ sub['stored_path'] }}</td></tr>
    <tr><th>Status</th><td>
      {% if sub['validation_status'] == 'success' %}
        <span class="badge badge-success">Passed</span>
      {% else %}
        <span class="badge badge-failure">Failed</span>
      {% endif %}
    </td></tr>
  </table>

  {% if sub['validator_stdout'] %}
  <h2 style="margin-bottom:8px">Validator stdout (summary)</h2>
  <pre>{{ sub['validator_stdout'] }}</pre>
  {% endif %}

  {% if sub['validator_stderr'] %}
  <h2 style="margin-top:16px; margin-bottom:8px">Validator stderr (errors)</h2>
  <pre>{{ sub['validator_stderr'] }}</pre>
  {% endif %}
</div>
""")

RECIPIENT_DETAIL_TEMPLATE = BASE_TEMPLATE.replace(
    "{% block content %}{% endblock %}", """
<div class="page-header">
  <h1>{{ email }}</h1>
  <a href="{{ url_for('recipients') }}" style="font-size:13px">← Back to Recipients</a>
</div>
<div class="card">
  <h2>Submission History ({{ subs|length }})</h2>
  {% if subs %}
  <table>
    <thead>
      <tr><th>Received</th><th>Filename</th><th>Status</th><th>Detail</th></tr>
    </thead>
    <tbody>
    {% for s in subs %}
      <tr>
        <td>{{ s['received_at'][:16].replace('T',' ') }}</td>
        <td>{{ s['original_filename'] }}</td>
        <td>
          {% if s['validation_status'] == 'success' %}
            <span class="badge badge-success">Passed</span>
          {% else %}
            <span class="badge badge-failure">Failed</span>
          {% endif %}
        </td>
        <td><a href="{{ url_for('submission_detail', sub_id=s['id']) }}"
               class="btn btn-primary btn-sm">View</a></td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
  {% else %}
  <p style="color:#64748b; font-size:13px">No submissions yet.</p>
  {% endif %}
</div>
""")


# ---------------------------------------------------------------------------
# Template rendering helper
# ---------------------------------------------------------------------------

def render(template_str: str, **kwargs):
    """Render a template string with common context variables injected."""
    from db import get_pending_queue as _gpq
    pending = _gpq()
    return render_template_string(
        template_str,
        autosend=is_autosend(),
        queue_count=len(pending),
        now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        **kwargs
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
@require_auth
def index():
    """Overview page: aggregate statistics."""
    with __import__("db").get_conn() as conn:
        recs = get_all_recipients(conn)
        subs = get_all_submissions()
        pending = get_pending_queue()

    total = len(recs)
    initial_sent  = sum(1 for r in recs if r["initial_sent_at"])
    responded     = sum(1 for r in recs if r["submission_count"] > 0)
    no_response   = total - responded
    passed        = sum(1 for r in recs if r["latest_status"] == "success")
    failed        = sum(1 for r in recs if r["latest_status"] == "failure")

    stats = dict(
        total=total, initial_sent=initial_sent, responded=responded,
        no_response=no_response, passed=passed, failed=failed,
        pending_queue=len(pending),
    )
    return render(OVERVIEW_TEMPLATE, active="overview", stats=stats)


@app.route("/recipients")
@require_auth
def recipients():
    """Per-recipient status table."""
    recs = get_all_recipients()
    return render(RECIPIENTS_TEMPLATE, active="recipients", recipients=recs)


@app.route("/recipients/<path:email>")
@require_auth
def recipient_detail(email):
    """Submission history for one recipient."""
    subs = get_submissions_for(email)
    return render(RECIPIENT_DETAIL_TEMPLATE, active="recipients",
                  email=email, subs=subs)


@app.route("/submissions")
@require_auth
def submissions():
    """Full submission log."""
    subs = get_all_submissions()
    return render(SUBMISSIONS_TEMPLATE, active="submissions", submissions=subs)


@app.route("/submission/<int:sub_id>")
@require_auth
def submission_detail(sub_id):
    """Detail for a single submission."""
    with __import__("db").get_conn() as conn:
        sub = conn.execute(
            "SELECT * FROM submissions WHERE id = ?", (sub_id,)
        ).fetchone()
    if sub is None:
        abort(404)
    return render(SUBMISSION_DETAIL_TEMPLATE, active="submissions", sub=sub)


@app.route("/queue")
@require_auth
def queue():
    """Email queue management page."""
    with __import__("db").get_conn() as conn:
        pending = conn.execute(
            "SELECT * FROM email_queue WHERE sent_at IS NULL ORDER BY created_at ASC"
        ).fetchall()
        sent_recent = conn.execute(
            "SELECT * FROM email_queue WHERE sent_at IS NOT NULL ORDER BY sent_at DESC LIMIT 20"
        ).fetchall()
    return render(QUEUE_TEMPLATE, active="queue",
                  pending=pending, sent_recent=sent_recent)


@app.route("/queue/<int:q_id>")
@require_auth
def queue_detail(q_id):
    """Preview a single queued email."""
    item = get_queue_item(q_id)
    if item is None:
        abort(404)
    return render(QUEUE_DETAIL_TEMPLATE, active="queue", item=item)


@app.route("/queue/<int:q_id>/approve", methods=["POST"])
@require_auth
def approve_one(q_id):
    """Approve a single queued email."""
    approve_email(q_id)
    return redirect(url_for("queue"))


@app.route("/queue/approve-all", methods=["POST"])
@require_auth
def approve_all():
    """Approve all currently pending emails."""
    approve_all_pending()
    return redirect(url_for("queue"))


@app.route("/queue/<int:q_id>/send-now", methods=["POST"])
@require_auth
def send_now(q_id):
    """
    Immediately approve and send a single queued email, bypassing the
    sender daemon's normal cycle.  Runs the send in a thread so the
    HTTP request does not time out on slow SMTP connections.
    """
    item = get_queue_item(q_id)
    if item is None:
        abort(404)

    def _send():
        try:
            send_email(
                recipient=item["recipient"],
                subject=item["subject"],
                body=item["body"],
                attachment_path=item["attachment_path"],
            )
            now_iso = datetime.now(timezone.utc).isoformat()
            mark_sent(q_id, now_iso)
            if item["email_type"] == "initial":
                mark_initial_sent(item["recipient"], now_iso)
        except Exception as e:
            app.logger.error(f"send_now failed for queue {q_id}: {e}", exc_info=True)

    threading.Thread(target=_send, daemon=True).start()
    return redirect(url_for("queue"))


@app.route("/autosend/on", methods=["POST"])
@require_auth
def autosend_on():
    set_autosend(True)
    return redirect(request.referrer or url_for("index"))


@app.route("/autosend/off", methods=["POST"])
@require_auth
def autosend_off():
    set_autosend(False)
    return redirect(request.referrer or url_for("index"))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    init_db()
    # Debug mode intentionally off — this runs on a LAN, but keep it safe.
    app.run(host="0.0.0.0", port=5000, debug=False)
