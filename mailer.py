#!/usr/bin/env python3
"""
mailer.py — SMTP sending utility.

Provides a single send_email() function used by sender.py.
Handles both STARTTLS (port 587) and implicit TLS / SMTPS (port 465),
as well as plain SMTP (port 25, not recommended).
"""

import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from config_loader import load_config


def send_email(recipient: str, subject: str, body: str,
               attachment_path: str = None) -> None:
    """
    Send a single email via SMTP.

    Args:
        recipient:       Destination email address.
        subject:         Email subject line.
        body:            Plain-text email body.
        attachment_path: Optional path to a file to attach.

    Raises:
        smtplib.SMTPException on any SMTP-level error.
        FileNotFoundError if attachment_path is given but does not exist.
    """
    cfg = load_config()
    email_cfg = cfg["email"]
    smtp_cfg = email_cfg["smtp"]

    from_addr = email_cfg["address"]
    username = email_cfg["username"]
    password = email_cfg["password"]

    # -----------------------------------------------------------------
    # Build the MIME message
    # -----------------------------------------------------------------
    if attachment_path:
        msg = MIMEMultipart()
        msg.attach(MIMEText(body, "plain", "utf-8"))

        # Attach the file
        filename = os.path.basename(attachment_path)
        with open(attachment_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{filename}"'
        )
        msg.attach(part)
    else:
        # Simple plain-text message
        msg = MIMEText(body, "plain", "utf-8")

    msg["From"] = from_addr
    msg["To"] = recipient
    msg["Subject"] = subject

    # -----------------------------------------------------------------
    # Connect and send
    # -----------------------------------------------------------------
    host = smtp_cfg["host"]
    port = smtp_cfg["port"]
    use_ssl = smtp_cfg.get("use_ssl", False)    # True = implicit TLS (port 465)
    use_tls = smtp_cfg.get("use_tls", True)     # True = STARTTLS (port 587)

    if use_ssl:
        # Implicit TLS — wrap the connection from the start
        with smtplib.SMTP_SSL(host, port) as server:
            server.login(username, password)
            server.sendmail(from_addr, [recipient], msg.as_bytes())
    else:
        with smtplib.SMTP(host, port) as server:
            server.ehlo()
            if use_tls:
                server.starttls()
                server.ehlo()
            server.login(username, password)
            server.sendmail(from_addr, [recipient], msg.as_bytes())
