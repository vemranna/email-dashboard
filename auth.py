#!/usr/bin/env python3
"""
auth.py — Simple HTTP Basic Authentication for the Flask dashboard.

Reads credentials from users.txt (one "username:password" line per entry).
Provides a decorator @require_auth that can be applied to any Flask route.
"""

import os
from functools import wraps
from flask import request, Response
from config_loader import load_config


def load_users() -> dict:
    """
    Parse users.txt and return a dict of {username: password}.
    Lines starting with '#' are treated as comments and ignored.
    """
    cfg = load_config()
    users_file = cfg["paths"]["users_file"]

    if not os.path.exists(users_file):
        # If no users file exists, create a default one as a reminder
        with open(users_file, "w") as f:
            f.write("# Add credentials below, one per line: username:password\n")
            f.write("admin:changeme\n")
        print(f"[auth] WARNING: Created default {users_file} with admin:changeme — change this!")

    users = {}
    with open(users_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                username, password = line.split(":", 1)
                users[username.strip()] = password.strip()
    return users


def check_auth(username: str, password: str) -> bool:
    """Return True if the username/password pair is valid."""
    users = load_users()
    return users.get(username) == password


def unauthorized_response():
    """Return a 401 response that triggers the browser's Basic Auth dialog."""
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": 'Basic realm="Data Collection Admin"'},
    )


def require_auth(f):
    """
    Flask route decorator that enforces HTTP Basic Auth.

    Usage:
        @app.route("/")
        @require_auth
        def index():
            ...
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return unauthorized_response()
        return f(*args, **kwargs)
    return decorated
