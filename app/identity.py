"""Signed anonymous shopper ownership and separate operator access."""
import os
from pathlib import Path
import secrets

from flask import abort, request, session


def local_secret(name):
    directory = Path(__file__).resolve().parents[1] / '.runtime' / 'auth'
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return path.read_text().strip()
    value = secrets.token_hex(32)
    with os.fdopen(descriptor, 'w') as handle:
        handle.write(value)
    return value


def configure_identity(app):
    app.secret_key = os.getenv('FLASK_SECRET_KEY') or local_secret('session-key')
    app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE='Lax',
                      SESSION_COOKIE_SECURE=os.getenv('COOKIE_SECURE') == '1')


def tokens_equal(supplied, expected):
    return isinstance(supplied, str) and supplied.isascii() and secrets.compare_digest(supplied, expected)


def shopper():
    if 'user_id' not in session:
        session['user_id'] = 'shopper-' + secrets.token_hex(16)
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(32)
    return session['user_id']


def authorize(user_id, payload=None):
    if user_id != shopper():
        abort(403, description='This shopper identity belongs to another browser')
    if request.method == 'POST':
        token = request.headers.get('X-CSRF-Token') or (payload or {}).get('csrf_token', '')
        if not tokens_equal(token, session['csrf_token']):
            abort(403, description='Invalid CSRF token')


def operator_token():
    return os.getenv('OPERATOR_TOKEN') or local_secret('operator-token')


def operator():
    return session.get('operator') is True
