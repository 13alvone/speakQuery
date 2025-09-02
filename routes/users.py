from flask import Blueprint, request, jsonify
from flask import current_app as app
from werkzeug.security import generate_password_hash
from utils.auth import admin_required
import sqlite3
import uuid
import hashlib
import logging


def validate_password_strength(password):
    from app import validate_password_strength as _validate
    return _validate(password)

users_bp = Blueprint('users_bp', __name__)


@users_bp.route('/users', methods=['GET'])
@admin_required
def list_users():
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cur = conn.cursor()
            cur.execute('SELECT username, role, last_login FROM users')
            rows = cur.fetchall()
        users = [
            {'username': r[0], 'role': r[1], 'last_login': r[2]}
            for r in rows
        ]
        return jsonify({'status': 'success', 'users': users})
    except Exception as exc:
        logging.error(f"[x] Error fetching users: {exc}")
        return jsonify({'status': 'error', 'message': 'Failed to fetch users'}), 500


@users_bp.route('/users', methods=['POST'])
@admin_required
def create_user():
    data = request.get_json(silent=True) or request.form
    username = data.get('username') if data else None
    password = data.get('password') if data else None
    role = data.get('role', 'standard_user') if data else 'standard_user'

    if not username or not password:
        return jsonify({'status': 'error', 'message': 'Missing credentials'}), 400
    if not validate_password_strength(password):
        return jsonify({'status': 'error', 'message': 'Weak password'}), 400

    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cur = conn.cursor()
            cur.execute('SELECT 1 FROM users WHERE username=?', (username,))
            if cur.fetchone():
                return jsonify({'status': 'error', 'message': 'Username already exists'}), 400
            if role == 'admin':
                cur.execute("SELECT COUNT(*) FROM users WHERE role='admin'")
                if cur.fetchone()[0] >= 10:
                    return jsonify({'status': 'error', 'message': 'Admin user limit reached'}), 400
            pw_hash = generate_password_hash(password)
            token = uuid.uuid4().hex
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            cur.execute(
                'INSERT INTO users (username, password_hash, role, api_token, force_password_change) '
                'VALUES (?, ?, ?, ?, 0)',
                (username, pw_hash, role, token_hash)
            )
            conn.commit()
        return jsonify({'status': 'success', 'api_token': token}), 201
    except Exception as exc:
        logging.error(f"[x] Error creating user {username}: {exc}")
        return jsonify({'status': 'error', 'message': 'Failed to create user'}), 500
