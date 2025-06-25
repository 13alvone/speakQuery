from functools import wraps
from flask import jsonify
from flask_login import login_required as flask_login_required, current_user

# Re-export Flask-Login's decorator for consistency
login_required = flask_login_required


def admin_required(view_func):
    """Ensure the current user is an authenticated admin."""

    @login_required
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if getattr(current_user, "role", None) != "admin":
            return jsonify({"status": "error", "message": "Forbidden"}), 403
        return view_func(*args, **kwargs)

    return wrapped
