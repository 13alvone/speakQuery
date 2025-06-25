from flask import Blueprint, request, jsonify
from flask import current_app as app
from utils.auth import admin_required
import sqlite3
import logging

settings_bp = Blueprint('settings_bp', __name__)

@settings_bp.route('/get_settings', methods=['GET'])
@admin_required
def get_settings():
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT key, value FROM app_settings')
            _settings = cursor.fetchall()

        settings_dict = {}
        for key, value in _settings:
            if key == 'ALLOWED_EXTENSIONS':
                settings_dict[key] = ','.join(app.config.get(key, []))
            elif key == 'ALLOWED_API_DOMAINS':
                settings_dict[key] = ','.join(app.config.get(key, []))
            elif key in ['MAX_CONTENT_LENGTH', 'KEEP_LATEST_FILES', 'QUEUE_SIZE', 'PROCESSING_LIMIT']:
                settings_dict[key] = int(app.config.get(key, 0))
            elif key == 'THROTTLE_ENABLED':
                settings_dict[key] = app.config.get(key, False)
            else:
                settings_dict[key] = app.config.get(key, '')

        return jsonify({'status': 'success', 'settings': settings_dict}), 200
    except Exception as e:
        logging.error(f"Error fetching settings: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to fetch settings.'}), 500


@settings_bp.route('/update_settings', methods=['POST'])
@admin_required
def update_settings():
    try:
        data = request.json
        if not data or 'settings' not in data:
            return jsonify({'status': 'error', 'message': 'No settings provided.'}), 400

        _settings = data['settings']
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cursor = conn.cursor()

        for key, value in _settings.items():
            if key == 'ALLOWED_EXTENSIONS':
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (value, key))
                app.config[key] = set(ext.strip().lower() for ext in value.split(','))
            elif key == 'ALLOWED_API_DOMAINS':
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (value, key))
                app.config[key] = set(d.strip() for d in value.split(',') if d.strip())
            elif key == 'MAX_CONTENT_LENGTH':
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (str(int(value)), key))
                app.config[key] = int(value)
            elif key == 'KEEP_LATEST_FILES':
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (str(int(value)), key))
                app.config[key] = int(value)
            elif key in ['QUEUE_SIZE', 'PROCESSING_LIMIT']:
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (str(int(value)), key))
                app.config[key] = int(value)
            elif key == 'THROTTLE_ENABLED':
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (str(value).lower(), key))
                app.config[key] = str(value).lower() in {'true', '1', 'yes'}
            else:
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (value, key))
                app.config[key] = value

            conn.commit()

        return jsonify({'status': 'success', 'message': 'Settings updated successfully.'}), 200

    except Exception as e:
        logging.error(f"Error updating settings: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to update settings.'}), 500

