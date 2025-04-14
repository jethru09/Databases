# app/auth/decorators.py
import jwt
from functools import wraps
from flask import request, jsonify, current_app # Use current_app for config

def token_required(f):
    """Decorator to ensure a valid local JWT token is present."""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            if auth_header.startswith('Bearer '):
                token = auth_header.split(' ')[1]

        if not token:
            current_app.logger.warning("Auth token missing in request header.")
            return jsonify({'error': 'Authorization Token is missing!'}), 401

        try:
            # Decode using the SECRET_KEY from the current app's config
            data = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=["HS256"])
            current_user_id = data['sub']
            current_user_role = data['role']
            current_app.logger.info(f"Token validated via decorator for user ID: {current_user_id}, Role: {current_user_role}")
        except jwt.ExpiredSignatureError:
            current_app.logger.warning("Expired token received.")
            return jsonify({'error': 'Token has expired!'}), 401
        except jwt.InvalidTokenError:
            current_app.logger.warning("Invalid token received.")
            return jsonify({'error': 'Token is invalid!'}), 401
        except Exception as e:
             current_app.logger.error(f"Error decoding token: {e}")
             return jsonify({'error': 'Token processing error', 'details': str(e)}), 401

        # Pass decoded info to the route function
        return f(current_user_id, current_user_role, *args, **kwargs)
    return decorated