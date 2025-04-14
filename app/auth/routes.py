# app/auth/routes.py
from flask import request, jsonify, current_app, Blueprint
import jwt
import datetime
import hashlib
import mysql.connector

# Import the database connection function from the utils module
from ..utils.database import get_cims_db_connection

# Create the Blueprint instance
auth_bp = Blueprint('auth', __name__) # 'auth' is the name of the blueprint

@auth_bp.route('/login', methods=['POST'])
def local_login():
    """
    Handles user login by querying the CIMS DB directly.
    Generates a local JWT session token upon success.
    Expects JSON Body: {"user": "MemberID", "password": "user_password"}
    """
    current_app.logger.info("Request received at LOCAL /login endpoint")
    try:
        data = request.get_json()
        if not data or 'user' not in data or 'password' not in data:
            current_app.logger.warning("Local Login attempt missing 'user' or 'password'.")
            return jsonify({"error": "Missing 'user' (MemberID) or 'password' parameter"}), 400
        member_id_str = data['user']
        password_provided = data['password']
    except Exception as e:
        current_app.logger.error(f"Error parsing local login request JSON: {e}")
        return jsonify({"error": "Invalid JSON data in request body"}), 400

    conn = None
    cursor = None
    try:
        conn = get_cims_db_connection() # Use the helper function
        if not conn: return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor(dictionary=True)

        # Use correct table name (Login)
        sql_select_user = "SELECT Password, Role FROM Login WHERE MemberID = %s"
        cursor.execute(sql_select_user, (member_id_str,))
        user_data = cursor.fetchone()

        if not user_data:
            current_app.logger.warning(f"Local Login failed: MemberID {member_id_str} not found.")
            return jsonify({"error": "Invalid credentials - User not found"}), 401

        stored_password_hash = user_data['Password']
        user_role = user_data['Role']

        # --- Password Verification Logic (Copied from previous version) ---
        password_match = False
        stored_password = user_data['Password']

        if stored_password.startswith(('$2a$', '$2b$', '$2y$')) and len(stored_password) == 60:
             try:
                 import bcrypt
                 if bcrypt.checkpw(password_provided.encode(), stored_password.encode()):
                     password_match = True
                     current_app.logger.info(f"Password matched (Bcrypt) for MemberID {member_id_str}.")
                 else:
                     current_app.logger.warning(f"Password mismatch (Bcrypt) for MemberID {member_id_str}.")
             except ImportError: current_app.logger.error("bcrypt library not installed.")
             except Exception as bcrypt_err: current_app.logger.error(f"Bcrypt check error: {bcrypt_err}")
        elif not password_match and len(stored_password) == 32:
             try:
                 int(stored_password, 16)
                 provided_hash = hashlib.md5(password_provided.encode()).hexdigest()
                 if provided_hash == stored_password:
                     password_match = True
                     current_app.logger.info(f"Password matched (MD5) for MemberID {member_id_str}.")
                 else:
                     current_app.logger.warning(f"Password mismatch (MD5) for MemberID {member_id_str}.")
             except ValueError: current_app.logger.warning(f"Stored password for {member_id_str} is 32 chars but not hex.")
             except Exception as md5_err: current_app.logger.error(f"Error during MD5 comparison: {md5_err}")
        elif not password_match:
             if password_provided == stored_password:
                 password_match = True
                 current_app.logger.info(f"Password matched (Plain Text) for MemberID {member_id_str}.")
             else:
                 current_app.logger.warning(f"Password mismatch (Plain Text fallback) for MemberID {member_id_str}.")
        # --- End Password Verification ---

        if not password_match:
            return jsonify({"error": "Invalid credentials - Password mismatch"}), 401

        current_app.logger.info(f"Credentials valid for MemberID {member_id_str}. Role: {user_role}. Generating local JWT.")

        # Generate JWT Token
        token_payload = {
            'sub': member_id_str,
            'role': user_role,
            # Use timezone aware UTC time
            'iat': datetime.datetime.now(datetime.timezone.utc),
            'exp': datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        }
        session_token = jwt.encode(
            token_payload,
            current_app.config['SECRET_KEY'], # Use key from app config
            algorithm="HS256"
        )
        current_app.logger.info(f"Local JWT generated for MemberID {member_id_str}.")

        return jsonify({
            "message": "Login successful (Locally Authenticated)",
            "session_token": session_token
        }), 200

    except mysql.connector.Error as db_err:
        current_app.logger.error(f"Database Error during local login: {db_err}")
        return jsonify({"error": "Database error during login", "details": str(db_err)}), 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error during local login: {e}", exc_info=True) # Log traceback
        return jsonify({"error": "An internal server error occurred", "details": str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn and conn.is_connected(): conn.close()