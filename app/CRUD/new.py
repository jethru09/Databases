
from flask import request, jsonify, current_app, Blueprint
import mysql.connector
import hashlib
import jwt

# Import helpers and decorators
from ..utils.database import get_cims_db_connection
from ..utils.database import get_g3_db_connection
from ..auth.decorators import token_required


# Create the Blueprint instance
insert_bp = Blueprint('insert', __name__) # 'members' is the blueprint name

def table_exists(conn, table_name):
    cursor = conn.cursor()
    query = "SHOW TABLES LIKE %s"
    cursor.execute(query, (table_name,))
    result = cursor.fetchone()
    cursor.close()
    return result is not None

# --- Insert Generic Endpoint (POST) ---
@insert_bp.route("/insert/<table_name>", methods=["POST"])
@token_required
def insert_into_table(current_user_id, current_user_role, table_name):
    
    current_app.logger.info(f"Request: /insert/{table_name} by user {current_user_id}, role: {current_user_role} with method {request.method}")

    # If admin-level insertion is needed, check role
    if current_user_role != "admin":
        current_app.logger.warning(f"Unauthorized attempt to insert into admin table: {table_name}")
        return jsonify({'error': 'Admin privileges required'}), 403
    
    current_app.logger.info(f"User {current_user_id} (role: {current_user_role}) authorized to insert into table {table_name}.")

    # Extract attributes from JSON payload
    data = request.get_json()
    if not data or 'attributes' not in data:
        return jsonify({'error': 'Missing attributes in request'}), 400

    attributes = data['attributes']
    columns = ', '.join(attributes.keys())
    values = tuple(attributes.values())
    placeholders = ', '.join(['%s'] * len(values))

    # First try DB_USER (cs432g3)
    # Check both DBs for table existence
    conn = None
    db_used = None
    try:
        conn = get_g3_db_connection()
        if conn and table_exists(conn, table_name):
            db_used = "G3"
        else:
            if conn:
                conn.close()
            conn = get_cims_db_connection()
            if conn and table_exists(conn, table_name):
                db_used = "CIMS"
            else:
                raise ValueError(f"Table '{table_name}' not found in either database.")
    except Exception as db_check_err:
        current_app.logger.error(f"Database/table validation failed: {db_check_err}")
        return jsonify({'error': str(db_check_err)}), 404

   # Insert
    try:
        cursor = conn.cursor()
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, values)
        conn.commit()
        inserted_id = cursor.lastrowid
        return jsonify({'message': 'Insert successful', 'inserted_id': inserted_id, 'database': db_used}), 201
    except Exception as insert_err:
        current_app.logger.error(f"Insertion error: {insert_err}")
        return jsonify({'error': 'Database insert failed', 'details': str(insert_err)}), 500
    except Exception as e:
        current_app.logger.exception(f"Unexpected error in insert route: {e}")
        return jsonify({'error': 'Internal server error', 'details': str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
    
