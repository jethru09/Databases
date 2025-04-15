
from flask import request, jsonify, current_app, Blueprint
import mysql.connector
import hashlib
import jwt

# Import helpers and decorators
from ..utils.database import get_cims_db_connection
from ..utils.database import get_g3_db_connection
from ..auth.decorators import token_required


# Create the Blueprint instance
update_bp = Blueprint('update', __name__) # 'members' is the blueprint name

def table_exists(conn, table_name):
    cursor = conn.cursor()
    query = "SHOW TABLES LIKE %s"
    cursor.execute(query, (table_name,))
    result = cursor.fetchone()
    cursor.close()
    return result is not None

# --- Insert Generic Endpoint (POST) ---
@update_bp.route("/update/<table_name>", methods=["POST"])
@token_required
def update_table(current_user_id, current_user_role, table_name):
    current_app.logger.info(f"Request: /update/{table_name} by user {current_user_id}, role: {current_user_role} with method {request.method}")

    # Admin check
    if current_user_role != "admin":
        current_app.logger.warning(f"Unauthorized update attempt to admin table: {table_name}")
        return jsonify({'error': 'Admin privileges required'}), 403
    
    current_app.logger.info(f"User {current_user_id} (role: {current_user_role}) authorized to insert into table {table_name}.")
    # Parse JSON
    data = request.get_json()
    if not data or 'attributes' not in data or 'attributes_changed' not in data:
        return jsonify({'error': 'Both "attributes" and "attributes_changed" are required'}), 400

    filters = data['attributes']
    updates = data['attributes_changed']

    if not filters or not updates:
        return jsonify({'error': 'Empty "attributes" or "attributes_changed"'}), 400

    where_clause = ' AND '.join([f"{k} = %s" for k in filters])
    update_clause = ', '.join([f"{k} = %s" for k in updates])
    where_values = tuple(filters.values())
    update_values = tuple(updates.values())

    # Check DBs
    conn = None
    db_used = None
    try:
        conn = get_g3_db_connection()
        if conn and table_exists(conn, table_name):
            db_used = "G3"
        else:
            if conn: conn.close()
            conn = get_cims_db_connection()
            if conn and table_exists(conn, table_name):
                db_used = "CIMS"
            else:
                raise ValueError(f"Table '{table_name}' not found in either database.")
    except Exception as db_check_err:
        current_app.logger.error(f"Database/table check failed: {db_check_err}")
        return jsonify({'error': str(db_check_err)}), 404

    # Perform update
    try:
        cursor = conn.cursor()
        update_query = f"UPDATE {table_name} SET {update_clause} WHERE {where_clause}"
        cursor.execute(update_query, update_values + where_values)
        conn.commit()
        affected = cursor.rowcount
        if affected == 0:
            return jsonify({'message': 'No matching record found to update'}), 404
        return jsonify({'message': 'Update successful', 'rows_updated': affected, 'database': db_used}), 200
    except Exception as update_err:
        current_app.logger.error(f"Update failed: {update_err}")
        return jsonify({'error': 'Database update failed', 'details': str(update_err)}), 500
    except Exception as e:
        current_app.logger.exception(f"Unexpected error in update route: {e}")
        return jsonify({'error': 'Internal server error', 'details': str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
