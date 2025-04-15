from flask import request, jsonify, current_app, Blueprint
import mysql.connector
import hashlib
import jwt

from ..utils.database import get_cims_db_connection, get_g3_db_connection
from ..auth.decorators import token_required

# Create the Blueprint instance
search_bp = Blueprint('search', __name__) # 'members' is the blueprint name

def table_exists(conn, table_name):
    cursor = conn.cursor()
    query = "SHOW TABLES LIKE %s"
    cursor.execute(query, (table_name,))
    result = cursor.fetchone()
    cursor.close()
    return result is not None

@search_bp.route("/search/<table_name>", methods=["POST"])
@token_required
def search_table(current_user_id, current_user_role, table_name):
    current_app.logger.info(f"Search request on table: {table_name} by user {current_user_id}, role: {current_user_role} ")
    
    data = request.get_json()
    if not data or 'attributes_like' not in data:
        return jsonify({'error': 'Missing attributes_like in request'}), 400

    search_attrs = data['attributes_like']
    if not isinstance(search_attrs, dict) or not search_attrs:
        return jsonify({'error': 'attributes_like must be a non-empty dictionary'}), 400

    # Prepare SQL query
    where_clause = " AND ".join([f"{key} LIKE %s" for key in search_attrs])
    like_values = [f"%{value}%" for value in search_attrs.values()]

    # Connect to both DBs
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
    except Exception as e:
        current_app.logger.error(f"Database/table validation failed: {e}")
        return jsonify({'error': str(e)}), 404

    # Execute the SELECT query
    try:
        cursor = conn.cursor(dictionary=True)
        query = f"SELECT * FROM {table_name} WHERE {where_clause}"
        cursor.execute(query, like_values)
        results = cursor.fetchall()
        return jsonify({'results': results, 'database': db_used}), 200
    except Exception as e:
        current_app.logger.error(f"Search query error: {e}")
        return jsonify({'error': 'Search failed', 'details': str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
