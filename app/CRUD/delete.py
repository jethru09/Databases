from flask import request, jsonify, current_app, Blueprint
import mysql.connector
import hashlib
import jwt

from ..utils.database import get_cims_db_connection, get_g3_db_connection
from ..auth.decorators import token_required

# Create the Blueprint instance
delete_bp = Blueprint('delete', __name__) # 'members' is the blueprint name

def table_exists(conn, table_name):
    cursor = conn.cursor()
    query = "SHOW TABLES LIKE %s"
    cursor.execute(query, (table_name,))
    result = cursor.fetchone()
    cursor.close()
    return result is not None

@delete_bp.route("/delete/<table_name>", methods=["POST"])
@token_required
def delete_from_table(current_user_id, current_user_role, table_name):
    current_app.logger.info(f"Delete request on table {table_name} by user {current_user_id}, role: {current_user_role} ")

    if current_user_role != "admin":
        return jsonify({'error': 'Admin privileges required'}), 403

    data = request.get_json()
    if not data or 'check_attributes' not in data:
        return jsonify({'error': 'Missing check_attributes in request'}), 400

    check_attributes = data['check_attributes']
    confirm = data.get('confirm', False)

    if not isinstance(check_attributes, dict) or not check_attributes:
        return jsonify({'error': 'check_attributes must be a non-empty dictionary'}), 400

    where_clause = " AND ".join([f"{key} = %s" for key in check_attributes])
    values = tuple(check_attributes.values())

    # Connect to both DBs to find table
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
    except Exception as err:
        return jsonify({'error': str(err)}), 404

    try:
        cursor = conn.cursor(dictionary=True)  # Return rows as dictionaries

        # Count how many will be deleted
        preview_query = f"SELECT * FROM {table_name} WHERE {where_clause}"
        cursor.execute(preview_query, values)
        matching_rows = cursor.fetchall()
        count = len(matching_rows)

        if not confirm:
            return jsonify({
                'preview': f"{count} row(s) will be deleted",
                'matching_rows': matching_rows,
                'confirm_required': True,
                'database': db_used
            }), 200

        # Proceed with deletion
        delete_query = f"DELETE FROM {table_name} WHERE {where_clause}"
        cursor.execute(delete_query, values)
        conn.commit()

        return jsonify({
            'message': f'{cursor.rowcount} row(s) deleted successfully',
            'database': db_used
        }), 200

    except Exception as delete_err:
        current_app.logger.error(f"Deletion error: {delete_err}")
        return jsonify({'error': 'Delete operation failed', 'details': str(delete_err)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
