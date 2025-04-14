# app/members/routes.py
from flask import request, jsonify, current_app, Blueprint
import mysql.connector
import hashlib

# Import helpers and decorators
from ..utils.database import get_cims_db_connection
from ..auth.decorators import token_required

# Create the Blueprint instance
members_bp = Blueprint('members', __name__) # 'members' is the blueprint name

# --- Task 1 Endpoint: Add Member (Admin Only) ---
@members_bp.route('/admin/add_member', methods=['POST'])
@token_required
def add_member_task1(current_user_id, current_user_role):
    """Task 1: Adds member/login. Requires LOCAL Admin auth."""
    current_app.logger.info(f"Request: /admin/add_member by user ID: {current_user_id}, Role: {current_user_role}")

    if current_user_role != 'admin':
        current_app.logger.warning(f"Access denied: User {current_user_id} (Role: {current_user_role}) is not admin.")
        return jsonify({"error": "Admin privileges required"}), 403

    current_app.logger.info(f"Admin user {current_user_id} authorized. Adding member.")

    try:
        data = request.get_json()
        if not data or 'name' not in data or 'email' not in data:
            current_app.logger.warning("Missing 'name' or 'email' in add_member request")
            return jsonify({"error": "Missing 'name' or 'email' in request JSON body"}), 400
        new_name = data['name']
        new_email = data['email']
    except Exception as e:
        current_app.logger.error(f"Error parsing add_member request JSON: {e}")
        return jsonify({"error": "Invalid JSON data in request body"}), 400

    conn = None
    cursor = None
    new_member_id = None
    try:
        conn = get_cims_db_connection()
        if not conn: return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        # Insert into members (Use correct column names: UserName, emailID)
        sql_insert_member = "INSERT INTO members (UserName, emailID) VALUES (%s, %s)"
        cursor.execute(sql_insert_member, (new_name, new_email))
        current_app.logger.info(f"Executed INSERT members for '{new_name}'.")

        new_member_id = cursor.lastrowid
        if not new_member_id:
            raise mysql.connector.Error("Failed to retrieve last inserted ID from members.")
        current_app.logger.info(f"New member ID: {new_member_id}")

        # Hash default password (using MD5 for consistency as decided earlier)
        default_password = current_app.config['DEFAULT_PASSWORD']
        hashed_default_password = hashlib.md5(default_password.encode()).hexdigest()

        # Insert into Login (Use correct table and column names: Login, MemberID, Password, Role)
        sql_insert_login = "INSERT INTO Login (MemberID, Password, Role) VALUES (%s, %s, %s)"
        cursor.execute(sql_insert_login, (new_member_id, hashed_default_password, 'user')) # Default role 'user'
        current_app.logger.info(f"Executed INSERT Login for MemberID {new_member_id}.")

        conn.commit()
        current_app.logger.info(f"DB transaction committed for new member {new_member_id}.")

        return jsonify({
            "message": "Task 1 Success: Member created and login entry added (using local auth).",
            "member_id": new_member_id,
            "MemberID": new_member_id,
        }), 201

    except mysql.connector.Error as db_err:
        current_app.logger.error(f"Database Error during Task 1: {db_err}")
        if conn: conn.rollback(); current_app.logger.info("DB transaction rolled back.")
        # Check specifically for duplicate key error
        if db_err.errno == 1062: # MySQL error code for duplicate entry
             return jsonify({"error": "Duplicate entry detected. MemberID might already exist in Login table.", "details": str(db_err)}), 409 # 409 Conflict
        return jsonify({"error": "Database error occurred", "details": str(db_err)}), 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error during Task 1: {e}", exc_info=True)
        if conn: conn.rollback(); current_app.logger.info("DB transaction rolled back.")
        return jsonify({"error": "An internal server error occurred", "details": str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn and conn.is_connected(): conn.close()


# --- Task 2 Demo: View Own Profile ---
@members_bp.route('/profile/me', methods=['GET'])
@token_required
def get_my_profile(current_user_id, current_user_role):
    """Allows any authenticated user to view their own profile."""
    current_app.logger.info(f"Request: /profile/me by user ID: {current_user_id}, Role: {current_user_role}")
    conn = None
    cursor = None
    try:
        conn = get_cims_db_connection()
        if not conn: return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(dictionary=True)

        # Use correct column names (ID, UserName, emailID, DoB)
        sql_select_me = "SELECT ID, UserName, emailID, DoB FROM members WHERE ID = %s"
        cursor.execute(sql_select_me, (current_user_id,))
        profile_data = cursor.fetchone()

        if not profile_data:
            current_app.logger.warning(f"Profile data not found for user ID: {current_user_id}")
            return jsonify({"error": "Profile data not found"}), 404

        current_app.logger.info(f"Retrieved profile for user ID: {current_user_id}")
        return jsonify(profile_data), 200

    except mysql.connector.Error as db_err:
         current_app.logger.error(f"Database Error in /profile/me: {db_err}")
         return jsonify({"error": "Database error", "details": str(db_err)}), 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error in /profile/me: {e}", exc_info=True)
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn and conn.is_connected(): conn.close()


# --- Task 2 Demo: View Any Profile (Admin Only) ---
@members_bp.route('/admin/profile/<int:target_member_id>', methods=['GET'])
@token_required
def get_any_profile(current_user_id, current_user_role, target_member_id):
    """Allows ONLY admins to view any member profile by ID."""
    current_app.logger.info(f"Request: /admin/profile/{target_member_id} by user ID: {current_user_id}, Role: {current_user_role}")

    if current_user_role != 'admin':
        current_app.logger.warning(f"Access denied: User {current_user_id} (Role: {current_user_role}) is not admin for /admin/profile.")
        return jsonify({"error": "Admin privileges required"}), 403

    current_app.logger.info(f"Admin user {current_user_id} authorized. Fetching profile for target: {target_member_id}")
    conn = None
    cursor = None
    try:
        conn = get_cims_db_connection()
        if not conn: return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(dictionary=True)

        # Use correct column names
        sql_select_target = "SELECT ID, UserName, emailID, DoB FROM members WHERE ID = %s"
        cursor.execute(sql_select_target, (target_member_id,))
        profile_data = cursor.fetchone()

        if not profile_data:
            current_app.logger.warning(f"Admin {current_user_id} requested non-existent profile ID: {target_member_id}")
            return jsonify({"error": f"Member with ID {target_member_id} not found"}), 404

        current_app.logger.info(f"Admin {current_user_id} retrieved profile for ID: {target_member_id}")
        return jsonify(profile_data), 200

    except mysql.connector.Error as db_err:
        current_app.logger.error(f"Database Error in /admin/profile: {db_err}")
        return jsonify({"error": "Database error", "details": str(db_err)}), 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error in /admin/profile: {e}", exc_info=True)
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn and conn.is_connected(): conn.close()
        
# --- ***** Task 3 Endpoint: Delete Member (Admin Only) ***** ---
@members_bp.route('/admin/delete_member/<int:member_id_to_delete>', methods=['DELETE'])
@token_required
def delete_member_task3(current_user_id, current_user_role, member_id_to_delete):
    """
    Task 3: Deletes a member conditionally based on group mappings.
    Requires LOCAL Admin authentication.
    """
    current_app.logger.info(f"Request: /admin/delete_member/{member_id_to_delete} by Admin ID: {current_user_id}")

    # 1. RBAC Check (Admin Only)
    if current_user_role != 'admin':
        current_app.logger.warning(f"Access denied: User {current_user_id} (Role: {current_user_role}) cannot delete members.")
        return jsonify({"error": "Admin privileges required to delete members"}), 403

    # 2. Check if Member Exists (Optional but good practice)
    conn = None
    cursor = None
    try:
        conn = get_cims_db_connection()
        if not conn: return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(dictionary=True) # Use dictionary cursor for easier access

        # Check if member exists in members table first
        cursor.execute("SELECT ID FROM members WHERE ID = %s", (member_id_to_delete,))
        member_exists = cursor.fetchone()
        if not member_exists:
            current_app.logger.warning(f"Admin {current_user_id} tried to delete non-existent MemberID: {member_id_to_delete}")
            return jsonify({"error": f"Member with ID {member_id_to_delete} not found in members table."}), 404

        # 3. Check Group Mappings
        # Ensure correct column names for MemberGroupMapping (assuming MemberID)
        sql_check_mapping = "SELECT COUNT(*) as mapping_count FROM MemberGroupMapping WHERE MemberID = %s"
        cursor.execute(sql_check_mapping, (member_id_to_delete,))
        result = cursor.fetchone()
        mapping_count = result['mapping_count'] if result else 0
        current_app.logger.info(f"MemberID {member_id_to_delete} found in {mapping_count} group mappings.")

        # Use a transaction for the deletion steps
        # conn.start_transaction()
        deleted_from_login = 0
        deleted_from_members = 0
        deleted_from_mapping = 0
        message = ""

        # 4. Conditional Deletion Logic
        if mapping_count == 0:
            # Case 1: Member is in NO groups - Delete fully
            current_app.logger.info(f"MemberID {member_id_to_delete} has no group mappings. Proceeding with full deletion.")

            # Delete from Login table (Use correct table/column names)
            sql_delete_login = "DELETE FROM Login WHERE MemberID = %s"
            cursor.execute(sql_delete_login, (member_id_to_delete,))
            deleted_from_login = cursor.rowcount
            current_app.logger.info(f"Deleted {deleted_from_login} row(s) from Login for MemberID {member_id_to_delete}.")

            # Delete from members table (Use correct table/column names)
            sql_delete_members = "DELETE FROM members WHERE ID = %s"
            cursor.execute(sql_delete_members, (member_id_to_delete,))
            deleted_from_members = cursor.rowcount
            current_app.logger.info(f"Deleted {deleted_from_members} row(s) from members for ID {member_id_to_delete}.")

            if deleted_from_members > 0: # Check if deletion from members actually happened
                 message = f"Member {member_id_to_delete} deleted successfully from members and Login tables."
            else:
                 # Should not happen if member_exists check passed, but good to handle
                 message = f"Member {member_id_to_delete} not found in members table during deletion (unexpected)."
                 # Might indicate a race condition or issue, consider raising error or rolling back implicitly


        else:
            # Case 2: Member IS in other groups - Delete only specific mapping
            our_group_id = current_app.config.get('GROUP_ID', 'cs432g2') # Get from config
            current_app.logger.info(f"MemberID {member_id_to_delete} has group mappings. Deleting mapping for group '{our_group_id}'.")

            # Delete specific mapping (Use correct table/column names: MemberGroupMapping, MemberID, GroupID)
            sql_delete_mapping = "DELETE FROM MemberGroupMapping WHERE MemberID = %s AND GroupID = %s"
            cursor.execute(sql_delete_mapping, (member_id_to_delete, our_group_id))
            deleted_from_mapping = cursor.rowcount
            current_app.logger.info(f"Deleted {deleted_from_mapping} mapping(s) for MemberID {member_id_to_delete} and GroupID {our_group_id}.")

            if deleted_from_mapping > 0:
                 message = f"Removed association for Member {member_id_to_delete} with Group {our_group_id}. Member NOT deleted from system."
            else:
                 message = f"Member {member_id_to_delete} was not associated with Group {our_group_id}. No mapping deleted."

        # 5. Commit Transaction
        conn.commit()
        current_app.logger.info(f"DB transaction committed for deletion task regarding MemberID {member_id_to_delete}.")

        return jsonify({
            "message": message,
            "deleted_from_login": deleted_from_login,
            "deleted_from_members": deleted_from_members,
            "deleted_from_mapping_for_this_group": deleted_from_mapping
            }), 200 # OK status for successful operation

    except mysql.connector.Error as db_err:
        current_app.logger.error(f"Database Error during Task 3 execution for MemberID {member_id_to_delete}: {db_err}")
        if conn: conn.rollback(); current_app.logger.info("DB transaction rolled back due to DB error.")
        return jsonify({"error": "Database error occurred during deletion", "details": str(db_err)}), 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error during Task 3 for MemberID {member_id_to_delete}: {e}", exc_info=True)
        if conn: conn.rollback(); current_app.logger.info("DB transaction rolled back due to unexpected error.")
        return jsonify({"error": "An internal server error occurred during deletion", "details": str(e)}), 500
    finally:
        # Ensure cursor and connection are always closed
        if cursor: cursor.close()
        if conn and conn.is_connected(): conn.close()