from flask import request, jsonify, current_app, Blueprint
import mysql.connector
import hashlib
import jwt

from ..utils.database import get_cims_db_connection, get_g3_db_connection
from ..auth.decorators import token_required

# Create the Blueprint instance
search_join_bp = Blueprint('search_join', __name__) # 'members' is the blueprint name


@search_join_bp.route("/search/teaching_staff_info", methods=["POST"])
@token_required
def search_teaching_staff_info(current_user_id, current_user_role):
    current_app.logger.info(f"Teaching search by user {current_user_id}, role {current_user_role}")

    data = request.get_json()
    if not data or 'attributes_like' not in data:
        return jsonify({'error': 'Missing attributes_like in request'}), 400

    filters = data['attributes_like']
    if not isinstance(filters, dict) or not filters:
        return jsonify({'error': 'attributes_like must be a non-empty dictionary'}), 400

    try:
        # ---------- Connect to cs432g3 ----------
        conn_g3 = get_g3_db_connection()
        cursor_g3 = conn_g3.cursor(dictionary=True)

        g3_query = """
            SELECT ts.Faculty_ID, ts.FirstName, ts.MiddleName, ts.LastName,
                   ts.Email, s.Discipline_name, s.Designation, s.Room_number, s.Building,
                   c.Work, p.Home, p.Emergency
            FROM Teaching_staff ts
            JOIN Specialization s ON ts.Faculty_ID = s.Faculty_ID
            JOIN T_contact c ON ts.Faculty_ID = c.Faculty_ID
            JOIN Phone p ON c.Work = p.Work
        """
        cursor_g3.execute(g3_query)
        g3_results = cursor_g3.fetchall()

        # ---------- Connect to CIMS ----------
        conn_cims = get_cims_db_connection()
        cursor_cims = conn_cims.cursor(dictionary=True)

        cims_query = "SELECT * FROM G3_job_desc"
        cursor_cims.execute(cims_query)
        cims_results = cursor_cims.fetchall()

        # ---------- Python-side Join ----------
        final_results = []
        for g3_row in g3_results:
            for cims_row in cims_results:
                if (g3_row['Discipline_name'] == cims_row['Discipline_name'] and
                    g3_row['Designation'] == cims_row['Designation'] and
                    g3_row['Room_number'] == cims_row['Room_number'] and
                    g3_row['Building'] == cims_row['Building']):
                    
                    full_name = " ".join(filter(None, [g3_row['FirstName'], g3_row['MiddleName'], g3_row['LastName']]))
                    home_emerg = f"{g3_row.get('Home', '')}/{g3_row.get('Emergency', '')}"
                    office = f"{cims_row.get('Building')}/{cims_row.get('Room_number')}"
                    
                    result = {
                        "Name": full_name,
                        "Faculty_ID": g3_row.get("Faculty_ID"),
                        "Designation": cims_row.get("Designation"),
                        "Email": g3_row.get("Email"),
                        "Discipline_Section": cims_row.get("Discipline_name"),
                        "Work": g3_row.get("Work"),
                        "Home_Emerg": home_emerg,
                        "Office": office
                    }

                    # Apply filters if present
                    match = True
                    for key, val in filters.items():
                        if key in result and val.lower() not in str(result[key]).lower():
                            match = False
                            break

                    if match:
                        final_results.append(result)

        return jsonify({"results": final_results}), 200

    except Exception as e:
        current_app.logger.error(f"Teaching search failed: {e}")
        return jsonify({'error': 'Search failed', 'details': str(e)}), 500
    finally:
        if 'cursor_g3' in locals(): cursor_g3.close()
        if 'conn_g3' in locals(): conn_g3.close()
        if 'cursor_cims' in locals(): cursor_cims.close()
        if 'conn_cims' in locals(): conn_cims.close()