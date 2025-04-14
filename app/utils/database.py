# app/utils/database.py
import mysql.connector
import logging
from flask import current_app 

def get_cims_db_connection():
    """
    Establishes a connection to the CIMS database using app config.
    Returns the connection object or None on failure.
    """
    try:
        conn = mysql.connector.connect(
            host=current_app.config['DB_HOST'],
            user=current_app.config['DB_USER'],
            password=current_app.config['DB_PASSWORD'],
            database=current_app.config['DB_NAME_CIMS']
        )
        # Using current_app.logger is often preferred within Flask contexts
        current_app.logger.debug("CIMS Database connection successful")
        return conn
    except mysql.connector.Error as err:
        current_app.logger.error(f"CIMS Database Connection Error: {err}")
        return None
    except Exception as e:
        # Catch potential errors if config isn't loaded yet (though factory should handle this)
        logging.critical(f"Failed to get DB connection config: {e}") # Use root logger if app context fails
        return None

def get_g3_db_connection():
    """
    Establishes a connection to the CIMS database using app config.
    Returns the connection object or None on failure.
    """
    try:
        conn = mysql.connector.connect(
            host=current_app.config['DB_HOST'],
            user=current_app.config['DB_USER'],
            password=current_app.config['DB_PASSWORD'],
            database=current_app.config['DB_USER']
        )
        # Using current_app.logger is often preferred within Flask contexts
        current_app.logger.debug("cs432g3 Database connection successful")
        return conn
    except mysql.connector.Error as err:
        current_app.logger.error(f"cs432g3 Database Connection Error: {err}")
        return None
    except Exception as e:
        # Catch potential errors if config isn't loaded yet (though factory should handle this)
        logging.critical(f"Failed to get DB connection config: {e}") # Use root logger if app context fails
        return None