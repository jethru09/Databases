# config.py
import os
import logging

# Find the absolute path of the directory this file is in
basedir = os.path.abspath(os.path.dirname(__file__))
log_dir = os.path.join(basedir, 'logs')
os.makedirs(log_dir, exist_ok=True) # Create logs directory if it doesn't exist

class Config:
    """Base configuration settings."""
    # --- IMPORTANT: Set a Secret Key for JWT ---
    # Consider using environment variables for production.
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'ANEWSECRETKEY'
    GROUP_ID = int(os.environ.get('GROUP_ID', 3)) 
    # --- Central Database Configuration ---
    DB_HOST = os.environ.get('DB_HOST') or '10.0.116.125'
    DB_USER = os.environ.get('DB_USER') or 'cs432g3' 
    DB_PASSWORD = os.environ.get('DB_PASSWORD') or 'wM5nYqXr' 
    DB_NAME_CIMS = os.environ.get('DB_NAME_CIMS') or 'cs432cims'

    # --- Default Password for New Users ---
    DEFAULT_PASSWORD = 'default@p'

    # --- Logging Configuration ---
    LOGGING_FILENAME = os.path.join(log_dir, 'app.log')
    LOGGING_LEVEL = logging.INFO
    LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Could add DevelopmentConfig, ProductionConfig classes inheriting from Config
# but for now, this base class is sufficient.

