# app/__init__.py
import logging
from logging.handlers import RotatingFileHandler
import os
from flask import Flask, jsonify

# Import configuration object
from config import Config

def create_app(config_class=Config):
    """Application Factory Function"""
    app = Flask(__name__, instance_relative_config=True)

    # Load configuration from config.py
    app.config.from_object(config_class)

    # Optional: Load instance config if it exists (e.g., for secrets)
    # app.config.from_pyfile('config.py', silent=True) # Looks in instance/config.py

    # Configure Logging
    # Ensure logs directory exists (handled in config.py now, but good practice)
    log_dir = os.path.dirname(app.config['LOGGING_FILENAME'])
    os.makedirs(log_dir, exist_ok=True)

    # Use RotatingFileHandler for better log management
    file_handler = RotatingFileHandler(
        app.config['LOGGING_FILENAME'],
        maxBytes=10240,  # Max 10KB per file
        backupCount=10   # Keep 10 backup files
    )
    file_handler.setFormatter(logging.Formatter(app.config['LOGGING_FORMAT']))
    file_handler.setLevel(app.config['LOGGING_LEVEL'])

    # Add handler to Flask's logger
    app.logger.addHandler(file_handler)
    app.logger.setLevel(app.config['LOGGING_LEVEL'])

    # Also configure root logger if needed (for libraries logging)
    logging.basicConfig(level=app.config['LOGGING_LEVEL'], format=app.config['LOGGING_FORMAT'])


    app.logger.info('CS432 API Startup') # Log app start

    # Register Blueprints
    from .auth.routes import auth_bp
    from .members.routes import members_bp
    from .CRUD.insert import insert_bp
    from .CRUD.update import update_bp
    from .CRUD.search import search_bp
    from .CRUD.delete import delete_bp
    from .CRUD.search_join import search_join_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(members_bp)
    app.register_blueprint(insert_bp) 
    app.register_blueprint(update_bp)
    app.register_blueprint(delete_bp)
    app.register_blueprint(search_bp)
    app.register_blueprint(search_join_bp)

    # Simple default route (optional)
    @app.route('/')
    def index():
         return jsonify({"message": "Welcome to the CS432 Group 3 API"})

    return app