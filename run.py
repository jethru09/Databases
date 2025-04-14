# run.py
from app import create_app # Import the factory function from app package

# Create the Flask app instance using the factory
app = create_app()

if __name__ == '__main__':
    # Use Flask's built-in server for development
    # Host 0.0.0.0 makes it accessible on your network
    # Debug=True enables auto-reload and debugger (disable for production)
    app.run(host='0.0.0.0', port=5000, debug=True)