import mysql.connector
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import os

app = FastAPI()

def create_table_if_not_exists():
    db = mysql.connector.connect(
        host="db",  # This is the service name in Docker Compose
        user="user",
        password="password",
        database="messages"
    )
    cursor = db.cursor()

    # The SQL statement to create the table if it does not exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS messages (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_name VARCHAR(255) NOT NULL,
        message TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    db.commit()
    cursor.close()
    db.close()

@app.on_event("startup")
def startup():
    # Ensure the table is created if it doesn't exist
    create_table_if_not_exists()

@app.get("/")
def read_root():
    return {"message": "Hello World"}

@app.get("/health")
def health_check():
    # Simple health check endpoint
    try:
        db = mysql.connector.connect(
            host="db",  # Host is the db service in Docker Compose
            user="user",
            password="password",
            database="messages"
        )
        db.ping(reconnect=True)  # Check if the DB is responsive
        db.close()
        return JSONResponse(status_code=200, content={"status": "healthy"})
    except mysql.connector.Error as err:
        return JSONResponse(status_code=500, content={"status": "unhealthy", "error": str(err)})

