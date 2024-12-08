"""
This is a FastAPI application that provides a simple health check endpoint
and potentially other functionalities related to message management.
"""

import time
import json
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import mysql.connector
import redis

class AppState:
    """
    Class to store the various connections
    """
    def __init__(self):
        self.mysql_db = db = mysql.connector.connect(
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

        self.redis_client = redis.StrictRedis(host="redis", port=6379, decode_responses=True)
    
    def health_check(self):
        """
        Health check pings
        """
        self.mysql_db.ping(reconnect=True)
        self.redis_client.ping()
    
    def shut_down(self):
        """
        Shutdown the connections
        """
        self.mysql_db.close()
        self.redis_client.close()


app = FastAPI()

@app.on_event("startup")
async def startup():
    """
    Creates the object for the app state and stores the connection
    """
    #waiting for 5s as in my local system things are slightly slow
    time.sleep(5)
    app.state = AppState()

@app.on_event("shutdown")
async def shutdown():
    """
    Closing the connection on shut down
    """
    app.state.shut_down()


@app.get("/")
async def read_root():
    """
    A simple root endpoint that returns a "Hello World" message.
    """
    return {"message": "Hello World"}


@app.get("/health")
async def health_check():
    """
    Performs a health check by attempting to connect to the database.

    Returns a JSON response with a status of "healthy" if successful,
    or "unhealthy" with an error message otherwise.
    """
    try:
        app.state.health_check()
        return JSONResponse(status_code=200, content={"status": "healthy"})
    except (mysql.connector.Error, redis.exceptions.ConnectionError) as err:
        return JSONResponse(status_code=500, content={"status": "unhealthy", "error": str(err)})

@app.post("/message")
def post_message(user_name: str, message: str):
    """
    Save the message in Redis.
    """
    try:
        timestamp = datetime.now(timezone.utc).timestamp()
        redis_message = json.dumps({"user_name": user_name, 
                                    "message": message, "timestamp": timestamp})
        app.state.redis_client.zadd("messages", {redis_message: timestamp})
        return {"message": "Message saved successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving message: {e}") from e

@app.get("/getmessage")
def fetch_messages(timestamp: str):
    """
    Fetch up to 5 messages older than the given timestamp from Redis or MySQL.
    """
    try:
        # Fetch from Redis
        utc_timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").timestamp()
        redis_messages = app.state.redis_client.zrangebyscore("messages", "-inf", utc_timestamp, start=0, num=5)
        if redis_messages:
            messages = [json.loads(msg) for msg in redis_messages]
            return {"source": "redis", "messages": messages}

        cursor = app.state.mysql_db.cursor(dictionary=True)

        query = """
            SELECT user_name, message, UNIX_TIMESTAMP(created_at) AS timestamp
            FROM messages
            WHERE UNIX_TIMESTAMP(created_at) < %s
            ORDER BY created_at DESC
            LIMIT 5
        """
        cursor.execute(query, (timestamp,))
        messages = cursor.fetchall()

        return {"source": "mysql", "messages": messages}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching messages: {e}") from e
    finally:
        cursor.close()