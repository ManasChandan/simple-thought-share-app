"""
This is a FastAPI application that provides a simple health check endpoint
and potentially other functionalities related to message management.
"""

import time
import json
import os
import pickle
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler
import mysql.connector
import redis

def retry(retries=3, delay=10):
    """
    Retry decorator for functions that may raise exceptions.

    Args:
        retries: Number of retries to attempt.
        delay: Fixed delay in seconds between retries.

    Returns:
        The decorated function.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 0
            raised_exception = None
            while attempt < retries:
                try:
                    return func(*args, **kwargs)
                except mysql.connector.Error as e:
                    attempt += 1
                    time.sleep(delay)
                    raised_exception = e   
            raise raised_exception
        return wrapper
    return decorator

class AppState:
    """
    Class to store the various connections
    """
    @retry(retries=3, delay=10)
    def __init__(self):
        self.mysql_db = db = mysql.connector.connect(
        host="127.0.0.1" if os.environ.get("ENV", "local_run") == "local_run" else "db",  # This is the service name in Docker Compose
        port="3306",
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

        self.redis_client = redis.StrictRedis(host="127.0.0.1" if os.environ.get("ENV", "local_run") == "local_run" else "redis", 
                                              port=6379, decode_responses=True)
    
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

scheduler = BackgroundScheduler()

def move_new_messages_to_mysql():
    """
    Transfer new messages from Redis to MySQL and delete them from Redis.
    """
    try:
        save_debug = []

        cursor = app.state.mysql_db.cursor()

        # Fetch all messages from Redis
        redis_messages = app.state.redis_client.zrange("messages", 0, -1, withscores=True)
        if not redis_messages:
            print("No new messages to transfer.")
            return

        for msg, score in redis_messages:
            data = json.loads(msg)
            save_debug.append(data)
            user_name = data["user_name"]
            message = data["message"]
            created_at = datetime.fromtimestamp(score, tz=timezone.utc)

            # Insert into MySQL (skip duplicates)
            query = """
                INSERT INTO messages (user_name, message, created_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE id=id;
            """
            cursor.execute(query, (user_name, message, created_at))

            # Remove the message from Redis after successful insertion
            app.state.redis_client.zrem("messages", msg)

        app.state.mysql_db.commit()
        cursor.close()
        print("Messages Moved to MySQL and Redis is Empty")

        os.makedirs("Debug", exist_ok=True)
        with open(f"Debug/messages_{int(datetime.now(timezone.utc).timestamp())}.pkl", 'wb') as file:
            pickle.dump(save_debug, file)

    except mysql.connector.Error as e:
        raise mysql.connector.Error(f"Error transferring messages: {e}") from e

scheduler.add_job(
    func=move_new_messages_to_mysql, 
    trigger="interval",
    minutes=1
)

@app.on_event("startup")
async def startup():
    """
    Creates the object for the app state and stores the connection
    """
    app.state = AppState()
    scheduler.start()

@app.on_event("shutdown")
async def shutdown():
    """
    Closing the connection on shut down
    """
    app.state.shut_down()
    scheduler.shutdown()


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
        messages = []
        utc_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").astimezone(timezone.utc)
        utc_timestamp = utc_time.timestamp()
        print(utc_timestamp)
        redis_messages = app.state.redis_client.zrangebyscore("messages", "-inf", utc_timestamp, start=0, num=5)
        if redis_messages:
            messages = [json.loads(msg) for msg in redis_messages]
        
        if len(messages) < 5:

            cursor = app.state.mysql_db.cursor(dictionary=True)

            query = """
                SELECT *
                FROM messages
                WHERE created_at < %s
                ORDER BY created_at DESC
                LIMIT %s
            """
            cursor.execute(query, (utc_time, 5-len(messages),))
            messages_sql = cursor.fetchall()
            messages.extend(messages_sql)
            cursor.close()

        return {"messages": messages}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching messages: {e}") from e