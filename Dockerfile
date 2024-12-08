# Base image
FROM python:3.11-slim

# Set the working directory
WORKDIR /simple-thought-share-app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY /simple-thought-share-app .

# Default command
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

