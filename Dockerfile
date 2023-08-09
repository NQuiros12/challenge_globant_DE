# Use the official Python image as the base
FROM python:3.9

# Set environment variables for MySQL
ENV MYSQL_DATABASE=globant_db \
    MYSQL_USER=root \
    MYSQL_PASSWORD=micolash12 \
    MYSQL_HOST=localhost \
    MYSQL_PORT=3306

# Install MySQL client and other dependencies
RUN apt-get update && apt-get install -y default-libmysqlclient-dev

# Set working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI application code into the container
COPY . /app/

# Expose the FastAPI port
EXPOSE 8000

# Start the FastAPI service
CMD ["uvicorn", "main:app", "--host", "0.0.0.0"]
