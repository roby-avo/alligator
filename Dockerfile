# syntax=docker/dockerfile:1
ARG PYTHON_VERSION
FROM python:$PYTHON_VERSION-slim

# Install necessary system packages and dependencies
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    libhdf5-dev \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /code

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0

# Copy requirements and install them
COPY ./api/requirements.txt requirements.txt
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Expose the port the app runs on
EXPOSE 5000