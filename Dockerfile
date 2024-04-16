# syntax=docker/dockerfile:1
ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim

# Set the working directory in the container
WORKDIR /code

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0

# copy requirements.txt 
COPY requirements.txt requirements.txt

# Install system dependencies required for h5py
RUN apt-get update && apt-get install -y \
    build-essential \
    libhdf5-dev \
    libhdf5-103 \
    pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy the Flask application into the container
COPY ./api/app.py /code/

# The command to run the app when the container starts
CMD ["flask", "run"]
