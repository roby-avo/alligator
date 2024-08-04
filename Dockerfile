# syntax=docker/dockerfile:1
ARG PYTHON_VERSION
FROM python:$PYTHON_VERSION-slim

WORKDIR /app

COPY requirements.txt .

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libhdf5-dev \
    && apt-get clean

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["gunicorn", "app.main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:5000", "--timeout", "300", "--reload", "--log-level", "debug"]