# syntax=docker/dockerfile:1
ARG PYTHON_VERSION
FROM python:$PYTHON_VERSION-slim
WORKDIR /code
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY ./work/app.py .
