FROM python:3.11-slim-bullseye
RUN pip install --no-cache-dir requests
COPY . .
