# Optional container image (the project runs without Docker as well). Base image pinned to the exact
# Python version used for the Linux checks (tests, Part 1 with SIGKILL, docker-compose demo).
FROM python:3.13.18-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY experiments ./experiments
ENV PYTHONPATH=/app/src
