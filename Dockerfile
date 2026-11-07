# Optional container image (the project runs without Docker as well).
FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY experiments ./experiments
ENV PYTHONPATH=/app/src
