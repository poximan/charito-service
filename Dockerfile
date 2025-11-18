FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src
COPY config /app/config

ENV PYTHONPATH=/app/src

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8082"]
