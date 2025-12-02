FROM python:3.12-slim

RUN useradd --create-home --shell /bin/bash charito

WORKDIR /app

COPY timeauthority-pkg /timeauthority-pkg
COPY charito-service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY charito-service/src /app/src
COPY charito-service/config /app/config

ENV PYTHONPATH=/app/src

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8082"]
