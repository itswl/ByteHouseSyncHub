FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY postgres_to_bytehouse.py .

RUN mkdir -p /app/logs

ENV LOG_FILE=/app/logs/postgres_to_bytehouse.log
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "postgres_to_bytehouse.py"]
CMD ["--mode", "full"]
