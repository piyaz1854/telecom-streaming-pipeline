FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sink.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "sink.py"]
