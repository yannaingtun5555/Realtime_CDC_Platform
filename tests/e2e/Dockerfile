FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY test_live_pipeline.py .

CMD ["python", "-u", "test_live_pipeline.py"]
