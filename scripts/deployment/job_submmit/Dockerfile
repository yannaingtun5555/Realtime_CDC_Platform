FROM python:3.11-slim

WORKDIR /app

ENV http_proxy=""
ENV https_proxy=""
ENV HTTP_PROXY=""
ENV HTTPS_PROXY=""

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY job_submitter.py .

CMD ["python", "job_submitter.py"]