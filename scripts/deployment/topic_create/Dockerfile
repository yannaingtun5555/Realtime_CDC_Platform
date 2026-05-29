FROM python:3.11-slim

# The --proxy="" flag tells pip to ignore system proxy settings
RUN pip install --proxy="" kafka-python -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY topic_creation4jobs.py /topic_creation4jobs.py
CMD ["python", "/topic_creation4jobs.py"]
