FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY nest_logger.py .
COPY web/ web/
ENV PYTHONUNBUFFERED=1
