FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming/ ./streaming/
COPY spark/ ./spark/
COPY tests/ ./tests/

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "spark.metrics_api:app", "--host", "0.0.0.0", "--port", "8000"]