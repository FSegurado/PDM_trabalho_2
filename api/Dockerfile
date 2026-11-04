# API REST privada (Cloud Run). Build a partir da RAIZ do repositório:
#   gcloud builds submit --config api/cloudbuild.yaml .
FROM python:3.11-slim
WORKDIR /app
COPY api/requirements.txt api/requirements.txt
RUN pip install --no-cache-dir -r api/requirements.txt
COPY api/main.py api/main.py
ENV PYTHONUNBUFFERED=1
CMD exec uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8080}
