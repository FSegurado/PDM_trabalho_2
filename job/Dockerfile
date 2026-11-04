# Cloud Run Job do produtor. Build a partir da RAIZ do repositório:
#   gcloud builds submit --config job/cloudbuild.yaml .
FROM python:3.12-slim
WORKDIR /app
COPY job/requirements.txt job/requirements.txt
RUN pip install --no-cache-dir -r job/requirements.txt
COPY ballon_common/ ballon_common/
COPY producer/publisher.py producer/publisher.py
COPY job/run_job.py job/run_job.py
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "job/run_job.py"]
