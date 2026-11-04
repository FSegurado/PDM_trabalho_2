#!/usr/bin/env bash
# Partes 3 e 4: produtor no Cloud Run Job + Cloud Scheduler, modelo no Vertex AI Model Registry
# e API REST privada no Cloud Run. Pode rodar mais de uma vez (o que já existe é atualizado).
# Uso (no Cloud Shell, na raiz do repositório):  bash infra/05_deploy_partes_3_4.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/00_config.sh

AR_REPO="ballon-dor"
TAG="$(date +%Y%m%d-%H%M%S)"
JOB_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/producer-job:${TAG}"
API_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/ballon-dor-api:${TAG}"
JOB_NAME_RUN="producer-fbref"
API_NAME="ballon-dor-api"
SCHED_NAME="producer-fbref-diario"
MODEL_NAME="ballon-dor-btree"
MODEL_GCS="gs://futdados/models/ballon_dor_btree/v1"
SA_JOB="producer-job@${PROJECT_ID}.iam.gserviceaccount.com"
SA_API="ballon-dor-api@${PROJECT_ID}.iam.gserviceaccount.com"
SA_SCHED="scheduler-invoker@${PROJECT_ID}.iam.gserviceaccount.com"

echo "== 1/7 APIs"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com aiplatform.googleapis.com --project "${PROJECT_ID}"

echo "== 2/7 Artifact Registry"
gcloud artifacts repositories describe "${AR_REPO}" --location "${REGION}" >/dev/null 2>&1 \
  || gcloud artifacts repositories create "${AR_REPO}" --location "${REGION}" --repository-format docker

echo "== 3/7 Contas de serviço (mínimo necessário)"
mk_sa() { gcloud iam service-accounts describe "$1@${PROJECT_ID}.iam.gserviceaccount.com" >/dev/null 2>&1 \
  || gcloud iam service-accounts create "$1" --display-name "$2"; }
mk_sa producer-job "Produtor FBref (Cloud Run Job)"
mk_sa ballon-dor-api "API Ballon d'Or (Cloud Run)"
mk_sa scheduler-invoker "Cloud Scheduler -> Cloud Run Job"
bind() { gcloud projects add-iam-policy-binding "${PROJECT_ID}" --member "serviceAccount:$1" \
  --role "$2" --condition None --quiet >/dev/null; echo "   $1 -> $2"; }
bind "${SA_JOB}" roles/pubsub.publisher
bind "${SA_JOB}" roles/storage.objectViewer
bind "${SA_API}" roles/aiplatform.viewer
bind "${SA_API}" roles/storage.objectViewer
bind "${SA_SCHED}" roles/run.invoker

echo "== 4/7 Vertex AI Model Registry (sem endpoint: não cobra por hora)"
if ! gcloud storage ls "${MODEL_GCS}/model/model.bst" >/dev/null 2>&1; then
  echo "   exportando o modelo do BigQuery (sql/08_export_modelo.sql)"
  bq --location "${BQ_LOCATION}" --project_id "${PROJECT_ID}" query --use_legacy_sql=false < sql/08_export_modelo.sql
fi
if [[ -z "$(gcloud ai models list --region "${REGION}" --filter "displayName=${MODEL_NAME}" --format 'value(name)' 2>/dev/null)" ]]; then
  gcloud ai models upload --region "${REGION}" --display-name "${MODEL_NAME}" \
    --artifact-uri "${MODEL_GCS}/model/" \
    --container-image-uri "us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest" \
    --description "BQML BOOSTED_TREE_REGRESSOR gold.model_btree_ballon_dor exportado (treinado so no historico)." \
    --labels origem=bqml,trabalho=pdm
else
  echo "   modelo ${MODEL_NAME} já registrado"
fi

echo "== 5/7 Imagens (Cloud Build)"
gcloud builds submit --config job/cloudbuild.yaml --substitutions "_IMAGE=${JOB_IMAGE}" .
gcloud builds submit --config api/cloudbuild.yaml --substitutions "_IMAGE=${API_IMAGE}" .

echo "== 6/7 Cloud Run Job + Cloud Scheduler"
gcloud run jobs deploy "${JOB_NAME_RUN}" --region "${REGION}" --image "${JOB_IMAGE}" \
  --service-account "${SA_JOB}" --task-timeout 3600 --max-retries 1 --memory 1Gi \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CACHE_BUCKET=futdados,CACHE_PREFIX=cache/,SEASON=2627,TOPIC=${TOPIC},DELAY=0.5"
JOB_URI="https://run.googleapis.com/v2/projects/${PROJECT_ID}/locations/${REGION}/jobs/${JOB_NAME_RUN}:run"
if gcloud scheduler jobs describe "${SCHED_NAME}" --location "${REGION}" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "${SCHED_NAME}" --location "${REGION}" --schedule "17 6 * * *" \
    --time-zone "America/Sao_Paulo" --uri "${JOB_URI}" --http-method POST --oauth-service-account-email "${SA_SCHED}"
else
  gcloud scheduler jobs create http "${SCHED_NAME}" --location "${REGION}" --schedule "17 6 * * *" \
    --time-zone "America/Sao_Paulo" --uri "${JOB_URI}" --http-method POST --oauth-service-account-email "${SA_SCHED}"
fi

echo "== 7/7 API REST privada (Cloud Run)"
gcloud run deploy "${API_NAME}" --region "${REGION}" --image "${API_IMAGE}" \
  --service-account "${SA_API}" --no-allow-unauthenticated --memory 1Gi --min-instances 0 --max-instances 2 \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},VERTEX_REGION=${REGION},VERTEX_MODEL=${MODEL_NAME}"

URL="$(gcloud run services describe "${API_NAME}" --region "${REGION}" --format 'value(status.url)')"
echo
echo "Pronto."
echo "  Job:        gcloud run jobs execute ${JOB_NAME_RUN} --region ${REGION}"
echo "  Scheduler:  todo dia 06:17 (America/Sao_Paulo)"
echo "  API:        curl -H \"Authorization: Bearer \$(gcloud auth print-identity-token)\" ${URL}/health"
