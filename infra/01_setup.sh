#!/usr/bin/env bash
# Cria tudo o que a parte 2 precisa. Pode rodar mais de uma vez: o que já existe é mantido.
# Uso (no Cloud Shell, na raiz do repositório):  bash infra/01_setup.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/00_config.sh

echo "== 1/6 Ativando APIs"
gcloud services enable dataflow.googleapis.com compute.googleapis.com pubsub.googleapis.com \
  bigquery.googleapis.com storage.googleapis.com --project "${PROJECT_ID}"

echo "== 2/6 Tópico e subscription do Pub/Sub"
gcloud pubsub topics describe "${TOPIC}" --project "${PROJECT_ID}" >/dev/null 2>&1 \
  || gcloud pubsub topics create "${TOPIC}" --project "${PROJECT_ID}"
# a subscription precisa existir ANTES do produtor publicar: mensagens anteriores a ela se perdem
gcloud pubsub subscriptions describe "${SUBSCRIPTION}" --project "${PROJECT_ID}" >/dev/null 2>&1 \
  || gcloud pubsub subscriptions create "${SUBSCRIPTION}" --project "${PROJECT_ID}" \
       --topic "${TOPIC}" --ack-deadline 60 --message-retention-duration 1d

echo "== 3/6 Bucket temporário do Dataflow"
gcloud storage buckets describe "${BUCKET}" >/dev/null 2>&1 \
  || gcloud storage buckets create "${BUCKET}" --project "${PROJECT_ID}" --location "${REGION}" \
       --uniform-bucket-level-access

echo "== 4/6 Conta de serviço do job e permissões"
gcloud iam service-accounts describe "${SA_EMAIL}" --project "${PROJECT_ID}" >/dev/null 2>&1 \
  || gcloud iam service-accounts create "${SA_NAME}" --project "${PROJECT_ID}" \
       --display-name "Dataflow streaming Ballon d'Or"
for role in roles/dataflow.worker roles/pubsub.subscriber roles/pubsub.viewer \
            roles/bigquery.dataEditor roles/storage.objectAdmin; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" --member "serviceAccount:${SA_EMAIL}" \
    --role "${role}" --condition None --quiet >/dev/null
  echo "   ${role}"
done

echo "== 5/6 Tabelas e views do streaming no BigQuery"
bq --location "${BQ_LOCATION}" --project_id "${PROJECT_ID}" query --use_legacy_sql=false < sql/01_tables.sql
bq --location "${BQ_LOCATION}" --project_id "${PROJECT_ID}" query --use_legacy_sql=false < sql/02_views.sql

echo "== 6/6 View híbrida batch + streaming"
if bq --project_id "${PROJECT_ID}" show silver.player_season >/dev/null 2>&1; then
  bq --location "${BQ_LOCATION}" --project_id "${PROJECT_ID}" query --use_legacy_sql=false < sql/04_hybrid_view.sql
else
  echo "   silver.player_season (Silver batch do T1) ainda não existe neste projeto."
  echo "   Rode os notebooks do T1 e depois: bq query --use_legacy_sql=false < sql/04_hybrid_view.sql"
fi

echo
echo "Pronto. Próximo passo: bash infra/02_run_pipeline.sh"
