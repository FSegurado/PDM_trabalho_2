#!/usr/bin/env bash
# Desliga o job de streaming com segurança: termina o que já está em processamento e para.
# Rode sempre depois dos testes e da apresentação, para não continuar pagando os workers.
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/00_config.sh

JOB_ID="$(gcloud dataflow jobs list --project "${PROJECT_ID}" --region "${REGION}" \
  --status active --filter "name=${JOB_NAME}" --format "value(id)" | head -n1)"

if [[ -z "${JOB_ID}" ]]; then
  echo "Nenhum job ativo chamado ${JOB_NAME}."
  exit 0
fi
gcloud dataflow jobs drain "${JOB_ID}" --project "${PROJECT_ID}" --region "${REGION}"
echo "Drain solicitado para ${JOB_ID}. O job aparece como 'Drained' em alguns minutos."
