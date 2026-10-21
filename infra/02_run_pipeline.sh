#!/usr/bin/env bash
# Sobe o job de streaming no Dataflow. Ele fica ligado (e cobrando) até rodar 03_drain.sh.
# Leva de 3 a 5 minutos para os workers ficarem prontos: ligue antes da apresentação.
# Uso:  bash infra/02_run_pipeline.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/00_config.sh

use_venv

python3 pipeline/streaming_pipeline.py \
  --runner DataflowRunner \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --job_name "${JOB_NAME}" \
  --subscription "${SUBSCRIPTION}" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --service_account_email "${SA_EMAIL}" \
  --setup_file ./setup.py \
  --enable_streaming_engine \
  --num_workers 1 \
  --max_num_workers 1 \
  --worker_machine_type e2-standard-2

echo
echo "Job enviado: https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}"
echo "Quando o grafo aparecer como 'Running', ligue o produtor: bash infra/04_run_producer.sh <csv>"
