#!/usr/bin/env bash
# Configuração comum a todos os scripts. Nenhum ID de projeto fixo:
# o projeto vem do gcloud (`gcloud config set project ...`) ou da variável PROJECT_ID.

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "ERRO: nenhum projeto ativo. Rode: gcloud config set project SEU_PROJETO" >&2
  return 1 2>/dev/null || exit 1
fi
export PROJECT_ID
export REGION="${REGION:-us-central1}"          # região do Dataflow
export BQ_LOCATION="${BQ_LOCATION:-US}"         # multirregião dos datasets (a mesma do T1)

export TOPIC="fbref-player-stats-stream"
export SUBSCRIPTION="fbref-player-stats-dataflow"
export BUCKET="gs://${PROJECT_ID}-dataflow"
export SA_NAME="dataflow-streaming"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export JOB_NAME="ballon-dor-streaming"

echo "projeto=${PROJECT_ID} região=${REGION} bigquery=${BQ_LOCATION}"

# Ambiente Python isolado (o Python do Cloud Shell não aceita pip install global)
use_venv() {
  if [[ ! -d .venv ]]; then
    echo "Criando ambiente Python (.venv) e instalando dependências (só na primeira vez)..."
    python3 -m venv .venv
    .venv/bin/pip install --quiet --upgrade pip
    .venv/bin/pip install --quiet -r requirements.txt
  fi
  # shellcheck disable=SC1091
  source .venv/bin/activate
}
