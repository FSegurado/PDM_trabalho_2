#!/usr/bin/env bash
# Liga o produtor publicando a temporada em andamento (2026-27) no Pub/Sub.
# Uso:  bash infra/04_run_producer.sh players_stats_final.csv [opções extras do publisher.py]
# Ex.:  bash infra/04_run_producer.sh players_stats_final.csv --delay 0.3 --bad-every 40
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/00_config.sh

CSV="${1:?informe o CSV do crawler}"
shift
use_venv
python3 producer/publisher.py --cache "${CSV}" --only-season 2627 --topic "${TOPIC}" "$@"
