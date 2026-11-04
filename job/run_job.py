"""
Cloud Run Job do produtor (Trabalho 2 · Parte 3).

Roda uma vez por dia (Cloud Scheduler) e faz o papel do "Publisher Python" do diagrama:
  1. lê o snapshot mais recente do crawler no Cloud Storage (cache CSV por temporada);
  2. ajusta só o NOME das colunas de identificação (nation/pos/age/born -> nation_/pos_/age_/born_),
     porque versões novas do soccerdata não trazem o sufixo "_" que o pipeline espera;
  3. publica 1 evento JSON por jogador no Pub/Sub, sem transformar valores (mesmo envelope do
     producer/publisher.py). Tipagem, limpeza e dedup continuam no Dataflow.

O crawler do FBref em si NÃO roda aqui: ele depende de um navegador que contorna a proteção
anti-bot do site, o que não é apropriado (nem confiável) a partir de IPs de nuvem. O crawler
roda fora e grava o snapshot em gs://<CACHE_BUCKET>/<CACHE_PREFIX>players_stats_<SEASON>.csv.

Variáveis de ambiente:
  CACHE_BUCKET   bucket do cache (padrão: futdados)
  CACHE_PREFIX   prefixo do cache (padrão: cache/)
  SEASON         temporada publicada (padrão: 2627)
  TOPIC          tópico do Pub/Sub (padrão: fbref-player-stats-stream)
  DELAY          segundos entre eventos (padrão: 0.5)
  LIMIT          máximo de eventos (opcional, para teste)
"""
from __future__ import annotations

import io
import json
import os
import sys
import time

import pandas as pd
from google.cloud import pubsub_v1, storage

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ballon_common import get_project_id  # noqa: E402
from producer.publisher import SCHEMA_VERSION, SOURCE, build_event  # noqa: E402

RENAME_ID_COLUMNS = {"nation": "nation_", "pos": "pos_", "age": "age_", "born": "born_"}


def load_snapshot(bucket_name: str, blob_name: str) -> tuple[pd.DataFrame, str]:
    blob = storage.Client().bucket(bucket_name).get_blob(blob_name)
    if blob is None:
        raise SystemExit(f"[ERRO] snapshot não encontrado: gs://{bucket_name}/{blob_name}")
    text = blob.download_as_text(encoding="utf-8-sig")
    header = text.split("\n", 1)[0]
    sep = ";" if header.count(";") > header.count(",") else ","
    df = pd.read_csv(io.StringIO(text), sep=sep, dtype=str, keep_default_na=False)
    # data da coleta: metadado "snapshot_date" (se o crawler gravou) ou a data do upload
    snapshot = (blob.metadata or {}).get("snapshot_date") or blob.updated.date().isoformat()
    return df, snapshot


def main() -> None:
    bucket = os.environ.get("CACHE_BUCKET", "futdados")
    prefix = os.environ.get("CACHE_PREFIX", "cache/")
    season = os.environ.get("SEASON", "2627")
    topic = os.environ.get("TOPIC", "fbref-player-stats-stream")
    delay = float(os.environ.get("DELAY", "0.5"))
    limit = int(os.environ["LIMIT"]) if os.environ.get("LIMIT") else None

    blob_name = f"{prefix}players_stats_{season}.csv"
    df, snapshot = load_snapshot(bucket, blob_name)
    df = df.rename(columns={k: v for k, v in RENAME_ID_COLUMNS.items() if k in df.columns and v not in df.columns})
    df = df[df["season"].astype(str).str.strip() == season]
    if limit:
        df = df.head(limit)
    rows = df.to_dict(orient="records")
    if not rows:
        raise SystemExit(f"[ERRO] nenhum jogador da temporada {season} em gs://{bucket}/{blob_name}")

    project = get_project_id()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic)
    print(json.dumps({"msg": "inicio", "snapshot": f"gs://{bucket}/{blob_name}", "coleta": snapshot,
                      "jogadores": len(rows), "topico": topic_path, "delay": delay}))

    futures, erros = [], 0
    for i, row in enumerate(rows, start=1):
        data = json.dumps(build_event(row, snapshot), ensure_ascii=False).encode("utf-8")
        futures.append(publisher.publish(topic_path, data=data, season=season,
                                         source=SOURCE, schema_version=SCHEMA_VERSION))
        if i % 250 == 0:
            print(json.dumps({"msg": "progresso", "publicados": i}))
        time.sleep(delay)
    for f in futures:
        try:
            f.result(timeout=60)
        except Exception as exc:  # noqa: BLE001
            erros += 1
            print(json.dumps({"msg": "erro_publicacao", "erro": str(exc)[:300]}))
    print(json.dumps({"msg": "fim", "publicados": len(rows) - erros, "erros": erros}))
    if erros:
        sys.exit(1)


if __name__ == "__main__":
    main()
