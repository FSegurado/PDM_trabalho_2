"""
Produtor — replay do cache do crawler FBref para o Pub/Sub (parte 1, ajustado ao CSV real).

Responsabilidade única: ler o cache do crawler e publicar cada linha (um jogador) como
evento JSON, sem transformar os valores. Tipagem, limpeza e deduplicação são do pipeline.

Exemplos:
    # publica só a temporada em andamento, 1 jogador a cada 0,5 s
    python producer/publisher.py --cache players_stats_final.csv --only-season 2627

    # demo: repete o cache em loop e injeta 1 evento quebrado a cada 40 (para mostrar o dead-letter)
    python producer/publisher.py --cache players_stats_final.csv --only-season 2627 --loop --bad-every 40

    # sem GCP: gera um arquivo JSONL com as mesmas mensagens (para o teste local do pipeline)
    python producer/publisher.py --cache players_stats_final.csv --only-season 2627 --dry-run mensagens.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import date, datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ballon_common import get_project_id  # noqa: E402

TOPIC_ID = "fbref-player-stats-stream"
SCHEMA_VERSION = "1"
SOURCE = "fbref-crawler"


def read_cache(path: str) -> pd.DataFrame:
    """Lê o CSV como texto puro. Detecta o separador (o crawler grava com ';')."""
    with open(path, encoding="utf-8-sig") as fh:
        header = fh.readline()
    sep = ";" if header.count(";") > header.count(",") else ","
    return pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8-sig")


def build_event(row: dict, snapshot_date: str) -> dict:
    """Envelope de metadados + linha crua do crawler, sem alterar valores."""
    return {
        "event_id": str(uuid.uuid4()),
        "published_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_date": snapshot_date,  # dia em que o crawler coletou o dado
        "source": SOURCE,
        "schema_version": SCHEMA_VERSION,
        "payload": row,
    }


def broken_event() -> bytes:
    """Evento propositalmente inválido, para demonstrar o dead-letter na apresentação."""
    return json.dumps({"event_id": str(uuid.uuid4()), "payload": {
        "league": "ESP-La Liga", "season": "2627", "team": "Demo FC",
        "player": "Evento Quebrado", "Playing Time_Min": "noventa"}}).encode("utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay do cache do crawler FBref para o Pub/Sub")
    ap.add_argument("--cache", required=True, help="CSV gerado pelo crawler")
    ap.add_argument("--only-season", help="Publica só esta temporada (ex.: 2627)")
    ap.add_argument("--snapshot-date", help="Data da coleta (AAAA-MM-DD). Padrão: data do arquivo")
    ap.add_argument("--delay", type=float, default=0.5, help="Segundos entre eventos")
    ap.add_argument("--limit", type=int, help="Publica no máximo N eventos por passada")
    ap.add_argument("--loop", action="store_true", help="Republica o cache indefinidamente")
    ap.add_argument("--bad-every", type=int, default=0, help="Demo: injeta 1 evento inválido a cada N")
    ap.add_argument("--project", help="Projeto GCP (padrão: detectado automaticamente)")
    ap.add_argument("--topic", default=TOPIC_ID)
    ap.add_argument("--dry-run", metavar="ARQUIVO.jsonl", help="Não publica: grava as mensagens em arquivo")
    args = ap.parse_args()

    df = read_cache(args.cache)
    if args.only_season:
        df = df[df["season"].astype(str).str.strip() == args.only_season]
    if args.limit:
        df = df.head(args.limit)
    rows = df.to_dict(orient="records")

    snapshot = args.snapshot_date or date.fromtimestamp(os.path.getmtime(args.cache)).isoformat()

    if args.dry_run:
        with open(args.dry_run, "w", encoding="utf-8") as out:
            for i, row in enumerate(rows, start=1):
                data = json.dumps(build_event(row, snapshot), ensure_ascii=False)
                out.write(json.dumps({"data": data, "message_id": f"local-{i}",
                                      "attributes": {"season": str(row.get("season", ""))}},
                                     ensure_ascii=False) + "\n")
                if args.bad_every and i % args.bad_every == 0:
                    out.write(json.dumps({"data": broken_event().decode(), "message_id": f"local-bad-{i}",
                                          "attributes": {}}) + "\n")
        print(f"[dry-run] {len(rows)} mensagens gravadas em {args.dry_run}")
        return

    from google.cloud import pubsub_v1

    project = get_project_id(args.project)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, args.topic)
    print(f"[*] {len(rows)} jogadores -> {topic_path} (delay={args.delay}s, coleta={snapshot})")

    def report(future, label):
        exc = future.exception()
        if exc:
            print(f"[ERRO] {label}: {exc}")

    passada = 0
    try:
        while True:
            passada += 1
            for i, row in enumerate(rows, start=1):
                data = json.dumps(build_event(row, snapshot), ensure_ascii=False).encode("utf-8")
                fut = publisher.publish(topic_path, data=data, season=str(row.get("season", "")),
                                        source=SOURCE, schema_version=SCHEMA_VERSION)
                fut.add_done_callback(lambda f, lb=row.get("player"): report(f, lb))
                print(f"[Pub/Sub] #{passada}.{i} {row.get('player', '?')} ({row.get('team', '?')})")
                if args.bad_every and i % args.bad_every == 0:
                    publisher.publish(topic_path, data=broken_event(), source="demo-dead-letter")
                    print("[Pub/Sub] evento inválido injetado (demo do dead-letter)")
                time.sleep(args.delay)
            if not args.loop:
                break
    finally:
        publisher.stop()  # envia o que estiver pendente antes de sair
    print("[*] Streaming encerrado.")


if __name__ == "__main__":
    main()
