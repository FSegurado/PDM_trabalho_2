"""
Pipeline de processamento em tempo real — Trabalho 2, parte 2.

Pub/Sub  ──►  [A] bronze.player_stats_stream_raw     (evento bruto, como chegou)
          └─►  1 parse + schema
               2 seleção e tipagem      (T1 · passo 1)
               3 padronização de nomes  (T1 · passo 4)
               4 deduplicação
               ├─► silver.player_stats_stream          (evento tratado)
               └─► bronze.player_stats_stream_errors   (dead-letter)

Os passos que juntam várias linhas (transferências, contexto de time, por 90 e corte de
minutos) ficam na view silver.player_season_stream (sql/02_views.sql).

Execução no Dataflow: veja infra/02_run_pipeline.sh.
Execução local de teste (sem GCP): veja o README, seção "Teste local".
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.coders import BooleanCoder
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import window
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec

# permite rodar direto do repositório sem instalar o pacote (no Dataflow ele vem via setup.py)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ballon_common import get_project_id  # noqa: E402
from ballon_common.preprocessing import InvalidRecord, error_row, parse_event  # noqa: E402

ERRORS_TAG = "errors"
DEDUP_WINDOW_SECONDS = 24 * 60 * 60  # janela da deduplicação em memória: 1 dia


# --------------------------------------------------------------------------------------
# Normalização da mensagem de entrada (Pub/Sub no Dataflow, JSONL no teste local)
# --------------------------------------------------------------------------------------
def to_message(element) -> dict:
    """Uniformiza PubsubMessage e o formato do arquivo local em um dict simples."""
    if isinstance(element, (str, bytes)):  # linha do arquivo local
        obj = json.loads(element)
        data = obj.get("data", "")
        return {
            "data": data.encode("utf-8") if isinstance(data, str) else data,
            "attributes": obj.get("attributes") or {},
            "message_id": obj.get("message_id"),
            "publish_time": obj.get("publish_time"),
        }
    return {
        "data": element.data,
        "attributes": dict(element.attributes or {}),
        "message_id": getattr(element, "message_id", None),
        "publish_time": _iso_timestamp(getattr(element, "publish_time", None)),
    }


def _iso_timestamp(value):
    """publish_time pode chegar como datetime, Timestamp do protobuf ou nada."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "ToDatetime"):
        return value.ToDatetime(tzinfo=timezone.utc).isoformat()
    if hasattr(value, "seconds"):
        return datetime.fromtimestamp(value.seconds + getattr(value, "nanos", 0) / 1e9,
                                      tz=timezone.utc).isoformat()
    return None


def to_raw_row(msg: dict) -> dict:
    """[A] Linha da Bronze de streaming: o evento exatamente como chegou."""
    return {
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "message_id": msg["message_id"],
        "publish_time": msg["publish_time"],
        "attributes": json.dumps(msg["attributes"], ensure_ascii=False),
        "data": msg["data"].decode("utf-8", errors="replace") if msg["data"] is not None else None,
    }


# --------------------------------------------------------------------------------------
# Etapas 1 a 3: parse, tipagem e nomes (regras em ballon_common.preprocessing)
# --------------------------------------------------------------------------------------
class ParseEvent(beam.DoFn):
    def __init__(self):
        self.valid = Metrics.counter("silver", "eventos_validos")
        self.no_minutes = Metrics.counter("silver", "descartados_sem_minutos")
        self.invalid = Metrics.counter("dead_letter", "eventos_invalidos")

    def process(self, msg: dict):
        now = datetime.now(timezone.utc)
        try:
            row = parse_event(msg["data"], message_id=msg["message_id"], ingest_ts=now)
        except InvalidRecord as exc:
            self.invalid.inc()
            Metrics.counter("dead_letter", f"etapa_{exc.stage}").inc()
            yield beam.pvalue.TaggedOutput(
                ERRORS_TAG, error_row(exc.stage, exc.message, msg["data"], msg["message_id"], now))
            return
        except Exception as exc:  # noqa: BLE001 - erro inesperado também não derruba o job
            self.invalid.inc()
            yield beam.pvalue.TaggedOutput(
                ERRORS_TAG, error_row("inesperado", repr(exc), msg["data"], msg["message_id"], now))
            return

        if row is None:
            self.no_minutes.inc()
            return
        self.valid.inc()
        yield row


# --------------------------------------------------------------------------------------
# Etapa 4: deduplicação (estado por chave, dentro de janelas de 1 dia)
# --------------------------------------------------------------------------------------
class DropDuplicates(beam.DoFn):
    """Mantém só a primeira ocorrência de cada record_key na janela.

    Cobre reentrega do Pub/Sub (entrega "pelo menos uma vez") e o replay em loop do produtor.
    Duplicatas em janelas diferentes não causam erro: a view escolhe a última coleta.
    """

    SEEN = ReadModifyWriteStateSpec("seen", BooleanCoder())

    def __init__(self):
        self.duplicates = Metrics.counter("silver", "duplicatas_descartadas")

    def process(self, keyed, seen=beam.DoFn.StateParam(SEEN)):
        _, row = keyed
        if seen.read():
            self.duplicates.inc()
            return
        seen.write(True)
        yield row


class Deduplicate(beam.PTransform):
    def expand(self, rows):
        return (
            rows
            | "Janela1Dia" >> beam.WindowInto(window.FixedWindows(DEDUP_WINDOW_SECONDS))
            | "ChaveDedup" >> beam.Map(lambda r: (r["record_key"], r))
            | "RemoveDuplicatas" >> beam.ParDo(DropDuplicates())
        )


# --------------------------------------------------------------------------------------
# Saídas
# --------------------------------------------------------------------------------------
def bq_sink(table: str):
    from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
    from apache_beam.io.gcp.bigquery_tools import RetryStrategy

    return WriteToBigQuery(
        table,
        create_disposition=BigQueryDisposition.CREATE_NEVER,  # tabelas criadas por sql/01_tables.sql
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        method=WriteToBigQuery.Method.STREAMING_INSERTS,       # linha disponível para consulta em segundos
        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
    )


def failed_insert_to_error(failed) -> dict:
    """Linha que o BigQuery recusou (ex.: tipo incompatível) também vai para o dead-letter."""
    _, row, errors = failed if len(failed) == 3 else (None, failed[1], None)
    return error_row("bigquery", f"inserção recusada: {errors}",
                     json.dumps(row, ensure_ascii=False, default=str), row.get("message_id"))


def local_sink(path: str):
    return beam.Map(lambda r: json.dumps(r, ensure_ascii=False, default=str)) | beam.io.WriteToText(
        path, file_name_suffix=".jsonl", shard_name_template="")


# --------------------------------------------------------------------------------------
# Montagem do pipeline
# --------------------------------------------------------------------------------------
def build(p: beam.Pipeline, args) -> None:
    local = bool(args.input_file)

    if local:
        messages = (p | "LeArquivoLocal" >> beam.io.ReadFromText(args.input_file)
                      | "NormalizaMensagem" >> beam.Map(to_message))
    else:
        from apache_beam.io.gcp.pubsub import ReadFromPubSub

        messages = (p | "LePubSub" >> ReadFromPubSub(subscription=args.subscription_path,
                                                     with_attributes=True)
                      | "NormalizaMensagem" >> beam.Map(to_message))

    raw_rows = messages | "[A] EventoBruto" >> beam.Map(to_raw_row)

    parsed = messages | "1-3 ParseTipagemNomes" >> beam.ParDo(ParseEvent()).with_outputs(
        ERRORS_TAG, main="valid")
    silver_rows = parsed.valid | "4 Deduplicacao" >> Deduplicate()
    error_rows = parsed[ERRORS_TAG]

    if local:
        raw_rows | "GravaBrutoLocal" >> local_sink(os.path.join(args.output_dir, "bronze_raw"))
        silver_rows | "GravaSilverLocal" >> local_sink(os.path.join(args.output_dir, "silver"))
        error_rows | "GravaErrosLocal" >> local_sink(os.path.join(args.output_dir, "errors"))
        return

    raw_rows | "GravaBronzeBQ" >> bq_sink(args.table_raw)
    silver_result = silver_rows | "GravaSilverBQ" >> bq_sink(args.table_silver)
    insert_failures = silver_result.failed_rows_with_errors | "FalhasDeInsercao" >> beam.Map(
        failed_insert_to_error)
    ((error_rows, insert_failures)
     | "JuntaErros" >> beam.Flatten()
     | "GravaDeadLetterBQ" >> bq_sink(args.table_errors))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", help="Projeto GCP (padrão: detectado automaticamente)")
    parser.add_argument("--subscription", default="fbref-player-stats-dataflow",
                        help="Nome curto ou caminho completo da subscription do Pub/Sub")
    parser.add_argument("--bronze_dataset", default="bronze")
    parser.add_argument("--silver_dataset", default="silver")
    parser.add_argument("--input_file", help="Modo local: arquivo JSONL com mensagens de teste")
    parser.add_argument("--output_dir", default="saida_local", help="Modo local: pasta de saída")
    args, beam_args = parser.parse_known_args(argv)

    if not args.input_file:
        args.project = get_project_id(args.project)
        sub = args.subscription
        args.subscription_path = sub if sub.startswith("projects/") else \
            f"projects/{args.project}/subscriptions/{sub}"
        args.table_raw = f"{args.project}:{args.bronze_dataset}.player_stats_stream_raw"
        args.table_silver = f"{args.project}:{args.silver_dataset}.player_stats_stream"
        args.table_errors = f"{args.project}:{args.bronze_dataset}.player_stats_stream_errors"
        if not any(a.startswith("--project") for a in beam_args):
            beam_args += [f"--project={args.project}"]
    return args, beam_args


def run(argv=None) -> None:
    args, beam_args = parse_args(argv)
    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = False
    if not args.input_file:
        options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=options)
    build(p, args)
    result = p.run()
    if args.input_file:
        result.wait_until_finish()  # teste local: espera terminar
    else:
        # streaming no Dataflow: o job fica rodando na nuvem; o terminal é liberado
        logging.info("Job enviado. Acompanhe em https://console.cloud.google.com/dataflow/jobs?project=%s",
                     args.project)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
