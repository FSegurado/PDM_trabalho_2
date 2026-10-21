"""Testa o grafo do Beam (etapas 1 a 4 e o dead-letter) com o DirectRunner, sem GCP."""
import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipeline.streaming_pipeline import ERRORS_TAG, Deduplicate, ParseEvent, to_message


def mensagem(player, gols="7.0", msg_id="m"):
    data = json.dumps({"event_id": msg_id, "snapshot_date": "2026-10-20", "payload": {
        "league": "ESP-La Liga", "season": "2627", "team": "Barcelona", "player": player,
        "born_": "2007.0", "Playing Time_Min": "669.0", "Performance_Gls": gols}})
    return json.dumps({"data": data, "message_id": msg_id, "attributes": {}})


def test_pipeline_valida_deduplica_e_separa_erros():
    entrada = [
        mensagem("Lamine Yamal", msg_id="1"),
        mensagem("Lamine Yamal", msg_id="2"),            # reentrega/replay: duplicata
        mensagem("Lamine Yamal", gols="8.0", msg_id="3"),  # dado novo: não é duplicata
        mensagem("Raphinha", msg_id="4"),
        json.dumps({"data": "{quebrado", "message_id": "5", "attributes": {}}),
    ]
    with TestPipeline() as p:
        msgs = p | beam.Create(entrada) | beam.Map(to_message)
        parsed = msgs | beam.ParDo(ParseEvent()).with_outputs(ERRORS_TAG, main="valid")
        silver = parsed.valid | Deduplicate()

        assert_that(silver | "Gols" >> beam.Map(lambda r: (r["player"], r["goals"])),
                    equal_to([("Lamine Yamal", 7.0), ("Lamine Yamal", 8.0), ("Raphinha", 7.0)]),
                    label="silver")
        assert_that(parsed[ERRORS_TAG] | beam.Map(lambda r: (r["message_id"], r["error_stage"])),
                    equal_to([("5", "parse")]), label="dead_letter")
