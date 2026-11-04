"""Testes das partes 3 e 4 (rodam sem GCP; pulam se a dependência não estiver instalada)."""
import math
import sys
import types

import pandas as pd
import pytest


def test_job_normaliza_colunas_do_crawler():
    pytest.importorskip("google.cloud.pubsub_v1")
    pytest.importorskip("google.cloud.storage")
    from job.run_job import normalize_columns

    df = pd.DataFrame(columns=["league", "season", "team", "player", "nation", "pos", "age", "born", "Playing Time_Min"])
    out = normalize_columns(df)
    assert {"nation_", "pos_", "age_", "born_"} <= set(out.columns)
    assert "nation" not in out.columns and "Playing Time_Min" in out.columns


def test_job_nao_duplica_colunas_ja_corretas():
    pytest.importorskip("google.cloud.pubsub_v1")
    pytest.importorskip("google.cloud.storage")
    from job.run_job import normalize_columns

    df = pd.DataFrame(columns=["player", "nation_", "pos_"])
    assert list(normalize_columns(df).columns) == ["player", "nation_", "pos_"]


@pytest.fixture
def api_state():
    pytest.importorskip("xgboost")
    pytest.importorskip("fastapi")
    pytest.importorskip("google.cloud.storage")
    from api import main

    main.STATE.clear()
    main.STATE.update(
        feature_names=["goals_per90_z", "minutes_z", "is_gk", "pos_primary", "league"],
        label_vocab={3: ["_null_filler", "DF", "FW", "GK", "MF"],
                     4: ["_null_filler", "ENG-Premier League", "ESP-La Liga"]},
        stats={2526: {"goals_per90": (0.2, 0.1), "minutes": (2000.0, 500.0)}},
        feature_means={"goals_per90_z": 0.0, "minutes_z": 0.05, "is_gk": 0.07},
    )
    return main


def test_api_zscore_por_temporada(api_state):
    row = api_state.build_row({"goals_per90": 0.5, "minutes": 2500, "is_gk": 0,
                               "pos_primary": "FW", "league": "ESP-La Liga"}, 2526)
    assert row == pytest.approx([3.0, 1.0, 0.0, 2.0, 2.0])


def test_api_imputa_nulos_como_o_bqml(api_state):
    row = api_state.build_row({"pos_primary": None, "league": "Liga Desconhecida"}, 2526)
    # numérico NULL -> média do treino; categoria NULL/desconhecida -> "_null_filler" (índice 0)
    assert row == pytest.approx([0.0, 0.05, 0.07, 0.0, 0.0])
    assert not any(math.isnan(v) for v in row)
