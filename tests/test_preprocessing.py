import json

import pytest

from ballon_common.preprocessing import (
    InvalidRecord, core_name, normalize_name, parse_age, parse_event, parse_season, to_number)


def evento(**payload):
    base = {
        "league": "ESP-La Liga", "season": "2627", "team": "Real Madrid",
        "player": " Kylian Mbappé ", "nation_": "FRA", "pos_": "FW", "age_": "27-301",
        "born_": "1998.0", "Playing Time_MP": "8.0", "Playing Time_Min": "720.0",
        "Performance_Gls": "7.0", "Performance_Ast": "", "Performance_G-PK": "6.0",
    }
    base.update(payload)
    return json.dumps({"event_id": "e1", "snapshot_date": "2026-10-20", "payload": base})


# --- regras copiadas do T1 -------------------------------------------------------------
@pytest.mark.parametrize("nome, norm, core", [
    ("Vinícius Júnior", "vinicius junior", "vinicius"),
    ("Simon Kjær", "simon kjaer", "simon kjaer"),
    ("Martin Ødegaard", "martin odegaard", "martin odegaard"),
    ("Rodrigo De Paul", "rodrigo de paul", "rodrigo paul"),
    ("N'Golo Kanté", "n golo kante", "golo kante"),
])
def test_nomes_iguais_ao_t1(nome, norm, core):
    assert normalize_name(nome) == norm
    assert core_name(nome) == core


def test_tipagem_virgula_decimal_e_vazio():
    assert to_number("6,1") == 6.1
    assert to_number("1997.0") == 1997.0
    assert to_number("") is None
    with pytest.raises(ValueError):
        to_number("noventa")


def test_idade_formato_anos_dias():
    assert parse_age("29-008") == 29.0
    assert parse_age("27") == 27.0
    assert parse_age("") is None


def test_temporada():
    assert parse_season("2627") == 2627
    assert parse_season("2026-27") == 2627
    with pytest.raises(ValueError):
        parse_season("2628")


# --- evento completo ---------------------------------------------------------------------
def test_evento_valido():
    row = parse_event(evento())
    assert row["player"] == "Kylian Mbappé"
    assert row["player_key"] == "Kylian Mbappé|1998"
    assert row["player_core"] == "kylian mbappe"
    assert row["minutes"] == 720.0 and row["assists"] is None
    assert row["age"] == 27.0 and row["season_label"] == "2026-27"


def test_replay_gera_mesma_chave_de_dedup():
    a = parse_event(evento())
    b = parse_event(evento().replace('"e1"', '"outro-id"'))
    assert a["record_key"] == b["record_key"]
    c = parse_event(evento(**{"Performance_Gls": "8.0"}))  # dado mudou: não é duplicata
    assert c["record_key"] != a["record_key"]


@pytest.mark.parametrize("dados, etapa", [
    (b"isso nao e json", "parse"),
    (json.dumps({"sem": "payload"}), "parse"),
    (evento(player=""), "schema"),
    (evento(league="BRA-Serie A"), "schema"),
    (evento(**{"Playing Time_Min": "noventa"}), "tipagem"),
    (evento(**{"Performance_Gls": "-1"}), "tipagem"),
    (evento(season="2628"), "tipagem"),
])
def test_invalidos_vao_para_dead_letter(dados, etapa):
    with pytest.raises(InvalidRecord) as exc:
        parse_event(dados)
    assert exc.value.stage == etapa


def test_sem_minutos_e_descartado_como_no_t1():
    assert parse_event(evento(**{"Playing Time_Min": "0"})) is None
