"""
Regras de pré-processamento compartilhadas.

Este módulo é a fonte única das regras "linha a linha" da Silver do Trabalho 1
(passo 1: seleção e tipagem; passo 4: padronização de nomes). Ele é importado por:

  * o pipeline de streaming (Dataflow), em pipeline/streaming_pipeline.py;
  * o notebook da Silver batch, se o grupo quiser trocar as funções locais por estas;
  * futuramente, a API do Cloud Run.

Não depende de pandas nem de Beam: só biblioteca padrão, para rodar igual em qualquer lugar.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

# --------------------------------------------------------------------------------------
# Constantes de negócio (mesmas do Trabalho 1)
# --------------------------------------------------------------------------------------
LEAGUES = {
    "ENG-Premier League",
    "ESP-La Liga",
    "ITA-Serie A",
    "GER-Bundesliga",
    "FRA-Ligue 1",
}

# Nome da coluna no CSV do crawler (FBref) -> nome na Silver (mesmo RENAME do T1, §12)
NUMERIC_COLUMNS = {
    "Playing Time_MP": "mp",
    "Playing Time_Starts": "starts",
    "Playing Time_Min": "minutes",
    "Playing Time_90s": "nineties",
    "Performance_Gls": "goals",
    "Performance_Ast": "assists",
    "Performance_G-PK": "npg",
    "Performance_PK": "pk",
    "Performance_PKatt": "pk_att",
    "Performance_CrdY": "cards_y",
    "Performance_CrdR": "cards_r",
}

# Contagens que nunca podem ser negativas
NON_NEGATIVE = set(NUMERIC_COLUMNS.values())

# Campos sem os quais o evento não tem como ser usado -> dead-letter
REQUIRED_FIELDS = ["league", "season", "team", "player", "Playing Time_Min"]

# Particulas e transliteracoes: copiadas da Silver do Trabalho 1 (§6)
PARTICULAS = {"jr", "junior", "filho", "neto", "dos", "das", "da", "de", "do", "del", "della",
              "van", "von", "der", "den", "el", "al", "bin", "ibn", "the", "st"}
TRANSLIT = {"ø": "o", "ð": "d", "þ": "th", "ł": "l", "đ": "d", "æ": "ae",
            "œ": "oe", "ß": "ss", "ı": "i", "ħ": "h", "ŧ": "t"}


class InvalidRecord(ValueError):
    """Evento que não pode seguir para a Silver. Vai para a tabela de dead-letter."""

    def __init__(self, stage: str, message: str):
        super().__init__(message)
        self.stage = stage
        self.message = message


# --------------------------------------------------------------------------------------
# Passo 1 do T1 — tipagem
# --------------------------------------------------------------------------------------
_NON_NUMERIC = re.compile(r"[^0-9.\-]")


def to_number(value: Any) -> float | None:
    """Mesma regra de `to_numeric_eu` do T1: vírgula decimal vira ponto e lixo textual some.

    Diferença deliberada do streaming: no T1 um texto inválido virava NaN em silêncio.
    Aqui, um valor não vazio que não vira número levanta erro, e o evento vai para o
    dead-letter. Vazio continua virando nulo.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None
    cleaned = _NON_NUMERIC.sub("", s.replace(",", "."))
    if cleaned in {"", "-", ".", "-."}:
        raise ValueError(f"valor não numérico: {value!r}")
    try:
        return float(cleaned)
    except ValueError as exc:
        raise ValueError(f"valor não numérico: {value!r}") from exc


def parse_age(value: Any) -> float | None:
    """Idade do FBref: "27" (temporada encerrada) ou "29-008" (anos-dias, temporada em curso).

    O `to_numeric_eu` do T1 transformaria "29-008" em NaN; aqui ficam só os anos.
    """
    if value is None or str(value).strip() == "":
        return None
    s = str(value).strip()
    m = re.fullmatch(r"(\d{1,2})-(\d{1,3})", s)
    if m:
        return float(m.group(1))
    return to_number(s)


def parse_season(value: Any) -> int:
    """Código de temporada do T1: 2627 = 2026-27. Aceita também "2026-27" e "2026-2027"."""
    s = str(value).strip()
    m = re.fullmatch(r"(\d{2})(\d{2})", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
    else:
        m = re.fullmatch(r"(?:19|20)?(\d{2})\s*[-/]\s*(?:19|20)?(\d{2})", s)
        if not m:
            raise ValueError(f"temporada inválida: {value!r}")
        a, b = int(m.group(1)), int(m.group(2))
    if (a + 1) % 100 != b:
        raise ValueError(f"temporada inválida: {value!r}")
    return a * 100 + b


def season_label(season: int) -> str:
    return f"20{season // 100:02d}-{season % 100:02d}"


# --------------------------------------------------------------------------------------
# Passo 4 do T1 — padronização de nomes (cópia fiel da §6)
# --------------------------------------------------------------------------------------
def normalize_name(s: Any) -> str:
    """Minúsculas, sem acento, sem pontuação."""
    if s is None:
        return ""
    s = str(s).lower()
    for a, b in TRANSLIT.items():
        s = s.replace(a, b)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def core_name(s: Any) -> str:
    """Chave de casamento: sem partículas nem sufixos de parentesco."""
    toks = [t for t in normalize_name(s).split() if t not in PARTICULAS and len(t) > 1]
    return " ".join(toks)


def player_key(player: str, born_year: float | None) -> str:
    """Mesma chave do T1: `nome + "|" + ano de nascimento` (`<NA>` quando falta o ano)."""
    born = "<NA>" if born_year is None else str(int(born_year))
    return f"{player}|{born}"


# --------------------------------------------------------------------------------------
# Evento completo: envelope do produtor -> linha da Silver de streaming
# --------------------------------------------------------------------------------------
def _sha1(*parts: Any) -> str:
    return hashlib.sha1("|".join("" if p is None else str(p) for p in parts).encode("utf-8")).hexdigest()


def parse_event(data: bytes | str, message_id: str | None = None,
                ingest_ts: datetime | None = None) -> dict:
    """Recebe a mensagem do Pub/Sub e devolve a linha tratada da Silver de streaming.

    Levanta InvalidRecord (com a etapa que falhou) quando o evento deve ir para o dead-letter.
    Devolve None quando o evento é válido mas descartado pela regra do T1 (minutos = 0).
    """
    ingest_ts = ingest_ts or datetime.now(timezone.utc)

    # ---- etapa 1: parse + schema -------------------------------------------------------
    try:
        text = data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else str(data)
        envelope = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidRecord("parse", f"JSON inválido: {exc}") from exc
    if not isinstance(envelope, dict) or not isinstance(envelope.get("payload"), dict):
        raise InvalidRecord("parse", "evento sem o objeto 'payload'")

    payload = envelope["payload"]
    missing = [f for f in REQUIRED_FIELDS if str(payload.get(f, "") or "").strip() == ""]
    if missing:
        raise InvalidRecord("schema", f"campos obrigatórios vazios: {', '.join(missing)}")

    league = str(payload["league"]).strip()
    if league not in LEAGUES:
        raise InvalidRecord("schema", f"liga fora das 5 grandes: {league!r}")

    # ---- etapa 2: seleção e tipagem (T1 · passo 1) -------------------------------------
    try:
        season = parse_season(payload["season"])
        numbers = {dst: to_number(payload.get(src)) for src, dst in NUMERIC_COLUMNS.items()}
        age = parse_age(payload.get("age_"))
        born_year = to_number(payload.get("born_"))
    except ValueError as exc:
        raise InvalidRecord("tipagem", str(exc)) from exc

    negatives = [k for k in NON_NEGATIVE if numbers.get(k) is not None and numbers[k] < 0]
    if negatives:
        raise InvalidRecord("tipagem", f"valores negativos em: {', '.join(sorted(negatives))}")

    # saneamento do T1: jogador sem minutos não entra na Silver (não é erro)
    if not numbers["minutes"] or numbers["minutes"] <= 0:
        return None

    # ---- etapa 3: padronização de nomes (T1 · passo 4) ---------------------------------
    player = str(payload["player"]).strip()
    team = str(payload["team"]).strip()

    snapshot_date = str(envelope.get("snapshot_date") or "").strip() or None

    row = {
        "ingest_ts": ingest_ts.isoformat(),
        "event_id": envelope.get("event_id"),
        "message_id": message_id,
        "published_at": envelope.get("published_at"),
        "snapshot_date": snapshot_date,
        "source": envelope.get("source"),
        "schema_version": str(envelope.get("schema_version") or ""),
        "season": season,
        "season_label": season_label(season),
        "league": league,
        "team": team,
        "team_norm": normalize_name(team),
        "player": player,
        "player_norm": normalize_name(player),
        "player_core": core_name(player),
        "player_key": player_key(player, born_year),
        "nation": (str(payload.get("nation_") or "").strip() or None),
        "pos_raw": (str(payload.get("pos_") or "").strip() or None),
        "age": age,
        "born_year": born_year,
        **numbers,
    }

    # ---- etapa 4 (apoio): chaves de deduplicação ----------------------------------------
    # business_key: mesmo jogador, mesmo clube, mesma coleta.
    # record_key: business_key + conteúdo; idêntico em reentrega do Pub/Sub ou replay em loop.
    row["business_key"] = _sha1(season, league, team, row["player_key"], snapshot_date)
    row["record_key"] = _sha1(row["business_key"], *(numbers[k] for k in sorted(numbers)), age)
    return row


def error_row(stage: str, message: str, data: bytes | str | None,
              message_id: str | None = None, ingest_ts: datetime | None = None) -> dict:
    """Linha da tabela de dead-letter: guarda o evento original e o motivo."""
    ingest_ts = ingest_ts or datetime.now(timezone.utc)
    if isinstance(data, (bytes, bytearray)):
        raw = data.decode("utf-8", errors="replace")
    else:
        raw = None if data is None else str(data)
    return {
        "ingest_ts": ingest_ts.isoformat(),
        "message_id": message_id,
        "error_stage": stage,
        "error_message": message[:1000],
        "raw_data": raw,
    }
