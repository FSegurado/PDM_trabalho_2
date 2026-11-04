"""
API REST do preditor Ballon d'Or (Trabalho 2 · Parte 4) — Cloud Run, privada (IAM).

Na subida, a API procura o modelo no Vertex AI Model Registry (display name em VERTEX_MODEL),
baixa o artefato apontado pelo registro (modelo BQML exportado em formato XGBoost) e os
arquivos de pré-processamento ao lado dele (média e desvio por temporada, iguais aos da Gold).

POST /predict recebe as estatísticas CRUAS de um ou mais jogadores, aplica o mesmo z-score por
temporada da camada Gold e o mesmo encoding do BQML (código do predictor exportado pelo próprio
BigQuery), e devolve o score previsto em JSON.
"""
from __future__ import annotations

import glob
import json
import math
import os
import tempfile
from typing import Any

import google.auth
import google.auth.transport.requests
import numpy as np
import requests
import xgboost as xgb
from fastapi import FastAPI, HTTPException
from google.cloud import storage
from pydantic import BaseModel, Field

RAW_FEATURES = [
    "goals_per90", "npg_per90", "xg_per90", "npxg_per90", "shots_per90", "sot_per90", "sot_pct",
    "g_minus_xg", "assists_per90", "xag_per90", "sca_per90", "gca_per90", "kp_per90", "ga_per90",
    "xg_xag_per90", "pass_cmp_pct", "prgc_per90", "prgp_per90", "prgr_per90", "take_on_succ_pct",
    "touches_att_pen_per90", "tkl_per90", "int_per90", "clr_per90", "blocks_per90", "recov_per90",
    "aerial_won_pct", "minutes", "age", "team_ppm", "team_ppm_on_pitch",
]
PASSTHROUGH = ["is_league_champion_proxy", "is_ucl_spot_proxy", "is_europe_spot_proxy", "is_gk"]
CATEGORICAL = ["pos_primary", "league"]

STATE: dict[str, Any] = {}
app = FastAPI(title="Preditor Ballon d'Or", version="1.0")


# ---------------------------------------------------------------- carga do modelo (startup)
def _gcs_download_prefix(gcs_uri: str, local_dir: str) -> list[str]:
    bucket_name, _, prefix = gcs_uri.removeprefix("gs://").partition("/")
    client = storage.Client()
    files = []
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        if blob.name.endswith("/"):
            continue
        dest = os.path.join(local_dir, os.path.relpath(blob.name, prefix))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        blob.download_to_filename(dest)
        files.append(dest)
    return files


def _registry_lookup(project: str, region: str, display_name: str) -> dict:
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(google.auth.transport.requests.Request())
    url = (f"https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}/models")
    r = requests.get(url, params={"filter": f'display_name="{display_name}"'},
                     headers={"Authorization": f"Bearer {creds.token}"}, timeout=30)
    r.raise_for_status()
    models = r.json().get("models", [])
    if not models:
        raise RuntimeError(f"modelo '{display_name}' não encontrado no Vertex AI Model Registry ({region})")
    m = sorted(models, key=lambda x: x.get("versionUpdateTime", ""))[-1]
    return {"name": m["name"], "display_name": m["displayName"], "version_id": m.get("versionId"),
            "artifact_uri": m["artifactUri"].rstrip("/") + "/"}


@app.on_event("startup")
def load_model() -> None:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or google.auth.default()[1]
    region = os.environ.get("VERTEX_REGION", "us-central1")
    info = _registry_lookup(project, region, os.environ.get("VERTEX_MODEL", "ballon-dor-btree"))
    work = tempfile.mkdtemp()
    model_dir, prep_dir = os.path.join(work, "model"), os.path.join(work, "prep")
    _gcs_download_prefix(info["artifact_uri"], model_dir)
    # pré-processamento fica ao lado do artefato: .../v1/model/ -> .../v1/preprocessing/
    prep_uri = info["artifact_uri"].rstrip("/").rsplit("/", 1)[0] + "/preprocessing/"
    _gcs_download_prefix(prep_uri, prep_dir)

    booster = xgb.Booster(model_file=os.path.join(model_dir, "model.bst"))
    with open(os.path.join(model_dir, "assets", "model_metadata.json")) as f:
        meta = json.load(f)
    label_vocab = {}
    for path in glob.glob(os.path.join(model_dir, "assets", "*_categorical_label.txt")):
        idx = int(os.path.basename(path).split("_")[0])
        with open(path) as f:
            label_vocab[idx] = f.read().splitlines()
    stats: dict[int, dict[str, tuple[float | None, float | None]]] = {}
    for path in glob.glob(os.path.join(prep_dir, "feature_stats_*")):
        with open(path) as f:
            for line in f:
                if line.strip():
                    r = json.loads(line)
                    stats.setdefault(int(r["season"]), {})[r["feature"]] = (r.get("mean"), r.get("std"))
    # BQML imputa NULL numérico com a média do treino (guardada no metadata do export)
    feature_means = {name: (meta["features"].get(name) or {}).get("mean") for name in meta["feature_names"]}
    STATE.update(model=booster, feature_names=meta["feature_names"], label_col=meta["label_col"],
                 label_vocab=label_vocab, stats=stats, registry=info, feature_means=feature_means)


# ---------------------------------------------------------------- pré-processamento
def _num(v: Any, mean: float | None) -> float:
    """NULL numérico -> média do treino (mesma imputação automática do BQML)."""
    if v is None or v == "":
        return float(mean) if mean is not None else math.nan
    return float(v)


def build_row(inst: dict, season: int) -> list[float]:
    """Mesma ordem de features e mesmo encoding do predictor exportado pelo BQML."""
    season_stats = STATE["stats"][season]
    values: dict[str, Any] = {}
    for f in RAW_FEATURES:            # z-score por temporada (igual à camada Gold)
        x = inst.get(f)
        mean, std = season_stats.get(f, (None, None))
        values[f + "_z"] = (None if x is None or mean is None or not std else (float(x) - mean) / std)
    for f in PASSTHROUGH + CATEGORICAL:
        values[f] = inst.get(f)
    row = []
    for i, name in enumerate(STATE["feature_names"]):
        v = values.get(name)
        if i in STATE["label_vocab"]:          # categorical_label: índice no vocabulário
            vocab = STATE["label_vocab"][i]
            if v is not None and str(v) in vocab:
                row.append(float(vocab.index(str(v))))
            else:                              # NULL/desconhecido -> categoria "_null_filler" do BQML
                row.append(float(vocab.index("_null_filler")) if "_null_filler" in vocab else math.nan)
        else:
            row.append(_num(v, STATE["feature_means"].get(name)))
    return row


# ---------------------------------------------------------------- endpoints
class Jogador(BaseModel):
    season: int = Field(2627, description="Temporada no código do T1 (2526 = 2025-26). Define a média/desvio do z-score.")
    league: str | None = Field(None, examples=["ESP-La Liga"])
    pos_primary: str | None = Field(None, examples=["FW"])
    minutes: float | None = Field(None, examples=[2500])
    age: float | None = Field(None, examples=[27])
    goals_per90: float | None = Field(None, examples=[0.8])
    assists_per90: float | None = Field(None, examples=[0.3])
    model_config = {"extra": "allow"}  # demais features cruas do T1 (xg_per90, kp_per90, ...)


class Pedido(BaseModel):
    instances: list[Jogador]


@app.get("/health")
def health() -> dict:
    return {"status": "ok" if "model" in STATE else "carregando"}


@app.get("/model")
def model_info() -> dict:
    return {"registro_vertex": STATE["registry"], "label": STATE["label_col"],
            "features": STATE["feature_names"], "temporadas_com_estatisticas": sorted(STATE["stats"])}


@app.post("/predict")
def predict(req: Pedido) -> dict:
    rows, seasons = [], []
    for j in req.instances:
        inst = j.model_dump()
        if j.season not in STATE["stats"]:
            raise HTTPException(422, f"temporada {j.season} sem estatísticas; use uma de {sorted(STATE['stats'])}")
        if inst.get("goals_per90") is not None and inst.get("assists_per90") is not None and inst.get("ga_per90") is None:
            inst["ga_per90"] = inst["goals_per90"] + inst["assists_per90"]
        if inst.get("is_gk") is None and inst.get("pos_primary") is not None:
            inst["is_gk"] = 1 if inst["pos_primary"] == "GK" else 0
        rows.append(build_row(inst, j.season))
        seasons.append(j.season)
    dm = xgb.DMatrix(np.array(rows, dtype=float), missing=np.nan)
    preds = STATE["model"].predict(dm).tolist()
    return {"modelo": {"vertex": STATE["registry"]["name"], "versao": STATE["registry"]["version_id"]},
            "predicoes": [{"season": s, f"predicted_{STATE['label_col']}": round(p, 4)}
                          for s, p in zip(seasons, preds)]}
