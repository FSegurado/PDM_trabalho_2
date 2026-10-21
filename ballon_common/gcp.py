"""Descobre o ID do projeto GCP automaticamente, sem deixar o ID fixo no código."""
from __future__ import annotations

import os
import shutil
import subprocess

_ENV_VARS = ("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "CLOUDSDK_CORE_PROJECT", "GCP_PROJECT")


def get_project_id(explicit: str | None = None) -> str:
    """Ordem de busca:

    1. valor passado explicitamente (ex.: argumento --project);
    2. variáveis de ambiente (Cloud Shell, Cloud Run e BigQuery Studio já definem uma delas);
    3. credenciais padrão do Google (`google.auth.default()`);
    4. projeto ativo do gcloud (`gcloud config get-value project`).
    """
    if explicit:
        return explicit

    for var in _ENV_VARS:
        if os.environ.get(var):
            return os.environ[var]

    try:
        import google.auth  # import tardio: só existe onde há SDK do Google

        _, project = google.auth.default()
        if project:
            return project
    except Exception:  # noqa: BLE001 - qualquer falha aqui só passa para a próxima opção
        pass

    gcloud = shutil.which("gcloud")
    if gcloud:
        try:
            out = subprocess.run([gcloud, "config", "get-value", "project"],
                                 capture_output=True, text=True, timeout=20)
            project = out.stdout.strip()
            if project and project != "(unset)":
                return project
        except Exception:  # noqa: BLE001
            pass

    raise RuntimeError(
        "Não encontrei o projeto GCP. Rode `gcloud config set project SEU_PROJETO` "
        "ou defina a variável de ambiente GOOGLE_CLOUD_PROJECT."
    )
