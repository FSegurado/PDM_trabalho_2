"""Pacote comum do projeto Ballon d'Or: regras compartilhadas entre batch, streaming e API."""
from .gcp import get_project_id  # noqa: F401
from .preprocessing import (  # noqa: F401
    InvalidRecord,
    core_name,
    error_row,
    normalize_name,
    parse_age,
    parse_event,
    parse_season,
    player_key,
    season_label,
    to_number,
)
