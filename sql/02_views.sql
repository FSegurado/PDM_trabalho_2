-- =====================================================================================
-- Trabalho 2 · Parte 2 · Views da Silver de streaming
--
-- O Dataflow faz o que cabe evento a evento (T1 · passos 1 e 4 + deduplicação).
-- Estas views fazem o que precisa juntar várias linhas, com as MESMAS regras da Silver do T1:
--   passo 2  transferências: soma das contagens, clube principal = mais minutos
--   passo 6  métricas por 90 recalculadas após consolidar + corte de minutos
--   passo 3  contexto de time: o crawler NÃO traz `Team Success_PPM`, então as colunas
--            ficam nulas (documentado no README)
--   passos 5 e 7 (prêmio e target): não se aplicam à temporada em andamento
--
-- Rodar com:  bq query --use_legacy_sql=false < sql/02_views.sql
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- 1) Todas as linhas consolidadas, com a marcação de qualificação (para auditoria/demo)
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.player_season_stream_all AS
WITH
ultima_coleta AS (            -- a foto mais recente de cada jogador em cada clube
  SELECT *
  FROM silver.player_stats_stream
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY season, league, team, player_key
    ORDER BY snapshot_date DESC, ingest_ts DESC) = 1
),
rodadas AS (                  -- andamento de cada liga, para o corte proporcional
  SELECT
    season, league,
    MAX(mp) AS rodadas_jogadas,
    IF(league IN ('GER-Bundesliga', 'FRA-Ligue 1'), 34, 38) AS rodadas_total
  FROM ultima_coleta
  GROUP BY season, league
),
consolidado AS (              -- passo 2: 1 linha por jogador-temporada
  SELECT
    season, player_key,
    ARRAY_AGG(STRUCT(player, player_norm, nation, pos_raw, age, born_year, team, league)
              ORDER BY minutes DESC LIMIT 1)[OFFSET(0)] AS principal,
    COUNT(*)        AS n_clubs,
    SUM(mp)         AS mp,
    SUM(starts)     AS starts,
    SUM(minutes)    AS minutes,
    SUM(nineties)   AS nineties,
    SUM(goals)      AS goals,
    SUM(assists)    AS assists,
    SUM(npg)        AS npg,
    SUM(pk)         AS pk,
    SUM(pk_att)     AS pk_att,
    SUM(cards_y)    AS cards_y,
    SUM(cards_r)    AS cards_r,
    MAX(snapshot_date) AS ultima_coleta,
    MAX(ingest_ts)     AS ultimo_evento
  FROM ultima_coleta
  GROUP BY season, player_key
),
base AS (
  SELECT
    c.*,
    r.rodadas_jogadas,
    r.rodadas_total,
    -- passo 6: o corte de 900 min do T1, proporcional às rodadas já disputadas na liga
    900 * SAFE_DIVIDE(r.rodadas_jogadas, r.rodadas_total) AS corte_minutos,
    NULLIF(c.minutes / 90, 0) AS n90
  FROM consolidado c
  JOIN rodadas r
    ON r.season = c.season AND r.league = c.principal.league
)
SELECT
  -- identificação (mesmos nomes e ordem da silver.player_season do T1) -------------
  season,
  CONCAT('20', LPAD(CAST(DIV(season, 100) AS STRING), 2, '0'), '-',
         LPAD(CAST(MOD(season, 100) AS STRING), 2, '0')) AS season_label,
  MOD(season, 100) + 2000                       AS award_year,
  principal.league                              AS league,
  player_key,
  principal.player                              AS player,
  principal.player_norm                         AS player_norm,
  principal.nation                              AS nation,
  principal.pos_raw                             AS pos_raw,
  SPLIT(COALESCE(principal.pos_raw, 'NA'), ',')[OFFSET(0)] AS pos_primary,
  IF(SPLIT(COALESCE(principal.pos_raw, 'NA'), ',')[OFFSET(0)] = 'GK', 1, 0) AS is_gk,
  principal.age                                 AS age,
  principal.born_year                           AS born_year,
  principal.team                                AS club_main,
  n_clubs,
  -- passo 3: contexto de time indisponível no crawler (sem Team Success_PPM) ---------
  CAST(NULL AS FLOAT64) AS team_ppm,
  CAST(NULL AS INT64)   AS team_rank_proxy,
  CAST(NULL AS INT64)   AS is_league_champion_proxy,
  CAST(NULL AS INT64)   AS is_ucl_spot_proxy,
  CAST(NULL AS INT64)   AS is_europe_spot_proxy,
  CAST(NULL AS FLOAT64) AS team_ppm_on_pitch,
  -- volume ---------------------------------------------------------------------------
  mp, starts, minutes, nineties,
  -- passo 6: por 90 recalculado a partir dos totais consolidados ----------------------
  ROUND(goals / n90, 4)   AS goals_per90,
  ROUND(assists / n90, 4) AS assists_per90,
  ROUND(npg / n90, 4)     AS npg_per90,
  CAST(NULL AS FLOAT64) AS xg_per90,
  CAST(NULL AS FLOAT64) AS npxg_per90,
  CAST(NULL AS FLOAT64) AS xag_per90,
  CAST(NULL AS FLOAT64) AS shots_per90,
  CAST(NULL AS FLOAT64) AS sot_per90,
  CAST(NULL AS FLOAT64) AS sca_per90,
  CAST(NULL AS FLOAT64) AS gca_per90,
  CAST(NULL AS FLOAT64) AS kp_per90,
  CAST(NULL AS FLOAT64) AS prgc_per90,
  CAST(NULL AS FLOAT64) AS prgp_per90,
  CAST(NULL AS FLOAT64) AS prgr_per90,
  CAST(NULL AS FLOAT64) AS tkl_per90,
  CAST(NULL AS FLOAT64) AS int_per90,
  CAST(NULL AS FLOAT64) AS clr_per90,
  CAST(NULL AS FLOAT64) AS blocks_per90,
  CAST(NULL AS FLOAT64) AS touches_att_pen_per90,
  CAST(NULL AS FLOAT64) AS recov_per90,
  ROUND(goals / n90, 4) + ROUND(assists / n90, 4) AS ga_per90,
  CAST(NULL AS FLOAT64) AS xg_xag_per90,
  CAST(NULL AS FLOAT64) AS g_minus_xg,
  CAST(NULL AS FLOAT64) AS sot_pct,
  CAST(NULL AS FLOAT64) AS pass_cmp_pct,
  CAST(NULL AS FLOAT64) AS take_on_succ_pct,
  CAST(NULL AS FLOAT64) AS aerial_won_pct,
  -- totais -----------------------------------------------------------------------------
  goals, assists, npg, pk, pk_att, cards_y, cards_r,
  -- passos 5 e 7: sem prêmio para a temporada em andamento (mesmo padrão do T1 p/ has_target = 0)
  0                       AS has_target,
  CAST(NULL AS FLOAT64)   AS bd_rank,
  CAST(NULL AS FLOAT64)   AS bd_points,
  0.0                     AS bd_score_share,
  0.0                     AS bd_score_rank,
  0.0                     AS ballon_dor_score,
  0                       AS is_ballon_dor_ranked,
  0                       AS is_top10,
  0                       AS is_winner,
  -- controle do streaming ------------------------------------------------------------
  minutes >= corte_minutos AS is_qualified,
  ROUND(corte_minutos, 1)  AS corte_minutos,
  rodadas_jogadas,
  rodadas_total,
  ultima_coleta,
  ultimo_evento
FROM base;


-- -------------------------------------------------------------------------------------
-- 2) Só os qualificados: equivalente à silver.player_season do T1 para a temporada em curso
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.player_season_stream AS
SELECT * EXCEPT (is_qualified, corte_minutos, rodadas_jogadas, rodadas_total,
                 ultima_coleta, ultimo_evento)
FROM silver.player_season_stream_all
WHERE is_qualified;
