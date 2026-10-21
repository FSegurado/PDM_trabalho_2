-- =====================================================================================
-- Trabalho 2 · Parte 2 · Consultas para a demo ao vivo (rodar uma de cada vez no console)
-- =====================================================================================

-- 1) O streaming chegando: eventos por minuto nos últimos 15 min (rode de novo e veja crescer)
SELECT
  TIMESTAMP_TRUNC(ingest_ts, MINUTE) AS minuto,
  COUNT(*) AS eventos_brutos
FROM bronze.player_stats_stream_raw
WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
GROUP BY minuto
ORDER BY minuto DESC;

-- 2) Latência ponta a ponta: publicação no Pub/Sub -> linha disponível na Silver
SELECT
  player, team,
  published_at,
  ingest_ts,
  TIMESTAMP_DIFF(ingest_ts, published_at, MILLISECOND) / 1000 AS segundos_ate_silver
FROM silver.player_stats_stream
WHERE DATE(ingest_ts) = CURRENT_DATE()
ORDER BY ingest_ts DESC
LIMIT 20;

-- 3) Funil do pipeline hoje: bruto -> tratado -> dead-letter
SELECT
  (SELECT COUNT(*) FROM bronze.player_stats_stream_raw    WHERE DATE(ingest_ts) = CURRENT_DATE()) AS brutos,
  (SELECT COUNT(*) FROM silver.player_stats_stream        WHERE DATE(ingest_ts) = CURRENT_DATE()) AS tratados,
  (SELECT COUNT(*) FROM bronze.player_stats_stream_errors WHERE DATE(ingest_ts) = CURRENT_DATE()) AS rejeitados;

-- 4) Dead-letter: o que foi rejeitado e por quê
SELECT ingest_ts, error_stage, error_message, LEFT(raw_data, 200) AS evento
FROM bronze.player_stats_stream_errors
WHERE DATE(ingest_ts) = CURRENT_DATE()
ORDER BY ingest_ts DESC
LIMIT 20;

-- 5) Silver consolidada da temporada em andamento (view): melhores G+A por 90 entre os qualificados
SELECT player, club_main, league, minutes, goals, assists, ga_per90, n_clubs
FROM silver.player_season_stream
ORDER BY ga_per90 DESC
LIMIT 15;

-- 6) Corte proporcional de minutos por liga (passo 6 adaptado à temporada em curso)
SELECT league, ANY_VALUE(rodadas_jogadas) AS rodadas_jogadas, ANY_VALUE(rodadas_total) AS rodadas_total,
       ANY_VALUE(corte_minutos) AS corte_minutos,
       COUNTIF(is_qualified) AS qualificados, COUNT(*) AS jogadores
FROM silver.player_season_stream_all
GROUP BY league
ORDER BY league;

-- 7) Relação batch + streaming: as duas origens lado a lado na view híbrida
SELECT origem, season_label, COUNT(*) AS jogadores, ROUND(AVG(goals_per90), 3) AS media_gols_90
FROM silver.player_season_hybrid
GROUP BY origem, season_label
ORDER BY season_label;

-- 8) Pruning de partição: compare os bytes processados destas duas consultas no console
SELECT COUNT(*) FROM silver.player_stats_stream WHERE DATE(ingest_ts) = CURRENT_DATE();
SELECT COUNT(*) FROM silver.player_stats_stream;

-- 9) Conferência de schema: colunas da Silver batch que a view de streaming não tem
SELECT column_name, data_type
FROM silver.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'player_season'
  AND column_name NOT IN (SELECT column_name FROM silver.INFORMATION_SCHEMA.COLUMNS
                          WHERE table_name = 'player_season_stream');
