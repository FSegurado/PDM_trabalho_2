-- =====================================================================================
-- Trabalho 2 · Batch 2025-26 (temporada encerrada): Bronze + Silver
--
-- Antes: gcloud storage cp players_stats_2526.csv gs://futdados/bronze/   (CSV do crawler, separador ;)
-- Aplica as regras da Silver do T1 / ballon_common (tipagem, nomes, transferências, por 90,
-- corte de 900 min). 2025-26 entra como inferência (has_target = 0): o modelo segue treinado
-- só no histórico.
--
-- Rodar com:  bq query --use_legacy_sql=false < sql/05_batch_2526.sql
-- =====================================================================================

-- 1) Bronze: cópia fiel do CSV, tudo como texto
LOAD DATA OVERWRITE `trabalho-pdm-joao-fut.bronze.players_stats_2526_raw` (`league` STRING, `season` STRING, `team` STRING, `player` STRING, `nation` STRING, `pos` STRING, `age` STRING, `born` STRING, `Playing Time_MP` STRING, `Playing Time_Starts` STRING, `Playing Time_Min` STRING, `Playing Time_90s` STRING, `Performance_Gls` STRING, `Performance_Ast` STRING, `Performance_G+A` STRING, `Performance_G-PK` STRING, `Performance_PK` STRING, `Performance_PKatt` STRING, `Performance_CrdY` STRING, `Performance_CrdR` STRING, `Per 90 Minutes_Gls` STRING, `Per 90 Minutes_Ast` STRING, `Per 90 Minutes_G+A` STRING, `Per 90 Minutes_G-PK` STRING, `Per 90 Minutes_G+A-PK` STRING)
FROM FILES (format = 'CSV', field_delimiter = ';', skip_leading_rows = 1,
            uris = ['gs://futdados/bronze/players_stats_2526.csv']);

-- 2) Bronze: resultado real do Ballon d'Or 2026 (30 indicados em 08/09/2026, ranking final de 26/10/2026)
CREATE OR REPLACE TABLE `trabalho-pdm-joao-fut.bronze.ballon_dor_2026_raw`
OPTIONS (description = 'Ballon d Or 2026: 30 indicados (France Football, 08/09/2026) e ranking final da cerimonia de 26/10/2026. Fonte: imprensa esportiva; 29o lugar empatado.')
AS SELECT * FROM UNNEST([STRUCT<year INT64, rank INT64, player STRING, club STRING>
  (2026, 1, 'Harry Kane', 'Bayern Munich'),
  (2026, 2, 'Kylian Mbappé', 'Real Madrid'),
  (2026, 3, 'Lamine Yamal', 'Barcelona'),
  (2026, 4, 'Rodri', 'Manchester City'),
  (2026, 5, 'Lionel Messi', 'Inter Miami'),
  (2026, 6, 'Khvicha Kvaratskhelia', 'Paris Saint-Germain'),
  (2026, 7, 'Ousmane Dembélé', 'Paris Saint-Germain'),
  (2026, 8, 'Erling Haaland', 'Manchester City'),
  (2026, 9, 'Michael Olise', 'Bayern Munich'),
  (2026, 10, 'Fabián Ruiz', 'Paris Saint-Germain'),
  (2026, 11, 'Jude Bellingham', 'Real Madrid'),
  (2026, 12, 'Pau Cubarsí', 'Barcelona'),
  (2026, 13, 'Achraf Hakimi', 'Paris Saint-Germain'),
  (2026, 14, 'Vitinha', 'Paris Saint-Germain'),
  (2026, 15, 'Declan Rice', 'Arsenal'),
  (2026, 16, 'Ferran Torres', 'Barcelona'),
  (2026, 17, 'Vinícius Júnior', 'Real Madrid'),
  (2026, 18, 'Marc Cucurella', 'Chelsea'),
  (2026, 19, 'João Neves', 'Paris Saint-Germain'),
  (2026, 20, 'Willian Pacho', 'Paris Saint-Germain'),
  (2026, 21, 'Gabriel Magalhães', 'Arsenal'),
  (2026, 22, 'Lautaro Martínez', 'Inter'),
  (2026, 23, 'Nuno Mendes', 'Paris Saint-Germain'),
  (2026, 24, 'Luis Díaz', 'Bayern Munich'),
  (2026, 25, 'Marquinhos', 'Paris Saint-Germain'),
  (2026, 26, 'Dayot Upamecano', 'Bayern Munich'),
  (2026, 27, 'Bruno Fernandes', 'Manchester United'),
  (2026, 28, 'Julián Quiñones', 'Al-Qadsiah'),
  (2026, 29, 'Sadio Mané', 'Al-Nassr'),
  (2026, 29, 'William Saliba', 'Arsenal')]);

-- 3) Silver: backup da tabela do T1 antes de acrescentar 2025-26 (só cria uma vez)
CREATE TABLE IF NOT EXISTS `trabalho-pdm-joao-fut.silver.player_season_t1_backup` COPY `trabalho-pdm-joao-fut.silver.player_season`;

-- 4) Silver 2025-26
DELETE FROM `trabalho-pdm-joao-fut.silver.player_season` WHERE season = 2526;
INSERT INTO `trabalho-pdm-joao-fut.silver.player_season`
  (season, season_label, award_year, league, player_key, player, player_norm, nation, pos_raw, pos_primary, is_gk,
   age, born_year, club_main, n_clubs, team_ppm, team_rank_proxy, is_league_champion_proxy, is_ucl_spot_proxy,
   is_europe_spot_proxy, team_ppm_on_pitch, mp, starts, minutes, nineties, goals_per90, assists_per90, npg_per90, ga_per90,
   goals, assists, npg, pk, pk_att, cards_y, cards_r,
   has_target, bd_rank, bd_points, bd_score_share, bd_score_rank, ballon_dor_score, is_ballon_dor_ranked, is_top10, is_winner,
   match_method, player_core, club_norm)
WITH tipado AS (            -- passo 1: seleção e tipagem
  SELECT
    2526 AS season, TRIM(league) AS league, TRIM(team) AS team, TRIM(player) AS player,
    NULLIF(TRIM(nation), '') AS nation, NULLIF(TRIM(pos), '') AS pos_raw,
    SAFE_CAST(REGEXP_EXTRACT(age, r'^\s*(\d{1,2})') AS FLOAT64) AS age,   -- "27" ou "29-008" -> 29
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`born`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS born_year,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Playing Time_MP`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS mp,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Playing Time_Starts`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS starts,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Playing Time_Min`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS minutes,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Playing Time_90s`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS nineties,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Performance_Gls`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS goals,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Performance_Ast`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS assists,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Performance_G-PK`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS npg,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Performance_PK`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS pk,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Performance_PKatt`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS pk_att,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Performance_CrdY`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS cards_y,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Performance_CrdR`), ',', '.'), r'[^0-9.\-]', ''), '') AS FLOAT64) AS cards_r
  FROM `trabalho-pdm-joao-fut.bronze.players_stats_2526_raw`
  WHERE TRIM(league) IN ('ENG-Premier League', 'ESP-La Liga', 'ITA-Serie A', 'GER-Bundesliga', 'FRA-Ligue 1')
),
chaveado AS (               -- passo 4: chave jogador|nascimento (mesma do T1)
  SELECT *, CONCAT(player, '|', IFNULL(CAST(CAST(born_year AS INT64) AS STRING), '<NA>')) AS player_key
  FROM tipado WHERE minutes > 0
),
consolidado AS (            -- passo 2: transferências -> 1 linha por jogador-temporada (clube principal = mais minutos)
  SELECT season, player_key,
    ARRAY_AGG(STRUCT(player, nation, pos_raw, age, born_year, team, league) ORDER BY minutes DESC LIMIT 1)[OFFSET(0)] AS p,
    COUNT(*) AS n_clubs, SUM(mp) mp, SUM(starts) starts, SUM(minutes) minutes, SUM(nineties) nineties,
    SUM(goals) goals, SUM(assists) assists, SUM(npg) npg, SUM(pk) pk, SUM(pk_att) pk_att, SUM(cards_y) cards_y, SUM(cards_r) cards_r
  FROM chaveado GROUP BY season, player_key
)
SELECT
  season, '2025-26', 2026, p.league, player_key, p.player, TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(p.player), 'ø', 'o'), 'ð', 'd'), 'ł', 'l'), 'đ', 'd'), 'æ', 'ae'), 'ß', 'ss'), 'ı', 'i'), NFKD), r'\p{M}', ''), r'[^a-z\s]', ' '), r'\s+', ' ')), p.nation, p.pos_raw,
  SPLIT(COALESCE(p.pos_raw, 'NA'), ',')[OFFSET(0)],
  IF(SPLIT(COALESCE(p.pos_raw, 'NA'), ',')[OFFSET(0)] = 'GK', 1, 0),
  p.age, p.born_year, p.team, n_clubs,
  NULL, NULL, NULL, NULL, NULL, NULL,          -- passo 3: o crawler não traz Team Success_PPM
  CAST(mp AS INT64), CAST(starts AS INT64), CAST(minutes AS INT64), CAST(ROUND(nineties * 10) AS INT64),
  ROUND(goals / (minutes / 90), 4), ROUND(assists / (minutes / 90), 4), ROUND(npg / (minutes / 90), 4),
  ROUND(goals / (minutes / 90), 4) + ROUND(assists / (minutes / 90), 4),
  CAST(goals AS INT64), CAST(assists AS INT64), CAST(npg AS INT64), CAST(pk AS INT64), CAST(pk_att AS INT64),
  CAST(cards_y AS INT64), CAST(cards_r AS INT64),
  0, NULL, NULL, 0.0, 0.0, 0.0, 0, 0, 0,       -- passos 5 e 7: inferência
  'crawler_2526', TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(p.player), 'ø', 'o'), 'ð', 'd'), 'ł', 'l'), 'đ', 'd'), 'æ', 'ae'), 'ß', 'ss'), 'ı', 'i'), NFKD), r'\p{M}', ''), r'[^a-z\s]', ' '), r'\s+', ' ')), TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(p.team), 'ø', 'o'), 'ð', 'd'), 'ł', 'l'), 'đ', 'd'), 'æ', 'ae'), 'ß', 'ss'), 'ı', 'i'), NFKD), r'\p{M}', ''), r'[^a-z\s]', ' '), r'\s+', ' '))
FROM consolidado
WHERE minutes >= 900;                          -- passo 6: corte de 900 minutos do T1
