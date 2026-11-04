-- =====================================================================================
-- Trabalho 2 · Gold: Top 30 + Top 3 do Ballon d'Or 2026 (ML.PREDICT sobre 2025-26)
--
-- Depende de sql/05_batch_2526.sql e sql/06_gold_hibrida.sql.
--
-- Rodar com:  bq query --use_legacy_sql=false < sql/07_top30_2026.sql
-- =====================================================================================

-- 1) refaz a Gold híbrida (com 2025-26 no batch) e o ranking parcial 2027
CALL `trabalho-pdm-joao-fut.gold.sp_ranking_parcial_2027`();

-- 2) previsão completa de 2025-26 (mesmo padrão da predictions_2425 do T1)
CREATE OR REPLACE TABLE `trabalho-pdm-joao-fut.gold.predictions_2526`
OPTIONS (description = 'ML.PREDICT do model_btree_ballon_dor (treinado so no historico) sobre a temporada 2025-26 -> Ballon d Or 2026.')
AS
SELECT
  RANK() OVER (ORDER BY predicted_ballon_dor_score DESC) AS posicao_prevista,
  season, season_label, award_year, league, player_key, player, club_main, pos_primary,
  minutes, goals_per90, assists_per90,
  ROUND(predicted_ballon_dor_score, 2) AS score_previsto,
  CURRENT_TIMESTAMP() AS gerado_em
FROM ML.PREDICT(MODEL `trabalho-pdm-joao-fut.gold.model_btree_ballon_dor`,
  (SELECT goals_per90_z, npg_per90_z, xg_per90_z, npxg_per90_z, shots_per90_z, sot_per90_z, sot_pct_z, g_minus_xg_z, assists_per90_z, xag_per90_z, sca_per90_z, gca_per90_z, kp_per90_z, ga_per90_z, xg_xag_per90_z, pass_cmp_pct_z, prgc_per90_z, prgp_per90_z, prgr_per90_z, take_on_succ_pct_z, touches_att_pen_per90_z, tkl_per90_z, int_per90_z, clr_per90_z, blocks_per90_z, recov_per90_z, aerial_won_pct_z, minutes_z, age_z, team_ppm_z, team_ppm_on_pitch_z, is_league_champion_proxy, is_ucl_spot_proxy, is_europe_spot_proxy, is_gk, pos_primary, league, season, season_label, award_year, player_key, player, club_main, minutes, goals_per90, assists_per90
   FROM `trabalho-pdm-joao-fut.gold.player_season_features` WHERE season = 2526));

-- 3) indicado real -> jogador do FBref: nome padronizado igual, ou nome do FBref começando pelo
--    nome da premiação ("Fabián Ruiz" -> "Fabián Ruiz Peña"); empate -> nome exato, depois mais minutos
CREATE OR REPLACE TABLE `trabalho-pdm-joao-fut.gold.indicados_2026_vs_previsto`
OPTIONS (description = 'Os 30 indicados reais ao Ballon d Or 2026 com a posicao prevista pelo modelo na temporada 2025-26.')
AS
WITH real AS (SELECT *, TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(player), 'ø', 'o'), 'ð', 'd'), 'ł', 'l'), 'đ', 'd'), 'æ', 'ae'), 'ß', 'ss'), 'ı', 'i'), NFKD), r'\p{M}', ''), r'[^a-z\s]', ' '), r'\s+', ' ')) AS nome_norm FROM `trabalho-pdm-joao-fut.bronze.ballon_dor_2026_raw`),
pred AS (SELECT *, TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(player), 'ø', 'o'), 'ð', 'd'), 'ł', 'l'), 'đ', 'd'), 'æ', 'ae'), 'ß', 'ss'), 'ı', 'i'), NFKD), r'\p{M}', ''), r'[^a-z\s]', ' '), r'\s+', ' ')) AS nome_norm FROM `trabalho-pdm-joao-fut.gold.predictions_2526`)
SELECT
  r.rank AS rank_real, r.player AS jogador_real, r.club AS clube_real,
  ARRAY_AGG(STRUCT(p.posicao_prevista, p.player, p.club_main, p.league, p.score_previsto, p.minutes)
            ORDER BY IF(p.nome_norm = r.nome_norm, 0, 1), p.minutes DESC LIMIT 1)[SAFE_OFFSET(0)] AS previsto,
  CASE WHEN COUNT(p.player_key) = 0 THEN 'fora das 5 grandes ligas ou < 900 min' ELSE 'casado' END AS status_casamento
FROM real r
LEFT JOIN pred p ON p.nome_norm = r.nome_norm OR STARTS_WITH(p.nome_norm, CONCAT(r.nome_norm, ' '))
GROUP BY r.rank, r.player, r.club;

-- 4) Top 30 previsto x 30 indicados reais
CREATE OR REPLACE TABLE `trabalho-pdm-joao-fut.gold.top30_2026`
OPTIONS (description = 'Top 30 previsto para o Ballon d Or 2026 (temporada 2025-26) comparado com os 30 indicados reais.')
AS
SELECT
  p.posicao_prevista, p.player, p.club_main, p.league, p.pos_primary, p.score_previsto,
  p.minutes, p.goals_per90, p.assists_per90,
  i.rank_real, i.rank_real IS NOT NULL AS indicado_real
FROM `trabalho-pdm-joao-fut.gold.predictions_2526` p
LEFT JOIN `trabalho-pdm-joao-fut.gold.indicados_2026_vs_previsto` i
  ON i.previsto.player = p.player AND i.previsto.club_main = p.club_main
WHERE p.posicao_prevista <= 30;

-- 5) Top 3 congelado: criado só uma vez (gerado em 03/11/2026, depois da cerimônia de 26/10)
CREATE TABLE IF NOT EXISTS `trabalho-pdm-joao-fut.gold.top3_2026_congelado`
OPTIONS (description = 'Top 3 previsto para o Ballon d Or 2026, congelado na primeira geracao.')
AS
SELECT posicao_prevista, player, club_main, league, score_previsto, gerado_em AS congelado_em
FROM `trabalho-pdm-joao-fut.gold.predictions_2526` WHERE posicao_prevista <= 3;

-- 6) conferência
SELECT
  (SELECT COUNT(*) FROM `trabalho-pdm-joao-fut.silver.player_season` WHERE season = 2526) AS silver_2526,
  (SELECT COUNTIF(indicado_real) FROM `trabalho-pdm-joao-fut.gold.top30_2026`) AS indicados_no_top30_previsto,
  (SELECT COUNTIF(status_casamento = 'casado') FROM `trabalho-pdm-joao-fut.gold.indicados_2026_vs_previsto`) AS indicados_casados;
