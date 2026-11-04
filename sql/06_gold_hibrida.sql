-- =====================================================================================
-- Trabalho 2 · Gold híbrida (batch + streaming) e ranking parcial 2027
--
-- Cria a procedure gold.sp_ranking_parcial_2027 ("roda só sob comando" do diagrama), que:
--   a) refaz gold.player_season_features com a MESMA query do T1, lendo silver.player_season_hybrid
--      (z-score por temporada: as linhas históricas ficam idênticas e o modelo não precisa ser retreinado);
--   b) gera gold.ranking_parcial_2027 com ML.PREDICT sobre a temporada em andamento (streaming).
-- Depois de novos dados no streaming:  CALL `trabalho-pdm-joao-fut.gold.sp_ranking_parcial_2027`();
--
-- Rodar com:  bq query --use_legacy_sql=false < sql/06_gold_hibrida.sql
-- =====================================================================================

-- 1) backup da Gold do T1 (só cria uma vez)
CREATE TABLE IF NOT EXISTS `trabalho-pdm-joao-fut.gold.player_season_features_t1_backup`
COPY `trabalho-pdm-joao-fut.gold.player_season_features`;

-- 2) procedure
CREATE OR REPLACE PROCEDURE `trabalho-pdm-joao-fut.gold.sp_ranking_parcial_2027`()
OPTIONS (description = 'Refaz gold.player_season_features (UNION Silver batch + view de streaming, z-score por temporada) e gera gold.ranking_parcial_2027 com ML.PREDICT do modelo treinado so no historico.')
BEGIN
  -- 2a) Gold features: mesma query do T1 (notebook gold_ballon_dor), agora lendo a Silver híbrida
  CREATE OR REPLACE TABLE `trabalho-pdm-joao-fut.gold.player_season_features`
  CLUSTER BY season, league AS
  SELECT
    -- identificação
    season, season_label, award_year, league, player_key, player, club_main,
    pos_primary, pos_raw,
    origem,  -- batch (T1) ou streaming (T2)
    -- split temporal
    CASE
      WHEN season IN (1718, 1819, 2021, 2122) THEN 'train'
      WHEN season IN (2223, 2324) THEN 'test'
      ELSE 'inference'
    END AS split,
    -- contexto (binárias)
    is_league_champion_proxy, is_ucl_spot_proxy, is_europe_spot_proxy,
    team_rank_proxy,
    -- numéricas normalizadas por temporada (z-score)
    SAFE_DIVIDE((goals_per90 - AVG(goals_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(goals_per90) OVER (PARTITION BY season), 0)) AS goals_per90_z,
    SAFE_DIVIDE((npg_per90 - AVG(npg_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(npg_per90) OVER (PARTITION BY season), 0)) AS npg_per90_z,
    SAFE_DIVIDE((xg_per90 - AVG(xg_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(xg_per90) OVER (PARTITION BY season), 0)) AS xg_per90_z,
    SAFE_DIVIDE((npxg_per90 - AVG(npxg_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(npxg_per90) OVER (PARTITION BY season), 0)) AS npxg_per90_z,
    SAFE_DIVIDE((shots_per90 - AVG(shots_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(shots_per90) OVER (PARTITION BY season), 0)) AS shots_per90_z,
    SAFE_DIVIDE((sot_per90 - AVG(sot_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(sot_per90) OVER (PARTITION BY season), 0)) AS sot_per90_z,
    SAFE_DIVIDE((sot_pct - AVG(sot_pct) OVER (PARTITION BY season)), NULLIF(STDDEV(sot_pct) OVER (PARTITION BY season), 0)) AS sot_pct_z,
    SAFE_DIVIDE((g_minus_xg - AVG(g_minus_xg) OVER (PARTITION BY season)), NULLIF(STDDEV(g_minus_xg) OVER (PARTITION BY season), 0)) AS g_minus_xg_z,
    SAFE_DIVIDE((assists_per90 - AVG(assists_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(assists_per90) OVER (PARTITION BY season), 0)) AS assists_per90_z,
    SAFE_DIVIDE((xag_per90 - AVG(xag_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(xag_per90) OVER (PARTITION BY season), 0)) AS xag_per90_z,
    SAFE_DIVIDE((sca_per90 - AVG(sca_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(sca_per90) OVER (PARTITION BY season), 0)) AS sca_per90_z,
    SAFE_DIVIDE((gca_per90 - AVG(gca_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(gca_per90) OVER (PARTITION BY season), 0)) AS gca_per90_z,
    SAFE_DIVIDE((kp_per90 - AVG(kp_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(kp_per90) OVER (PARTITION BY season), 0)) AS kp_per90_z,
    SAFE_DIVIDE((ga_per90 - AVG(ga_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(ga_per90) OVER (PARTITION BY season), 0)) AS ga_per90_z,
    SAFE_DIVIDE((xg_xag_per90 - AVG(xg_xag_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(xg_xag_per90) OVER (PARTITION BY season), 0)) AS xg_xag_per90_z,
    SAFE_DIVIDE((pass_cmp_pct - AVG(pass_cmp_pct) OVER (PARTITION BY season)), NULLIF(STDDEV(pass_cmp_pct) OVER (PARTITION BY season), 0)) AS pass_cmp_pct_z,
    SAFE_DIVIDE((prgc_per90 - AVG(prgc_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(prgc_per90) OVER (PARTITION BY season), 0)) AS prgc_per90_z,
    SAFE_DIVIDE((prgp_per90 - AVG(prgp_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(prgp_per90) OVER (PARTITION BY season), 0)) AS prgp_per90_z,
    SAFE_DIVIDE((prgr_per90 - AVG(prgr_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(prgr_per90) OVER (PARTITION BY season), 0)) AS prgr_per90_z,
    SAFE_DIVIDE((take_on_succ_pct - AVG(take_on_succ_pct) OVER (PARTITION BY season)), NULLIF(STDDEV(take_on_succ_pct) OVER (PARTITION BY season), 0)) AS take_on_succ_pct_z,
    SAFE_DIVIDE((touches_att_pen_per90 - AVG(touches_att_pen_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(touches_att_pen_per90) OVER (PARTITION BY season), 0)) AS touches_att_pen_per90_z,
    SAFE_DIVIDE((tkl_per90 - AVG(tkl_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(tkl_per90) OVER (PARTITION BY season), 0)) AS tkl_per90_z,
    SAFE_DIVIDE((int_per90 - AVG(int_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(int_per90) OVER (PARTITION BY season), 0)) AS int_per90_z,
    SAFE_DIVIDE((clr_per90 - AVG(clr_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(clr_per90) OVER (PARTITION BY season), 0)) AS clr_per90_z,
    SAFE_DIVIDE((blocks_per90 - AVG(blocks_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(blocks_per90) OVER (PARTITION BY season), 0)) AS blocks_per90_z,
    SAFE_DIVIDE((recov_per90 - AVG(recov_per90) OVER (PARTITION BY season)), NULLIF(STDDEV(recov_per90) OVER (PARTITION BY season), 0)) AS recov_per90_z,
    SAFE_DIVIDE((aerial_won_pct - AVG(aerial_won_pct) OVER (PARTITION BY season)), NULLIF(STDDEV(aerial_won_pct) OVER (PARTITION BY season), 0)) AS aerial_won_pct_z,
    SAFE_DIVIDE((minutes - AVG(minutes) OVER (PARTITION BY season)), NULLIF(STDDEV(minutes) OVER (PARTITION BY season), 0)) AS minutes_z,
    SAFE_DIVIDE((age - AVG(age) OVER (PARTITION BY season)), NULLIF(STDDEV(age) OVER (PARTITION BY season), 0)) AS age_z,
    SAFE_DIVIDE((team_ppm - AVG(team_ppm) OVER (PARTITION BY season)), NULLIF(STDDEV(team_ppm) OVER (PARTITION BY season), 0)) AS team_ppm_z,
    SAFE_DIVIDE((team_ppm_on_pitch - AVG(team_ppm_on_pitch) OVER (PARTITION BY season)), NULLIF(STDDEV(team_ppm_on_pitch) OVER (PARTITION BY season), 0)) AS team_ppm_on_pitch_z,
    -- escala natural (leitura humana / BI)
    is_gk,
    goals_per90, npg_per90, xg_per90, npxg_per90, shots_per90, sot_per90, sot_pct, g_minus_xg, assists_per90, xag_per90, sca_per90, gca_per90, kp_per90, ga_per90, xg_xag_per90, pass_cmp_pct, prgc_per90, prgp_per90, prgr_per90, take_on_succ_pct, touches_att_pen_per90, tkl_per90, int_per90, clr_per90, blocks_per90, recov_per90, aerial_won_pct, minutes, age, team_ppm, team_ppm_on_pitch,
    -- target e apoio
    has_target, bd_rank, bd_points, ballon_dor_score, is_ballon_dor_ranked, is_top10, is_winner
  FROM `trabalho-pdm-joao-fut.silver.player_season_hybrid`;

  -- 2b) ranking parcial da temporada em andamento (2026-27), via streaming
  CREATE OR REPLACE TABLE `trabalho-pdm-joao-fut.gold.ranking_parcial_2027`
  OPTIONS (description = 'Ranking parcial Ballon d Or 2027: ML.PREDICT do model_btree_ballon_dor sobre a temporada 2026-27 (streaming). Gerado por CALL gold.sp_ranking_parcial_2027().')
  AS
  WITH pred AS (
    SELECT * FROM ML.PREDICT(MODEL `trabalho-pdm-joao-fut.gold.model_btree_ballon_dor`,
      (SELECT goals_per90_z, npg_per90_z, xg_per90_z, npxg_per90_z, shots_per90_z, sot_per90_z, sot_pct_z, g_minus_xg_z, assists_per90_z, xag_per90_z, sca_per90_z, gca_per90_z, kp_per90_z, ga_per90_z, xg_xag_per90_z, pass_cmp_pct_z, prgc_per90_z, prgp_per90_z, prgr_per90_z, take_on_succ_pct_z, touches_att_pen_per90_z, tkl_per90_z, int_per90_z, clr_per90_z, blocks_per90_z, recov_per90_z, aerial_won_pct_z, minutes_z, age_z, team_ppm_z, team_ppm_on_pitch_z, is_league_champion_proxy, is_ucl_spot_proxy, is_europe_spot_proxy, is_gk, pos_primary, league,
              season, season_label, award_year, player_key, player, club_main, minutes, goals_per90, assists_per90
       FROM `trabalho-pdm-joao-fut.gold.player_season_features`
       WHERE origem = 'streaming'))
  )
  SELECT
    RANK() OVER (PARTITION BY p.season ORDER BY p.predicted_ballon_dor_score DESC) AS posicao_prevista,
    p.season, p.season_label, p.award_year, p.league, p.player, p.club_main, p.pos_primary,
    p.minutes, p.goals_per90, p.assists_per90,
    ROUND(p.predicted_ballon_dor_score, 2) AS score_previsto,
    s.rodadas_jogadas, s.rodadas_total, s.ultima_coleta,
    CURRENT_TIMESTAMP() AS gerado_em
  FROM pred p
  LEFT JOIN `trabalho-pdm-joao-fut.silver.player_season_stream_all` s
    ON s.season = p.season AND s.player_key = p.player_key;
END;

-- 3) roda agora
CALL `trabalho-pdm-joao-fut.gold.sp_ranking_parcial_2027`();
