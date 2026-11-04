-- =====================================================================================
-- Trabalho 2 · Parte 3: exporta o modelo BQML para o Vertex AI
--
-- Gera em gs://futdados/models/ballon_dor_btree/v1/:
--   model/          XGBoost + assets do BQML (registrado no Vertex AI por infra/05_deploy_partes_3_4.sh)
--   preprocessing/  média e desvio por temporada (z-score da Gold) e amostra de referência do ML.PREDICT,
--                   usados pela API (api/main.py) e pela validação (api/validate.py).
--
-- Rodar com:  bq query --use_legacy_sql=false < sql/08_export_modelo.sql
-- =====================================================================================

EXPORT MODEL `trabalho-pdm-joao-fut.gold.model_btree_ballon_dor` OPTIONS (URI = 'gs://futdados/models/ballon_dor_btree/v1/model/');

-- média e desvio por temporada (mesmo z-score da Gold)
EXPORT DATA OPTIONS (uri = 'gs://futdados/models/ballon_dor_btree/v1/preprocessing/feature_stats_*.json', format = 'JSON', overwrite = true) AS
SELECT season, s.feature, s.mean, s.std
FROM (
  SELECT season, [
    STRUCT('goals_per90' AS feature, AVG(goals_per90) AS mean, STDDEV(goals_per90) AS std),
    STRUCT('npg_per90' AS feature, AVG(npg_per90) AS mean, STDDEV(npg_per90) AS std),
    STRUCT('xg_per90' AS feature, AVG(xg_per90) AS mean, STDDEV(xg_per90) AS std),
    STRUCT('npxg_per90' AS feature, AVG(npxg_per90) AS mean, STDDEV(npxg_per90) AS std),
    STRUCT('shots_per90' AS feature, AVG(shots_per90) AS mean, STDDEV(shots_per90) AS std),
    STRUCT('sot_per90' AS feature, AVG(sot_per90) AS mean, STDDEV(sot_per90) AS std),
    STRUCT('sot_pct' AS feature, AVG(sot_pct) AS mean, STDDEV(sot_pct) AS std),
    STRUCT('g_minus_xg' AS feature, AVG(g_minus_xg) AS mean, STDDEV(g_minus_xg) AS std),
    STRUCT('assists_per90' AS feature, AVG(assists_per90) AS mean, STDDEV(assists_per90) AS std),
    STRUCT('xag_per90' AS feature, AVG(xag_per90) AS mean, STDDEV(xag_per90) AS std),
    STRUCT('sca_per90' AS feature, AVG(sca_per90) AS mean, STDDEV(sca_per90) AS std),
    STRUCT('gca_per90' AS feature, AVG(gca_per90) AS mean, STDDEV(gca_per90) AS std),
    STRUCT('kp_per90' AS feature, AVG(kp_per90) AS mean, STDDEV(kp_per90) AS std),
    STRUCT('ga_per90' AS feature, AVG(ga_per90) AS mean, STDDEV(ga_per90) AS std),
    STRUCT('xg_xag_per90' AS feature, AVG(xg_xag_per90) AS mean, STDDEV(xg_xag_per90) AS std),
    STRUCT('pass_cmp_pct' AS feature, AVG(pass_cmp_pct) AS mean, STDDEV(pass_cmp_pct) AS std),
    STRUCT('prgc_per90' AS feature, AVG(prgc_per90) AS mean, STDDEV(prgc_per90) AS std),
    STRUCT('prgp_per90' AS feature, AVG(prgp_per90) AS mean, STDDEV(prgp_per90) AS std),
    STRUCT('prgr_per90' AS feature, AVG(prgr_per90) AS mean, STDDEV(prgr_per90) AS std),
    STRUCT('take_on_succ_pct' AS feature, AVG(take_on_succ_pct) AS mean, STDDEV(take_on_succ_pct) AS std),
    STRUCT('touches_att_pen_per90' AS feature, AVG(touches_att_pen_per90) AS mean, STDDEV(touches_att_pen_per90) AS std),
    STRUCT('tkl_per90' AS feature, AVG(tkl_per90) AS mean, STDDEV(tkl_per90) AS std),
    STRUCT('int_per90' AS feature, AVG(int_per90) AS mean, STDDEV(int_per90) AS std),
    STRUCT('clr_per90' AS feature, AVG(clr_per90) AS mean, STDDEV(clr_per90) AS std),
    STRUCT('blocks_per90' AS feature, AVG(blocks_per90) AS mean, STDDEV(blocks_per90) AS std),
    STRUCT('recov_per90' AS feature, AVG(recov_per90) AS mean, STDDEV(recov_per90) AS std),
    STRUCT('aerial_won_pct' AS feature, AVG(aerial_won_pct) AS mean, STDDEV(aerial_won_pct) AS std),
    STRUCT('minutes' AS feature, AVG(minutes) AS mean, STDDEV(minutes) AS std),
    STRUCT('age' AS feature, AVG(age) AS mean, STDDEV(age) AS std),
    STRUCT('team_ppm' AS feature, AVG(team_ppm) AS mean, STDDEV(team_ppm) AS std),
    STRUCT('team_ppm_on_pitch' AS feature, AVG(team_ppm_on_pitch) AS mean, STDDEV(team_ppm_on_pitch) AS std)
  ] AS arr
  FROM `trabalho-pdm-joao-fut.gold.player_season_features` GROUP BY season
), UNNEST(arr) s;

-- amostra de referência: entrada crua + score do ML.PREDICT (15 jogadores por temporada)
EXPORT DATA OPTIONS (uri = 'gs://futdados/models/ballon_dor_btree/v1/preprocessing/reference_*.json', format = 'JSON', overwrite = true) AS
SELECT * EXCEPT (goals_per90_z, npg_per90_z, xg_per90_z, npxg_per90_z, shots_per90_z, sot_per90_z, sot_pct_z, g_minus_xg_z, assists_per90_z, xag_per90_z, sca_per90_z, gca_per90_z, kp_per90_z, ga_per90_z, xg_xag_per90_z, pass_cmp_pct_z, prgc_per90_z, prgp_per90_z, prgr_per90_z, take_on_succ_pct_z, touches_att_pen_per90_z, tkl_per90_z, int_per90_z, clr_per90_z, blocks_per90_z, recov_per90_z, aerial_won_pct_z, minutes_z, age_z, team_ppm_z, team_ppm_on_pitch_z)
FROM ML.PREDICT(MODEL `trabalho-pdm-joao-fut.gold.model_btree_ballon_dor`,
  (SELECT goals_per90_z, npg_per90_z, xg_per90_z, npxg_per90_z, shots_per90_z, sot_per90_z, sot_pct_z, g_minus_xg_z, assists_per90_z, xag_per90_z, sca_per90_z, gca_per90_z, kp_per90_z, ga_per90_z, xg_xag_per90_z, pass_cmp_pct_z, prgc_per90_z, prgp_per90_z, prgr_per90_z, take_on_succ_pct_z, touches_att_pen_per90_z, tkl_per90_z, int_per90_z, clr_per90_z, blocks_per90_z, recov_per90_z, aerial_won_pct_z, minutes_z, age_z, team_ppm_z, team_ppm_on_pitch_z, is_league_champion_proxy, is_ucl_spot_proxy, is_europe_spot_proxy, is_gk, pos_primary, league, season, player, club_main, goals_per90, npg_per90, xg_per90, npxg_per90, shots_per90, sot_per90, sot_pct, g_minus_xg, assists_per90, xag_per90, sca_per90, gca_per90, kp_per90, ga_per90, xg_xag_per90, pass_cmp_pct, prgc_per90, prgp_per90, prgr_per90, take_on_succ_pct, touches_att_pen_per90, tkl_per90, int_per90, clr_per90, blocks_per90, recov_per90, aerial_won_pct, minutes, age, team_ppm, team_ppm_on_pitch
   FROM `trabalho-pdm-joao-fut.gold.player_season_features`
   WHERE season IN (2324, 2526, 2627)
   QUALIFY ROW_NUMBER() OVER (PARTITION BY season ORDER BY goals_per90 DESC) <= 15));
