-- =====================================================================================
-- Trabalho 2 · Parte 2 · View híbrida batch + streaming
--
-- Depende da Silver batch do Trabalho 1 (silver.player_season). Se ela ainda não existe
-- no projeto, rode os notebooks do T1 antes; o infra/01_setup.sh pula este arquivo nesse caso.
--
-- Rodar com:  bq query --use_legacy_sql=false < sql/04_hybrid_view.sql
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- Ponto de encontro batch + streaming: as colunas que a Gold do T1 lê, das duas fontes.
--    Para a Gold enxergar o streaming, troque `silver.player_season` por esta view no
--    CREATE TABLE da Gold (seção 5 do notebook gold_ballon_dor).
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.player_season_hybrid AS
SELECT
  'batch' AS origem,
  season, season_label, award_year, league, player_key, player, club_main, pos_primary, pos_raw,
  is_league_champion_proxy, is_ucl_spot_proxy, is_europe_spot_proxy, team_rank_proxy,
  goals_per90, npg_per90, xg_per90, npxg_per90, shots_per90, sot_per90, sot_pct, g_minus_xg,
  assists_per90, xag_per90, sca_per90, gca_per90, kp_per90, ga_per90, xg_xag_per90, pass_cmp_pct,
  prgc_per90, prgp_per90, prgr_per90, take_on_succ_pct, touches_att_pen_per90,
  tkl_per90, int_per90, clr_per90, blocks_per90, recov_per90, aerial_won_pct,
  minutes, age, team_ppm, team_ppm_on_pitch, is_gk,
  has_target, bd_rank, bd_points, ballon_dor_score, is_ballon_dor_ranked, is_top10, is_winner
FROM silver.player_season
WHERE season NOT IN (SELECT DISTINCT season FROM silver.player_stats_stream)
UNION ALL
SELECT
  'streaming' AS origem,
  season, season_label, award_year, league, player_key, player, club_main, pos_primary, pos_raw,
  is_league_champion_proxy, is_ucl_spot_proxy, is_europe_spot_proxy, team_rank_proxy,
  goals_per90, npg_per90, xg_per90, npxg_per90, shots_per90, sot_per90, sot_pct, g_minus_xg,
  assists_per90, xag_per90, sca_per90, gca_per90, kp_per90, ga_per90, xg_xag_per90, pass_cmp_pct,
  prgc_per90, prgp_per90, prgr_per90, take_on_succ_pct, touches_att_pen_per90,
  tkl_per90, int_per90, clr_per90, blocks_per90, recov_per90, aerial_won_pct,
  minutes, age, team_ppm, team_ppm_on_pitch, is_gk,
  has_target, bd_rank, bd_points, ballon_dor_score, is_ballon_dor_ranked, is_top10, is_winner
FROM silver.player_season_stream;
