-- =====================================================================================
-- Trabalho 2 · Parte 2 · Tabelas do streaming
--
-- Sem ID de projeto no código: `bronze.tabela` usa o projeto ativo do bq/gcloud.
-- Rodar com:  bq query --use_legacy_sql=false < sql/01_tables.sql
--
-- Particionamento: todas as tabelas de streaming são particionadas por dia de ingestão.
--   Motivo: a consulta típica é "o que chegou hoje / na última coleta", então o BigQuery
--   lê só a partição do dia em vez da tabela inteira (custo e tempo menores).
-- Clusterização da Silver por liga e jogador: o ranking e a view filtram/agrupam por elas.
-- =====================================================================================

CREATE SCHEMA IF NOT EXISTS bronze OPTIONS (location = 'US');
CREATE SCHEMA IF NOT EXISTS silver OPTIONS (location = 'US');

-- [A] Bronze: evento exatamente como chegou do Pub/Sub (rastreabilidade) -------------
CREATE TABLE IF NOT EXISTS bronze.player_stats_stream_raw (
  ingest_ts     TIMESTAMP NOT NULL OPTIONS (description = 'Momento em que o Dataflow recebeu o evento'),
  message_id    STRING    OPTIONS (description = 'ID da mensagem no Pub/Sub'),
  publish_time  TIMESTAMP OPTIONS (description = 'Momento em que o produtor publicou'),
  attributes    STRING    OPTIONS (description = 'Atributos da mensagem (JSON)'),
  data          STRING    OPTIONS (description = 'Corpo da mensagem, sem nenhuma alteração')
)
PARTITION BY DATE(ingest_ts)
OPTIONS (description = 'Bronze de streaming: eventos brutos do crawler FBref (temporada em andamento)');

-- Dead-letter: eventos que não passaram na validação ------------------------------
CREATE TABLE IF NOT EXISTS bronze.player_stats_stream_errors (
  ingest_ts      TIMESTAMP NOT NULL,
  message_id     STRING,
  error_stage    STRING OPTIONS (description = 'parse | schema | tipagem | bigquery | inesperado'),
  error_message  STRING,
  raw_data       STRING OPTIONS (description = 'Evento original, para reprocessar depois')
)
PARTITION BY DATE(ingest_ts)
OPTIONS (description = 'Dead-letter do streaming: eventos inválidos e o motivo da rejeição');

-- Silver: evento tratado, 1 linha por jogador · clube · coleta --------------------
CREATE TABLE IF NOT EXISTS silver.player_stats_stream (
  ingest_ts       TIMESTAMP NOT NULL,
  event_id        STRING,
  message_id      STRING,
  published_at    TIMESTAMP,
  snapshot_date   DATE   OPTIONS (description = 'Dia da coleta do crawler'),
  source          STRING,
  schema_version  STRING,
  season          INT64  OPTIONS (description = 'Código do T1: 2627 = 2026-27'),
  season_label    STRING,
  league          STRING,
  team            STRING,
  team_norm       STRING,
  player          STRING,
  player_norm     STRING,
  player_core     STRING OPTIONS (description = 'Chave de casamento de nomes (T1 · passo 4)'),
  player_key      STRING OPTIONS (description = 'nome|ano de nascimento, igual ao T1'),
  nation          STRING,
  pos_raw         STRING,
  age             FLOAT64,
  born_year       FLOAT64,
  mp              FLOAT64,
  starts          FLOAT64,
  minutes         FLOAT64,
  nineties        FLOAT64,
  goals           FLOAT64,
  assists         FLOAT64,
  npg             FLOAT64,
  pk              FLOAT64,
  pk_att          FLOAT64,
  cards_y         FLOAT64,
  cards_r         FLOAT64,
  business_key    STRING OPTIONS (description = 'Hash de jogador + clube + temporada + coleta'),
  record_key      STRING OPTIONS (description = 'business_key + conteúdo: usado na deduplicação')
)
PARTITION BY DATE(ingest_ts)
CLUSTER BY league, player_key
OPTIONS (description = 'Silver de streaming: eventos tipados, com nomes padronizados e sem duplicatas');
