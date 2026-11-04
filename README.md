# Preditor Ballon d'Or · Trabalho 2: arquitetura híbrida Batch + Streaming no GCP

Evolução do Trabalho 1 (pipeline batch Medallion no BigQuery) para uma arquitetura híbrida. As temporadas encerradas continuam entrando por **batch**. A temporada em andamento (2026-27) entra por **streaming** (Pub/Sub → Dataflow → BigQuery). O modelo do BigQuery ML é registrado no **Vertex AI** e servido por uma **API REST no Cloud Run**.

| | |
|---|---|
| **API no Cloud Run** | `<URL da API>` (veja [Chamar a API](#7-chamar-a-api)) |
| **Projeto GCP** | `trabalho-pdm-joao-fut` · Dataflow, Cloud Run e Vertex AI em `us-central1` · BigQuery em `US` |
| **Roteiro de teste/demo** | [`roteiro_demo_streaming.md`](roteiro_demo_streaming.md) |

---

## 1. Do Trabalho 1 ao Trabalho 2

| | Trabalho 1 | Trabalho 2 |
|---|---|---|
| Ingestão | CSVs do FBref carregados por notebook | Batch (temporadas encerradas, até 2025-26) **+ streaming** da temporada 2026-27 via Pub/Sub |
| Processamento | Notebooks Bronze → Silver → Gold | Notebooks/SQL no batch **+ Dataflow (Apache Beam)** em tempo real, com as mesmas regras da Silver do T1 |
| Integração | Uma Silver | View `silver.player_season_hybrid`: batch e streaming lado a lado, mesmo schema, coluna `origem` |
| Modelo | BQML `BOOSTED_TREE_REGRESSOR` no BigQuery | O mesmo modelo, exportado e registrado no **Vertex AI Model Registry** |
| Predição | `ML.PREDICT` no BigQuery | `ML.PREDICT` sob comando **+ API REST** em contêiner no **Cloud Run** (`POST /predict` → JSON) |

---

## 2. Arquitetura

```
 (1) STREAMING · temporada 2026-27                          BATCH · temporadas encerradas
 ─────────────────────────────────                          ─────────────────────────────
 Crawler FBref (máquina local, soccerdata)                  CSVs FBref 2017–2025 (T1)
   │ gcloud storage cp                                      CSV 2025-26 do crawler
   ▼                                                        resultado do Ballon d'Or
 gs://futdados/cache/players_stats_2627.csv                   │ notebooks do T1 + sql/05
   │                                                          │
 Cloud Scheduler (06:17) ─► Cloud Run Job producer-fbref      │
   │ 1 evento JSON por jogador                                │
   ▼                                                          │
 Pub/Sub  fbref-player-stats-stream                           │
   │ subscription fbref-player-stats-dataflow                 │
   ▼                                                          │
 (2) Dataflow ballon-dor-streaming (Apache Beam)              │
   ├─[A] evento bruto ──────► bronze.player_stats_stream_raw  │
   ├─ 1 parse + schema                                        ▼
   ├─ 2 seleção e tipagem   ┌─ bronze.players_stats_*_raw, bronze.ballon_dor_2026_raw
   ├─ 3 nomes padronizados  │
   ├─ 4 deduplicação        └► silver.player_season (2017-18 a 2025-26)
   ├─────────────────────► silver.player_stats_stream                  │
   └─ inválidos ─────────► bronze.player_stats_stream_errors           │
                                   │                                   │
          views: silver.player_season_stream_all / player_season_stream│
                                   ▼                                   ▼
                        silver.player_season_hybrid  ◄── ponto de encontro (coluna origem)
                                   │  CALL gold.sp_ranking_parcial_2027()  (só sob comando)
                                   ▼
                        gold.player_season_features (z-score por temporada)
                                   │ treino só no histórico (T1)
                                   ▼
                        gold.model_btree_ballon_dor ─ ML.PREDICT ─► gold.ranking_parcial_2027
                                   │                             └► gold.predictions_2526, top30_2026,
                                   │                                indicados_2026_vs_previsto, top3_2026_congelado
                                   │ EXPORT MODEL (sql/08)
                                   ▼
 (3) gs://futdados/models/ballon_dor_btree/v1/  model/ + preprocessing/
                                   │ gcloud ai models upload
                                   ▼
     Vertex AI Model Registry: ballon-dor-btree (sem endpoint)
                                   │ a API lê o registro e baixa o artefato na subida
                                   ▼
 (4) Cloud Run ballon-dor-api (privada, IAM) ◄── HTTP + token ── cURL / Postman / api/validate.py
       GET /health · GET /model · POST /predict
       imagens no Artifact Registry (repositório ballon-dor), build pelo Cloud Build
```

### Papel de cada componente

| # | Componente | Serviço GCP | O que faz |
|---|---|---|---|
| 1 | Crawler | fora da nuvem | Coleta a tabela "standard" do FBref (Big 5) com o `soccerdata` e grava um CSV. Roda na máquina local porque o FBref bloqueia acessos automatizados vindos de IPs de nuvem. |
| 1 | Produtor | Cloud Run Job + Cloud Scheduler | Lê o snapshot no GCS e publica 1 evento JSON por jogador no Pub/Sub, sem alterar valores. Roda todo dia às 06:17 ou sob comando. |
| 1 | Fila | Pub/Sub | Desacopla produtor e pipeline. Entrega pelo menos uma vez e retém por 1 dia. |
| 2 | Pipeline de streaming | Dataflow | Grava o evento bruto, valida, tipa, padroniza nomes e deduplica. Grava a Silver e manda os inválidos para a dead-letter. |
| 2 | Armazenamento | BigQuery | Camadas Bronze/Silver/Gold das duas origens. A view híbrida junta batch e streaming. |
| 2 | Gold e predição em lote | BigQuery ML | A procedure refaz a Gold a partir da view híbrida e roda `ML.PREDICT`. |
| 3 | Registro do modelo | Vertex AI Model Registry | Guarda o modelo exportado do BQML (XGBoost) com versão e metadados. |
| 4 | Inferência online | Cloud Run | API FastAPI em contêiner que devolve a predição em JSON. |

---

## 3. Tabelas e views no BigQuery

| Camada | Objeto | Origem | Criado por |
|---|---|---|---|
| Bronze | `player_stats_stream_raw` | streaming | `sql/01_tables.sql` (gravado pelo Dataflow) |
| Bronze | `player_stats_stream_errors` | streaming (dead-letter) | `sql/01_tables.sql` |
| Bronze | `players_stats_2526_raw`, `ballon_dor_2026_raw` | batch | `sql/05_batch_2526.sql` |
| Silver | `player_stats_stream` | streaming | `sql/01_tables.sql` |
| Silver | `player_season_stream_all`, `player_season_stream` (views) | streaming | `sql/02_views.sql` |
| Silver | `player_season` | batch (T1 + 2025-26) | notebooks do T1 + `sql/05` |
| Silver | `player_season_hybrid` (view) | **batch + streaming** | `sql/04_hybrid_view.sql` |
| Gold | `player_season_features`, `ranking_parcial_2027`, procedure `sp_ranking_parcial_2027` | híbrida | `sql/06_gold_hibrida.sql` |
| Gold | `predictions_2526`, `top30_2026`, `indicados_2026_vs_previsto`, `top3_2026_congelado` | batch | `sql/07_top30_2026.sql` |
| Gold | `model_btree_ballon_dor` | treinado no T1 | notebook Gold do T1 |

As tabelas de streaming são **particionadas por dia de ingestão**. A Silver de streaming é **clusterizada por liga e jogador**.

---

## 4. Estrutura do repositório

| Pasta / arquivo | O que tem |
|---|---|
| `producer/publisher.py` | Produtor local: replay do CSV do crawler para o Pub/Sub (`--loop`, `--limit`, `--dry-run`). |
| `producer/crawlerlimpezaalemacompleto_corrigido.py` | Crawler do FBref (roda fora da nuvem). |
| `job/` | Cloud Run Job do produtor: `run_job.py`, `Dockerfile`, `requirements.txt`, `cloudbuild.yaml`. |
| `pipeline/streaming_pipeline.py` | Pipeline Apache Beam. Roda no Dataflow ou localmente com um arquivo. |
| `ballon_common/` | Regras da Silver do T1 compartilhadas pelo Dataflow (`preprocessing.py`) e detecção do projeto (`gcp.py`). |
| `api/` | API REST: `main.py` (FastAPI), `Dockerfile`, `requirements.txt`, `cloudbuild.yaml`, `validate.py`. |
| `sql/01` a `04` | Tabelas e views de streaming, consultas da demo e view híbrida. |
| `sql/05` a `08` | Batch 2025-26, Gold híbrida, Top 30 de 2026 e export do modelo. |
| `infra/` | Scripts para o Cloud Shell: `00_config` (configuração), `01_setup`, `02_run_pipeline`, `03_drain`, `04_run_producer` e `05_deploy_partes_3_4` (Vertex AI, Cloud Run Job, Scheduler e API). |
| `notebooks_t1/` | Notebooks de ingestão e Silver do T1, com o projeto detectado automaticamente. |
| `tests/` | Testes das regras, do grafo do Beam, do job e da API (rodam sem GCP). |
| `roteiro_demo_streaming.md` | Passo a passo para testar o fluxo completo e para a demo. |

---

## 5. Pré-requisitos

- Projeto GCP com faturamento ativo.
- **Cloud Shell**, que já traz `gcloud`, `bq`, Docker (via Cloud Build) e Python 3.
- Batch do T1 já carregado: `silver.player_season`, `gold.player_season_features` e o modelo `gold.model_btree_ballon_dor`, criados pelos notebooks do T1.
- Bucket `gs://futdados` com os CSVs do T1.

```bash
git clone https://github.com/FSegurado/PDM_trabalho_2 && cd PDM_trabalho_2
gcloud config set project trabalho-pdm-joao-fut
```

> **Projeto e bucket.** Os scripts de `infra/`, o pipeline, o produtor e o `sql/01` a `04` usam o projeto ativo do `gcloud`, sem ID fixo. Já o `sql/05` a `08`, o `api/validate.py` e o `infra/05_deploy_partes_3_4.sh` citam `trabalho-pdm-joao-fut` e `gs://futdados`, que são os do T1. Em outro projeto, crie um bucket e troque os dois nomes antes de rodar:
> ```bash
> sed -i 's/trabalho-pdm-joao-fut/NOVO_PROJETO/g; s#gs://futdados#gs://NOVO_BUCKET#g' \
>   sql/0[5-8]_*.sql api/validate.py infra/05_deploy_partes_3_4.sh
> ```

---

## 6. Como executar, do zero

Todos os comandos rodam no Cloud Shell, na raiz do repositório. A ordem importa.

### 6.1 Streaming: infraestrutura (uma vez, ≈2 min)

```bash
bash infra/01_setup.sh
```

Ativa as APIs e cria o tópico, a subscription, o bucket temporário do Dataflow, a conta de serviço, as tabelas e views de streaming e a view híbrida. Pode rodar de novo sem problema.

> A subscription precisa existir **antes** do produtor publicar. Mensagens publicadas antes dela se perdem.

### 6.2 Batch 2025-26 e Gold híbrida (uma vez)

```bash
# CSV da temporada 2025-26 gerado pelo crawler
gcloud storage cp players_stats_2526.csv gs://futdados/bronze/

bq query --use_legacy_sql=false < sql/05_batch_2526.sql   # Bronze + Silver 2025-26, resultado 2026
bq query --use_legacy_sql=false < sql/06_gold_hibrida.sql # procedure + Gold híbrida + ranking 2027
bq query --use_legacy_sql=false < sql/07_top30_2026.sql   # Top 30 / Top 3 de 2026 x indicados reais
```

### 6.3 Modelo no Vertex AI, produtor no Cloud Run Job e API no Cloud Run (uma vez, ≈10 min)

```bash
# snapshot da temporada em andamento para o produtor
gcloud storage cp players_stats_final.csv gs://futdados/cache/players_stats_2627.csv \
  --custom-metadata=snapshot_date=$(date +%F)

bash infra/05_deploy_partes_3_4.sh
```

O script pode rodar de novo; o que já existe é atualizado. Ele faz, em ordem:
1. ativa as APIs do Cloud Run, Cloud Build, Artifact Registry, Cloud Scheduler e Vertex AI;
2. cria o repositório `ballon-dor` no Artifact Registry;
3. cria as contas de serviço do job, da API e do Scheduler, só com as permissões necessárias;
4. roda o `sql/08` (export do modelo e das estatísticas) se ainda não houver export e registra o modelo `ballon-dor-btree` no Vertex AI;
5. gera as imagens do job e da API com o Cloud Build;
6. publica o Cloud Run Job `producer-fbref` e agenda o Cloud Scheduler para 06:17;
7. publica a API `ballon-dor-api`: privada, de 0 a 2 instâncias, 1 GiB.

### 6.4 Rodar o streaming (a cada teste)

```bash
bash infra/02_run_pipeline.sh                  # sobe o Dataflow (3 a 5 min até "Running")

# produtor na nuvem: a mesma execução que o Scheduler faz
gcloud run jobs execute producer-fbref --region us-central1
#   mais rápido / menor:  --update-env-vars DELAY=0.1   ou   --update-env-vars LIMIT=300,DELAY=0.2

# alternativa: produtor local com o CSV no Cloud Shell
bash infra/04_run_producer.sh players_stats_final.csv --delay 0.3 --limit 300
```

Acompanhe pelo grafo em **Dataflow → Jobs → ballon-dor-streaming** e pelas consultas de `sql/03_demo_queries.sql` (eventos por minuto, funil bruto → tratado → rejeitado, latência e view híbrida).

### 6.5 Atualizar a Gold e o ranking parcial (sob comando)

```sql
CALL `trabalho-pdm-joao-fut.gold.sp_ranking_parcial_2027`();
SELECT * FROM gold.ranking_parcial_2027 ORDER BY posicao_prevista LIMIT 10;
```

### 6.6 Desligar (sempre ao terminar)

```bash
bash infra/03_drain.sh
gcloud dataflow jobs list --region us-central1 --status active   # até "Listed 0 items"
```

---

## 7. Chamar a API

A API é **privada**: só responde a quem tem o papel `roles/run.invoker` no serviço. A chamada leva um token de identidade do Google.

```bash
URL=$(gcloud run services describe ballon-dor-api --region us-central1 --format 'value(status.url)')
TOKEN=$(gcloud auth print-identity-token)

curl -H "Authorization: Bearer $TOKEN" "$URL/health"     # {"status":"ok"}
curl -H "Authorization: Bearer $TOKEN" "$URL/model"      # modelo do Vertex AI, features e temporadas

curl -X POST "$URL/predict" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"instances":[{"season":2526,"league":"ENG-Premier League","pos_primary":"FW",
                     "minutes":2900,"age":25,"goals_per90":0.95,"assists_per90":0.15}]}'
```

Resposta:

```text
{"modelo": {"vertex": "projects/.../locations/us-central1/models/...", "versao": "1"},
 "predicoes": [{"season": 2526, "predicted_ballon_dor_score": <score previsto>}]}
```

| Endpoint | Método | O que faz |
|---|---|---|
| `/health` | GET | `ok` quando o modelo está carregado. |
| `/model` | GET | Registro do Vertex AI usado, lista de features e temporadas disponíveis para o z-score. |
| `/predict` | POST | Recebe `instances` com as estatísticas **cruas** de um ou mais jogadores e devolve o score previsto de cada um. |

**Entrada do `/predict`:** `season` (código do T1, ex.: `2526` = 2025-26), `league`, `pos_primary`, `minutes`, `age`, `goals_per90` e `assists_per90`. Qualquer outra feature crua do T1 também é aceita (`xg_per90`, `kp_per90`...). Campo ausente recebe a média do treino, como o BigQuery ML faz.

**Como a API carrega o modelo:** na subida, ela consulta o Vertex AI Model Registry (`ballon-dor-btree`), baixa do GCS o artefato que o registro aponta e as estatísticas de pré-processamento ao lado dele. Depois aplica o mesmo z-score por temporada da Gold e o mesmo encoding do BQML.

**Postman:** POST em `<URL>/predict`. Na aba Authorization, escolha Bearer Token e cole a saída de `gcloud auth print-identity-token` (vale 1 h).

**Validação contra o BigQuery:** `python3 api/validate.py "$URL"` compara a API com o `ML.PREDICT` numa amostra exportada pelo `sql/08`.

**Dar acesso a outra pessoa:**
```bash
gcloud run services add-iam-policy-binding ballon-dor-api --region us-central1 \
  --member="user:EMAIL_DA_PESSOA" --role="roles/run.invoker"
```

---

## 8. Teste local (sem GCP)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q          # regras, grafo do Beam, job e API

# pipeline inteiro na máquina, a partir do CSV real
python producer/publisher.py --cache players_stats_final.csv --only-season 2627 --dry-run mensagens.jsonl
python pipeline/streaming_pipeline.py --input_file mensagens.jsonl --output_dir saida_local
# saida_local/bronze_raw.jsonl · silver.jsonl · errors.jsonl
```

Os testes da API e do job são pulados se `xgboost`, `fastapi` ou as bibliotecas do Google Cloud não estiverem instalados. Para incluí-los: `pip install -r api/requirements.txt -r job/requirements.txt`.

---

## 9. Decisões técnicas

**Streaming (partes 1 e 2)**
- **O crawler roda fora da nuvem; o produtor, dentro.** O FBref bloqueia acessos automatizados vindos de IPs de nuvem. O crawler grava um snapshot no GCS, e o Cloud Run Job (agendado) faz o replay para o Pub/Sub.
- **O produtor não transforma nada nem injeta erros.** Limpeza é papel do pipeline, e só chegam à dead-letter problemas reais do crawler.
- **O Dataflow faz o que é por evento; as views fazem o que junta linhas.** Transferências, métricas por 90 min e corte de minutos dependem de várias mensagens do mesmo jogador. Por isso ficam nas views, que aplicam as regras da Silver do T1 sobre a última coleta. No CSV real de 2025-26, a view devolve os mesmos 1.597 jogadores que a lógica pandas do T1.
- **Bruto e tratado gravados juntos.** O evento original vai para a Bronze antes de qualquer transformação. Se uma regra mudar, dá para reprocessar.
- **Dead-letter em tabela.** JSON quebrado, campo obrigatório vazio, texto em campo numérico, valor negativo, temporada inválida e linhas recusadas pelo BigQuery não derrubam o job: vão para `bronze.player_stats_stream_errors` com a etapa e o motivo.
- **Deduplicação em duas camadas.** O Pub/Sub entrega pelo menos uma vez. O Dataflow descarta repetições idênticas numa janela de 1 dia (estado por chave de conteúdo), e a view usa só a última coleta de cada jogador e clube.
- **Particionamento por dia de ingestão e clusterização por liga e jogador.** As consultas filtram "hoje" e agrupam por jogador. A consulta 8 do `sql/03` mostra a diferença em bytes processados.
- **Streaming inserts.** A linha fica consultável em segundos.
- **Corte de minutos proporcional.** O T1 usa 900 min na temporada inteira. No streaming, o corte vira `900 × rodadas jogadas ÷ rodadas totais da liga` (38 rodadas, ou 34 na Bundesliga e na Ligue 1).

**Gold, modelo e API (partes 3 e 4)**
- **O modelo não foi retreinado.** A Gold híbrida usa a mesma query do T1 lendo a view híbrida. Como o z-score é por temporada, as linhas históricas ficam idênticas. O modelo foi treinado só com temporadas encerradas: treino 2017-18 a 2021-22, teste 2022-23 e 2023-24.
- **`ML.PREDICT` só sob comando.** Nada fica agendado nem embutido em view: a procedure `gold.sp_ranking_parcial_2027` é chamada quando há dados novos.
- **Vertex AI sem endpoint.** O modelo fica registrado (versão, artefato, contêiner `xgboost-cpu 1.7`) sem custo por hora. A API carrega o artefato apontado pelo registro.
- **API privada, com contas de serviço de acesso mínimo.** `--no-allow-unauthenticated`. O acesso é liberado por pessoa com `roles/run.invoker`.
- **Escala a zero.** A API tem de 0 a 2 instâncias. A primeira chamada depois de um tempo parado demora alguns segundos, porque baixa o modelo.

---

## 10. Evidências

- **API x BigQuery (03/11/2026):** em 45 jogadores de 2023-24, 2025-26 e 2026-27, a maior diferença entre a API e o `ML.PREDICT` foi de 0,00005 (`api/validate.py`).
- **Streaming x lógica do T1:** a view de streaming reproduz os 1.597 jogadores de 2025-26 da Silver pandas do T1.
- **Ballon d'Or 2026:** `gold.top30_2026` e `gold.indicados_2026_vs_previsto` comparam o Top 30 previsto com os 30 indicados reais. O `gold.top3_2026_congelado` foi gerado em 03/11/2026, depois da cerimônia de 26/10. Como o modelo foi treinado sem 2026, é um teste retrospectivo, não uma previsão feita antes.

---

## 11. Limitações conhecidas

1. **O crawler traz só a tabela "standard" do FBref** (minutos, gols, assistências, pênaltis e cartões). xG, chutes, passes, progressão, defesa e `Team Success_PPM` ficam nulos em 2025-26 e 2026-27 e recebem a média do treino. Na prática, as predições dessas temporadas dependem de gols, assistências, minutos, idade, liga e posição.
2. **Sem contexto de time** (`team_rank_proxy`, `is_*_proxy`) nas temporadas do crawler, pelo mesmo motivo.
3. **Idade no formato "anos-dias".** Na temporada em andamento, o FBref grava `29-008`. O módulo comum lê como 29.
4. **Estatísticas da API congeladas no export.** A média e o desvio da 2026-27 usados pela API são os do dia do `sql/08`. Depois de um novo `CALL`, os scores da API para 2026-27 podem diferir um pouco do `gold.ranking_parcial_2027`. Para alinhar, rode de novo os dois `EXPORT DATA` do `sql/08`.
5. **Campanha em seleções** (Copa do Mundo) fica fora do modelo.

---

## 12. Custos

| Recurso | Cobrança |
|---|---|
| Dataflow (1 worker `e2-standard-2`, Streaming Engine) | Por hora enquanto ligado, mesmo sem mensagens. **Ligue só para testar ou apresentar e rode `infra/03_drain.sh`.** |
| Pub/Sub, streaming inserts, BigQuery | Centavos neste volume (~2 mil eventos por varredura). |
| Cloud Run (API com 0 instâncias mínimas, Job) | Só durante as requisições ou execuções. |
| Vertex AI Model Registry (sem endpoint) | Sem custo por hora; só o armazenamento no GCS. |

---

## 13. Notebooks do T1 com projeto automático

`notebooks_t1/` tem a ingestão e a Silver com uma célula nova no topo (`get_project_id()`), no lugar do ID fixo. No BigQuery Studio o projeto é detectado sozinho. Em outro ambiente, defina antes `os.environ["GOOGLE_CLOUD_PROJECT"] = "seu-projeto"`.

- Na ingestão, o bucket `futdados` continua fixo, porque nome de bucket é global. Num projeto novo, crie um bucket e troque `BUCKET_NAME`.
- O notebook da Gold e do treino do T1 (`gold_ballon_dor.ipynb`) cria `gold.player_season_features` e `gold.model_btree_ballon_dor`. Para rodá-lo em outro projeto, faça a mesma troca: cole a célula do `get_project_id()` e mude `PROJECT = "trabalho-pdm-joao-fut"` para `PROJECT = PROJECT_ID`.
