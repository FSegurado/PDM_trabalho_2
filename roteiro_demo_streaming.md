# Roteiro: teste do fluxo completo (partes 1 a 4)

Preditor Ballon d'Or · Batch (T1) + Streaming + Vertex AI + API REST
Projeto GCP: `trabalho-pdm-joao-fut` · Região: `us-central1` · BigQuery: `US`

```
                         BATCH (T1 + 2025-26)                         STREAMING (2026-27)
CSVs históricos ─► bronze ─► silver.player_season          Crawler FBref (fora da nuvem)
crawler 2025-26 ─► sql/05 ──┘          │                          │ gs://futdados/cache/players_stats_2627.csv
                                       │                   Cloud Scheduler 06:17 ─► Cloud Run Job producer-fbref
                                       │                          │ JSON, 1 evento por jogador
                                       │                   Pub/Sub fbref-player-stats-stream
                                       │                          │
                                       │                   Dataflow ballon-dor-streaming
                                       │                    ├─► bronze.player_stats_stream_raw
                                       │                    ├─► silver.player_stats_stream ─► views stream_all / stream
                                       │                    └─► bronze.player_stats_stream_errors (dead-letter)
                                       ▼                          ▼
                         silver.player_season_hybrid  (batch + streaming, coluna `origem`)
                                       │  CALL gold.sp_ranking_parcial_2027()   ← só sob comando
                                       ▼
                         gold.player_season_features ─► ML.PREDICT (model_btree_ballon_dor, treinado só no histórico)
                           ├─► gold.predictions_2526 / top30_2026 / indicados_2026_vs_previsto / top3_2026_congelado
                           └─► gold.ranking_parcial_2027
                                       │  EXPORT MODEL (sql/08)
                                       ▼
                         gs://futdados/models/ballon_dor_btree/v1/ ─► Vertex AI Model Registry: ballon-dor-btree
                                       ▼
                         Cloud Run ballon-dor-api (privada, IAM) ─ POST /predict → JSON
```

Duração do teste completo: ~40 min (15 deles esperando o Dataflow subir).

---

## 0. Antes do dia (uma vez)

No Cloud Shell:

```bash
cd ~/PDM_trabalho_2 && git pull
gcloud config set project trabalho-pdm-joao-fut
export GOOGLE_CLOUD_PROJECT=trabalho-pdm-joao-fut

gcloud billing projects describe trabalho-pdm-joao-fut --format 'value(billingEnabled)'   # tem que ser True
```

Confira se as partes 3 e 4 estão no ar. Cada linha tem que devolver algo:

```bash
gcloud storage ls -l gs://futdados/cache/players_stats_2627.csv                          # snapshot do crawler
gcloud run jobs describe producer-fbref --region us-central1 --format 'value(metadata.name)'
gcloud scheduler jobs describe producer-fbref-diario --location us-central1 --format 'value(state,schedule)'
gcloud ai models list --region us-central1 --filter displayName=ballon-dor-btree --format 'value(name)'
gcloud run services describe ballon-dor-api --region us-central1 --format 'value(status.url)'
```

Se faltar algo, rode na ordem do README: `sql/05` → `sql/06` → `sql/07` → `sql/08` → `bash infra/05_deploy_partes_3_4.sh`.

Quer dados mais novos da 2026-27? Rode o crawler na sua máquina e suba o snapshot:

```bash
gcloud storage cp players_stats_final.csv gs://futdados/cache/players_stats_2627.csv \
  --custom-metadata=snapshot_date=$(date +%F)
```

---

## 1. T-20 min: preparar

### 1.1 Limpar a fila do Pub/Sub (importante)

O Cloud Scheduler publica o snapshot inteiro todo dia às 06:17. Essas mensagens ficam esperando na subscription. Se o Dataflow subir com essa fila cheia, ele trata tudo de uma vez. Aí a deduplicação descarta o replay ao vivo (mesmo conteúdo) e a Silver não cresce na hora da demo. Descarte a fila antes de ligar o job:

```bash
gcloud pubsub subscriptions seek fbref-player-stats-dataflow \
  --time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

### 1.2 Ligar o Dataflow

```bash
source .venv/bin/activate 2>/dev/null; pip install -q setuptools wheel build
gcloud dataflow jobs list --region us-central1 --status active   # tem que vir vazio
bash infra/02_run_pipeline.sh
```

- No terminal: confira `projeto=trabalho-pdm-joao-fut` e espere **"Job enviado"**.
- No console: **Dataflow → Jobs → ballon-dor-streaming** até ficar **Running** (3 a 5 min).

### 1.3 Aquecer a API (evita o cold start)

A API sobe com `min-instances 0`. Na primeira chamada ela consulta o Vertex AI e baixa o modelo do GCS, o que leva alguns segundos. Faça uma chamada antes:

```bash
URL=$(gcloud run services describe ballon-dor-api --region us-central1 --format 'value(status.url)')
TOKEN=$(gcloud auth print-identity-token)
curl -s -H "Authorization: Bearer $TOKEN" "$URL/health"          # {"status":"ok"}
```

> Na apresentação, para não arriscar: `gcloud run services update ballon-dor-api --region us-central1 --min-instances 1`. Isso cobra enquanto estiver ligado; volte para 0 no passo 6.

---

## 2. Batch: o histórico já está no BigQuery

No BigQuery Studio, consulta 7 do `sql/03_demo_queries.sql`:

```sql
SELECT origem, season_label, COUNT(*) AS jogadores, ROUND(AVG(goals_per90), 3) AS media_gols_90
FROM silver.player_season_hybrid
GROUP BY origem, season_label
ORDER BY season_label;
```

✅ Esperado: `batch` de 2017-18 até **2025-26** (a 2025-26 veio do `sql/05`) e `streaming` em **2026-27** (se já houve teste antes).

O que falar: o batch cuida das temporadas encerradas; o streaming, da temporada em andamento. As duas se encontram nesta view, com o mesmo schema.

---

## 3. Streaming: produtor → Pub/Sub → Dataflow → BigQuery

### 3.1 Disparar o produtor (Cloud Run Job)

Execução manual, a mesma que o Scheduler faz às 06:17. No teste completo, publique tudo, mais rápido que o padrão:

```bash
gcloud run jobs execute producer-fbref --region us-central1 --update-env-vars DELAY=0.1
#  ~2 mil jogadores × 0,1 s ≈ 3 a 4 min
#  na apresentação, mais curto:  --update-env-vars LIMIT=300,DELAY=0.2   (≈ 1 min)
```

Acompanhe em **Cloud Run → Jobs → producer-fbref → Execuções → Registros**: linhas `inicio`, `progresso` e `fim`.

### 3.2 Grafo do Dataflow

Clique nas caixas `LePubSub`, `1-3 ParseTipagemNomes`, `4 Deduplicacao` e `GravaSilverBQ`. Os contadores de elementos sobem.

### 3.3 Consultas no BigQuery (`sql/03_demo_queries.sql`)

| # | O que mostrar | ✅ Esperado |
|---|---|---|
| 1 | Eventos por minuto. Rode duas vezes. | O número do minuto atual cresce. |
| 3 | Funil bruto → tratado → rejeitado. | `brutos ≈ tratados` na primeira execução do dia. |
| 4 | Dead-letter. | Normalmente vazia: o produtor não injeta erros. A validação está ativa, e os casos de erro estão nos `tests/`. |
| 2 | Latência Pub/Sub → Silver. | Poucos segundos. |
| 5 | Top G+A por 90 na Silver de streaming (view). | Jogadores da 2026-27 já qualificados. |
| 6 | Corte proporcional de minutos por liga. | `corte_minutos` < 900, proporcional às rodadas. |

### 3.4 Teste da deduplicação (opcional, 1 min)

Publique de novo uma parte do mesmo snapshot:

```bash
gcloud run jobs execute producer-fbref --region us-central1 --update-env-vars LIMIT=50,DELAY=0.1 --wait
```

Rode a consulta 3 de novo. ✅ `brutos` sobe 50, `tratados` não muda. O Bronze guarda tudo; a Silver descarta a repetição.

> Isso vale enquanto o mesmo job do Dataflow estiver ligado e no mesmo dia (UTC). Se o job for drenado e ligado de novo, o estado zera, e a view continua pegando só a última coleta.

---

## 4. Gold: batch + streaming no mesmo modelo

### 4.1 Ranking parcial 2027 (sob comando)

```sql
CALL `trabalho-pdm-joao-fut.gold.sp_ranking_parcial_2027`();

SELECT origem, split, season_label, COUNT(*) AS jogadores
FROM gold.player_season_features
GROUP BY origem, split, season_label
ORDER BY season_label;

SELECT posicao_prevista, player, club_main, league, minutes, score_previsto,
       rodadas_jogadas, rodadas_total, ultima_coleta
FROM gold.ranking_parcial_2027
ORDER BY posicao_prevista
LIMIT 10;
```

✅ Esperado: Gold com `train`/`test` no histórico e `inference` em 2025-26 e 2026-27 (`origem = streaming`), e o top 10 parcial da 2026-27 com `gerado_em` de agora.

### 4.2 Ballon d'Or 2026: previsto x real

```sql
SELECT * FROM gold.top3_2026_congelado ORDER BY posicao_prevista;

SELECT posicao_prevista, player, club_main, score_previsto, indicado_real, rank_real
FROM gold.top30_2026 ORDER BY posicao_prevista;

SELECT rank_real, jogador_real, previsto.posicao_prevista, previsto.player, status_casamento
FROM gold.indicados_2026_vs_previsto ORDER BY rank_real;
```

O que falar: quantos dos 30 indicados reais o modelo pôs no top 30 (consulta de conferência no fim do `sql/07`). O modelo foi treinado só com o histórico, então não viu o resultado de 2026. Mas o Top 3 foi "congelado" em 03/11, depois da cerimônia: apresente como teste retrospectivo, não como previsão feita antes.

---

## 5. MLOps: Vertex AI + API REST no Cloud Run

### 5.1 Modelo no Vertex AI Model Registry

```bash
gcloud ai models list --region us-central1 --filter displayName=ballon-dor-btree
```

No console: **Vertex AI → Model Registry → ballon-dor-btree** (artefato em `gs://futdados/models/ballon_dor_btree/v1/model/`, sem endpoint, então não cobra por hora).

### 5.2 API: privada, saúde e origem do modelo

```bash
URL=$(gcloud run services describe ballon-dor-api --region us-central1 --format 'value(status.url)')
TOKEN=$(gcloud auth print-identity-token)

curl -s -o /dev/null -w 'sem token: HTTP %{http_code}\n' "$URL/health"     # ✅ 403: API privada
curl -s -H "Authorization: Bearer $TOKEN" "$URL/health"                     # ✅ {"status":"ok"}
curl -s -H "Authorization: Bearer $TOKEN" "$URL/model" | python3 -m json.tool | head -15
#  ✅ registro_vertex.name = projects/.../models/...  → a API carregou o modelo pelo Vertex AI
```

### 5.3 Predição em tempo real, com latência

```bash
curl -s -w '\n⏱  %{time_total}s\n' -X POST "$URL/predict" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"instances":[
        {"season":2526,"league":"ENG-Premier League","pos_primary":"FW","minutes":2900,"age":25,"goals_per90":0.95,"assists_per90":0.15},
        {"season":2526,"league":"ESP-La Liga","pos_primary":"DF","minutes":3000,"age":28,"goals_per90":0.05,"assists_per90":0.10}
      ]}'
```

✅ Esperado: JSON com `predicoes` (o atacante com score bem maior que o zagueiro) e tempo bem abaixo de 1 s com a API aquecida.

**Postman:** POST em `$URL/predict`, aba Authorization → Bearer Token → cole a saída de `gcloud auth print-identity-token` (vale 1 h), e o mesmo corpo JSON.

### 5.4 Jogadores reais do streaming pela API (opcional)

```bash
bq query --use_legacy_sql=false --format=json \
  'SELECT player, league, pos_primary, minutes, age, goals_per90, assists_per90
   FROM silver.player_season_stream ORDER BY ga_per90 DESC LIMIT 3' > top3.json

python3 - <<'EOF' > pedido.json
import json
rows = json.load(open("top3.json"))
txt = ("player", "league", "pos_primary")
print(json.dumps({"instances": [{"season": 2627, **{k: (v if k in txt else float(v))
                  for k, v in r.items() if v is not None}} for r in rows]}))
EOF

curl -s -X POST "$URL/predict" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d @pedido.json | python3 -m json.tool
```

As predições vêm na mesma ordem dos jogadores do `top3.json`. O score pode diferir um pouco do `gold.ranking_parcial_2027`: a API usa média e desvio da 2026-27 do dia do export (`sql/08`), e o `CALL` recalcula com os dados de agora.

### 5.5 API x BigQuery (evidência para os slides)

```bash
python3 api/validate.py "$URL"
```

✅ Esperado: `45 jogadores | maior diferença API x ML.PREDICT = 0.0000x` e saída 0.

---

## 6. Logo depois: desligar

```bash
bash infra/03_drain.sh
gcloud dataflow jobs list --region us-central1 --status active    # repita até "Listed 0 items"

# só se você subiu para 1 no passo 1.3
gcloud run services update ballon-dor-api --region us-central1 --min-instances 0
```

> O Dataflow cobra por hora enquanto está ligado, mesmo sem mensagens. O Cloud Run Job, o Scheduler, a API com 0 instâncias e o modelo sem endpoint custam centavos ou nada. Podem ficar.

---

## Na apresentação (10 min): ordem enxuta

1. Diagrama (topo deste arquivo) — 2 min.
2. Consulta 7 (batch + streaming na view híbrida) — 1 min.
3. `gcloud run jobs execute ... LIMIT=300,DELAY=0.2` → grafo do Dataflow → consultas 1, 3 e 2 — 3 min.
4. `CALL sp_ranking_parcial_2027` + top 10 parcial, ou `top30_2026` — 1,5 min.
5. Vertex AI Model Registry + `curl` do `/predict` com o tempo na tela — 2 min.
6. Reserva — 0,5 min.

Deixe o Dataflow Running, a fila limpa (1.1) e a API aquecida (1.3) antes de começar.

---

## Se algo der errado

| Sintoma | Solução |
|---|---|
| `Billing account ... is not open` | O crédito acabou. Não dá para resolver na hora: use o plano B. |
| `setup.py sdist ... returned non-zero exit status 1` | `pip install setuptools wheel build` com o `.venv` ativo e rode o `02_run_pipeline.sh` de novo. |
| `02_run_pipeline.sh` recusa: job com esse nome já existe | Há um job ativo. Use esse, ou rode `03_drain.sh` e espere ele sair da lista. |
| Consulta 1 vazia com o job Running | Veja os registros do `producer-fbref` (terminou com `fim`?). Rode `echo $GOOGLE_CLOUD_PROJECT`. |
| `tratados` não cresce, `brutos` sim | A deduplicação descartou um replay repetido (a fila não foi limpa no 1.1 ou o job já tinha recebido o snapshot hoje). É o comportamento esperado: explique, ou drene e ligue o job de novo. |
| Consultas com `CURRENT_DATE()` vazias à noite | O BigQuery usa UTC: depois das 21h em Brasília já é o dia seguinte. Troque por `DATE(ingest_ts) >= CURRENT_DATE() - 1`. |
| Job falha com `snapshot não encontrado` | Suba o CSV para `gs://futdados/cache/players_stats_2627.csv` (passo 0). |
| API responde 403 com token | `TOKEN=$(gcloud auth print-identity-token)` de novo; sua conta precisa de `roles/run.invoker` (owner já tem). |
| `/predict` devolve 422 `temporada ... sem estatísticas` | Use uma temporada exportada (veja `temporadas_com_estatisticas` no `/model`). |
| Primeira chamada à API demorada | Cold start. Faça o passo 1.3 antes. |

**Plano B (produtor):** se o Cloud Run Job falhar, use o produtor local com o CSV no Cloud Shell:
`bash infra/04_run_producer.sh players_stats_final.csv --delay 0.3 --limit 300`

**Plano B (GCP fora do ar):** leve prints deste teste: execução do job, grafo do Dataflow, consultas 3 e 7, `ranking_parcial_2027`, `top30_2026`, Model Registry, `curl` do `/predict` com o tempo e a saída do `validate.py`.
