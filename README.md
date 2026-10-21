# Trabalho 2 · Parte 2: pipeline de processamento em tempo real

Pipeline de streaming do preditor Ballon d'Or. Ele lê os eventos do crawler no Pub/Sub, trata cada um no Dataflow (Apache Beam) e grava no BigQuery, ao lado das tabelas batch do Trabalho 1.

```
Produtor (replay do cache do crawler, só 2026-27)
   │  JSON, 1 jogador a cada 0,5 s
   ▼
Pub/Sub  tópico fbref-player-stats-stream ─► subscription fbref-player-stats-dataflow
   │
   ▼
Dataflow (streaming, us-central1)
   ├─[A]──────────────────────────────► bronze.player_stats_stream_raw     evento bruto
   ├─ 1 parse + schema
   ├─ 2 seleção e tipagem      (T1 · passo 1)
   ├─ 3 padronização de nomes  (T1 · passo 4)
   ├─ 4 deduplicação
   ├──────────────────────────────────► silver.player_stats_stream         evento tratado
   └─ inválidos ──────────────────────► bronze.player_stats_stream_errors  dead-letter
                                                    │
BigQuery (views, mesmas regras da Silver do T1)     ▼
   silver.player_season_stream_all   última coleta + transferências + por 90 + corte de minutos
   silver.player_season_stream       só os qualificados (mesmo schema da silver.player_season)
   silver.player_season_hybrid       batch (T1) + streaming lado a lado ◄── ponto de encontro
```

## Estrutura do repositório

| Pasta / arquivo | O que faz |
|---|---|
| `ballon_common/preprocessing.py` | Regras linha a linha da Silver do T1 (tipagem, nomes, chaves). É o **módulo comum** usado pelo Dataflow e, depois, pela API. |
| `ballon_common/gcp.py` | `get_project_id()`: descobre o projeto GCP sozinho. Nenhum ID fixo no código. |
| `pipeline/streaming_pipeline.py` | O job Beam. Roda no Dataflow ou localmente com um arquivo de teste. |
| `producer/publisher.py` | Produtor da parte 1, ajustado ao CSV real do crawler (separador `;`, filtro de temporada, data da coleta). |
| `sql/01_tables.sql` | Tabelas de streaming: bruto, tratado e dead-letter (particionadas por dia). |
| `sql/02_views.sql` | Views da Silver de streaming (passos 2 e 6 do T1). |
| `sql/03_demo_queries.sql` | Consultas prontas para a demo ao vivo. |
| `sql/04_hybrid_view.sql` | View híbrida batch + streaming (precisa da Silver do T1 no projeto). |
| `infra/*.sh` | Scripts para o Cloud Shell: setup, subir job, ligar produtor, desligar job. |
| `tests/` | Testes das regras e do grafo do Beam (rodam sem GCP). |
| `notebooks_t1/` | Notebooks de ingestão e Silver do T1 com o ID do projeto automático. |

## Passo a passo no Cloud Shell

Tudo roda na nuvem. O Cloud Shell já vem com `gcloud`, `bq` e Python.

```bash
# 0) suba esta pasta para o Cloud Shell (ou git clone do repositório) e entre nela
cd parte2_streaming
gcloud config set project SEU_PROJETO        # o único lugar onde o projeto aparece

# 1) cria APIs, tópico, subscription, bucket, conta de serviço, tabelas e views (≈2 min)
bash infra/01_setup.sh

# 2) sobe o job de streaming no Dataflow (3 a 5 min até ficar "Running")
bash infra/02_run_pipeline.sh

# 3) liga o produtor com o CSV do crawler (publica só 2026-27)
bash infra/04_run_producer.sh players_stats_final.csv
#    demo: mais rápido, em loop e com 1 evento quebrado a cada 40 para mostrar o dead-letter
bash infra/04_run_producer.sh players_stats_final.csv --delay 0.3 --loop --bad-every 40

# 4) acompanhe: console do Dataflow (grafo com contadores) + sql/03_demo_queries.sql no BigQuery

# 5) SEMPRE no fim: desliga o job (senão ele continua cobrando)
bash infra/03_drain.sh
```

**Ordem importa:** a subscription é criada no passo 1. Mensagens publicadas antes de ela existir se perdem. Depois disso, o produtor pode rodar mesmo com o job desligado: as mensagens esperam na subscription por até 1 dia.

**Trocar de projeto:** basta `gcloud config set project NOVO_PROJETO` e rodar o passo 1 de novo. Os scripts, o pipeline, o produtor e o SQL pegam o projeto ativo.

## Teste local (sem GCP)

```bash
pip install -r requirements.txt
python -m pytest -q                                   # 19 testes: regras + grafo do Beam

# gera mensagens a partir do CSV real e roda o pipeline inteiro na máquina
python producer/publisher.py --cache players_stats_final.csv --only-season 2627 \
       --dry-run mensagens.jsonl --bad-every 100
python pipeline/streaming_pipeline.py --input_file mensagens.jsonl --output_dir saida_local
# saida_local/bronze_raw.jsonl · silver.jsonl · errors.jsonl
```

## Decisões técnicas (para a apresentação)

- **Dataflow faz o que é por evento; a view faz o que junta linhas.** Transferências, por 90 e corte de minutos dependem de várias mensagens do mesmo jogador. Por isso ficam numa view, que aplica as regras da Silver do T1 sobre a última coleta. Conferido no CSV real: para 2025-26, a view devolve exatamente os mesmos 1.597 jogadores e clubes principais que a lógica pandas do T1.
- **Bruto e tratado gravados juntos.** O evento original vai para a Bronze antes de qualquer transformação. Se uma regra mudar, dá para reprocessar.
- **Dead-letter em tabela.** JSON quebrado, campo obrigatório vazio, texto em campo numérico, valor negativo ou temporada inválida não derrubam o job. Vão para `bronze.player_stats_stream_errors` com a etapa e o motivo. Linhas que o BigQuery recusar também vão para lá.
- **Deduplicação em duas camadas.** O Pub/Sub entrega "pelo menos uma vez" e o produtor em loop reenvia tudo. O Dataflow descarta repetições idênticas dentro de uma janela de 1 dia (estado por chave de conteúdo). A view ainda escolhe só a última coleta de cada jogador e clube.
- **Particionamento por dia de ingestão e clusterização por liga e jogador.** As consultas da demo filtram "hoje" e agrupam por jogador. Compare os bytes processados na consulta 8 do `03_demo_queries.sql`.
- **Gravação via streaming inserts.** A linha fica consultável em segundos e não depende de Java no Cloud Shell, ao contrário da Storage Write API no Python.
- **Corte de minutos proporcional.** O T1 usa 900 min na temporada inteira. No streaming o corte vira `900 × rodadas jogadas ÷ rodadas totais da liga` (38, ou 34 na Bundesliga e na Ligue 1). Com a temporada completa, ele volta a ser 900.

## Limitações conhecidas (leia antes de ligar ao modelo)

1. **O crawler traz 25 colunas; o modelo do T1 usa 37 features.** O CSV do crawler tem só a tabela "standard" do FBref: minutos, gols, assistências, pênaltis e cartões. Não traz xG, chutes, passes, progressão, defesa nem `Team Success_PPM`. Na view, essas colunas existem mas ficam **nulas**. Para a parte 2 isso não atrapalha. Para prever com o modelo do T1, ~80% das features chegam vazias, e o mesmo vale para a 2025-26 se ela vier deste crawler. Há duas saídas: o crawler coletar as outras tabelas do FBref, ou treinar uma versão do modelo só com as features disponíveis nas duas fontes.
2. **Contexto de time (passo 3) indisponível** pelo mesmo motivo (sem `Team Success_PPM`). `team_rank_proxy` e `is_*_proxy` ficam nulos.
3. **Idade no formato "anos-dias".** Na temporada em andamento o FBref grava `29-008`. O módulo comum lê como 29. A regra antiga do T1 (`to_numeric_eu`) transformaria isso em nulo.
4. **Arredondamento.** Em 1 de 1.597 jogadores o `goals_per90` difere na 4ª casa decimal (0,2812 no pandas vs. 0,2813 no BigQuery), porque cada um arredonda o meio de um jeito.

## Notebooks do T1 com projeto automático

`notebooks_t1/` tem a ingestão e a Silver com uma célula nova no topo (`get_project_id()`). Todos os `"trabalho-pdm-joao-fut"` foram trocados por `PROJECT_ID`. No BigQuery Studio o projeto é detectado sozinho. Em outro ambiente, defina antes `os.environ["GOOGLE_CLOUD_PROJECT"] = "seu-projeto"`.

- Na ingestão, o bucket `futdados` continua fixo, porque nome de bucket é global. Num projeto novo, crie um bucket e troque `BUCKET_NAME`.
- No notebook da Gold, faça a mesma troca na primeira célula: cole a célula do `get_project_id()` (copie do notebook da Silver) e mude `PROJECT = "trabalho-pdm-joao-fut"` para `PROJECT = PROJECT_ID`.

## Custos

O job usa 1 worker `e2-standard-2` com Streaming Engine e cobra por hora enquanto estiver ligado, mesmo sem mensagens. Pub/Sub, streaming inserts e BigQuery custam centavos neste volume (~2 mil eventos por varredura). Ligue o job só para testar e para a apresentação, e rode `infra/03_drain.sh` ao terminar.
