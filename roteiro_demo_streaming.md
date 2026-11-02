# Roteiro da demo: streaming do Trabalho 2

Preditor Ballon d'Or · partes 1 e 2 (Pub/Sub → Dataflow → BigQuery)
Projeto GCP: `trabalho-pdm-joao-fut` · Região do Dataflow: `us-central1`

---

## Antes do dia (uma vez)

```bash
gcloud billing projects describe trabalho-pdm-joao-fut    # tem que vir billingEnabled: true
```

Se vier `false`, o crédito acabou ou venceu. Resolva antes da apresentação: novo cupom do professor ou outra conta de faturamento.

---

## T-15 min: preparar e ligar o Dataflow

```bash
cd ~/PDM_trabalho_2
git pull
gcloud config set project trabalho-pdm-joao-fut
export GOOGLE_CLOUD_PROJECT=trabalho-pdm-joao-fut
source .venv/bin/activate
pip install -q setuptools wheel build        # evita o erro de empacotamento
ls -lh players_stats_final.csv               # o CSV precisa estar aqui

bash infra/02_run_pipeline.sh
```

- No terminal, confira `projeto=trabalho-pdm-joao-fut` e espere a linha **"Job enviado"**.
- No console, abra **Dataflow → Jobs → ballon-dor-streaming** e espere o status **Running** (3 a 5 min).

---

## Na apresentação: demo ao vivo

### 1. Ligar o produtor (no terminal)

```bash
bash infra/04_run_producer.sh players_stats_final.csv --delay 0.3 --bad-every 40
```

Mostre as linhas `[Pub/Sub]` passando e o "evento inválido injetado".

### 2. Mostrar o grafo do Dataflow

Clique nas caixas para mostrar os contadores subindo.

### 3. Rodar as consultas no BigQuery Studio

Consultas do arquivo `sql/03_demo_queries.sql`, nesta ordem:

| # | O que falar |
|---|---|
| 1 | Os eventos chegam minuto a minuto. Rode duas vezes e o número cresce. |
| 3 | Funil: bruto → tratado → rejeitado. |
| 4 | Dead-letter: o evento quebrado com a etapa e o motivo. O job não cai. |
| 2 | Latência de poucos segundos entre o Pub/Sub e a Silver. |
| 7 | **Batch + streaming lado a lado** na view híbrida (o ponto principal). |
| 6 | Corte de minutos proporcional por liga (se der tempo). |

### 4. Parar o produtor

Aperte **Ctrl + C** no terminal.

---

## Logo depois: desligar

```bash
bash infra/03_drain.sh
gcloud dataflow jobs list --region us-central1 --status active    # repita até "Listed 0 items"
```

> O job cobra por hora enquanto está ligado, mesmo sem mensagens. Não pule esta etapa.

---

## Se algo der errado na hora

| Sintoma | Solução |
|---|---|
| `Billing account ... is not open` | O crédito acabou. Não tem como resolver na hora; mostre os prints dos testes. |
| `setup.py sdist ... returned non-zero exit status 1` | Rode `pip install setuptools wheel build` com o `.venv` ativo e rode o `02_run_pipeline.sh` de novo. |
| Job Running, mas a consulta 1 vem vazia | Rode `echo $GOOGLE_CLOUD_PROJECT`. Se estiver errado, refaça o `export` e ligue o produtor de novo. |
| `players_stats_final.csv` não encontrado | Envie de novo pelo menu ⋮ → Upload e rode `mv ~/players_stats_final.csv .` |
| Job demorando para ficar Running | É normal até ~5 min. Por isso ele é ligado 15 min antes. |

**Plano B:** leve os prints do teste de 26/10 (grafo do Dataflow, consultas 3, 4 e 7). Se o GCP falhar na hora, eles mostram que a arquitetura funcionou.
