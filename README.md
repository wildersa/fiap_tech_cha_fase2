# Pipeline Batch Bovespa - Tech Challenge (Fase 2)

Este repositório contém o componente de **Ingestão** de um pipeline de dados completo para a B3. O objetivo é extrair dados históricos/diários, convertê-los para o formato otimizado Parquet e armazená-los no AWS S3 seguindo uma estrutura de particionamento por data.

## 📐 Arquitetura do Pipeline

- Ingestão: `ingestor.py` usa `yfinance` para extrair preços e grava arquivos Parquet particionados por dia em S3 (padrão: `raw/dt=YYYY-MM-DD/`).
- Transformação: AWS Glue (Spark) processa o raw e escreve o refined, particionando por data/ticker para consulta via Athena.
- Consumo: Athena / Spark para análises e agregações.

> Observação: a partição no raw usa a chave `dt=YYYY-MM-DD` (consistente com o padrão do repositório).
## 🎯 Decisões do Projeto

Para atender aos requisitos de Machine Learning Avançado e análise de dados, foram eleitos **10 ativos (Blue Chips)** que representam diferentes setores da economia brasileira, garantindo uma base de dados rica para as fases subsequentes de ETL e análise:

1. **VALE3.SA** (Mineração)
2. **PETR4.SA** (Petróleo)
3. **ITUB4.SA** (Setor Bancário)
4. **BBDC4.SA** (Setor Bancário)
5. **ABEV3.SA** (Consumo/Bebidas)
6. **WEGE3.SA** (Indústria/Bens de Capital)
7. **BBAS3.SA** (Setor Bancário)
8. **B3SA3.SA** (Serviços Financeiros/Bolsa)
9. **RENT3.SA** (Serviços/Locação de Veículos)
10. **SUZB3.SA** (Papel e Celulose)

## 🏗️ Arquitetura de Ingestão

- **Linguagem:** Python 3.10+
- **Bibliotecas Base:** `yfinance` (extração), `pandas` + `pyarrow` (processamento e Parquet), `boto3` (AWS S3).
- **Gerenciador de Dependências:** `Poetry`.
- **Formato de Saída:** Parquet (Requisito 2).
- **Particionamento (ingestão raw):** `raw/dt=YYYY-MM-DD/` — o `ingestor.py` gera um arquivo consolidado por data (`b3_stocks.parquet`). A separação por `ticker` e o particionamento adicional são realizados no **Glue** (zona `refined`), que grava `refined/date=YYYY-MM-DD/ticker=.../` para otimizar consultas via Athena (Requisito 6).

## 🚀 Como Executar

### Pré-requisitos

- Poetry instalado (`pip install poetry`)
- Conta na AWS com permissões de S3 e Glue.

### Configuração (.env)

Crie um arquivo `.env` na raiz do projeto seguindo o modelo abaixo para que o script possa se autenticar na AWS e identificar o destino dos dados:

```env
S3_BUCKET=nome-do-seu-bucket
AWS_ACCESS_KEY_ID=sua_key
AWS_SECRET_ACCESS_KEY=seu_secret
AWS_DEFAULT_REGION=us-east-1
# AWS_SESSION_TOKEN=token_se_necessario (comum em contas Lab/Academy)
```

### Instalação

```powershell
poetry install
```

### Execução Local (Validação)

Para testar a geração de arquivos Parquet particionados na sua máquina:

#### Uso (parâmetros)

- `tickers` (opcional, positional): lista de símbolos a serem baixados. Se omitido, usa a lista padrão definida em `ingestor.py`.
- `--period <period>`: janela de tempo para o download (ex.: `1d`, `5d`, `30d`, `6mo`). Use `30d` para os últimos 30 dias.
- `--local`: flag opcional — grava os arquivos localmente em `data/` ao invés de fazer upload para S3.

#### Exemplo — 30 dias (apenas este exemplo)

```powershell
# Gera arquivos locais para os últimos 30 dias (partições diárias)
poetry run python ingestor.py --period 30d --local
```

#### Exemplo — data única (uso do `--date`)

```powershell
# Baixa e grava apenas os dados do dia 2026-01-16 (inclusivo)
poetry run python ingestor.py --date 2026-01-16 --local
```

Os arquivos serão gerados em: `data/raw/dt=YYYY-MM-DD/b3_stocks.parquet` (arquivo consolidado por data). A separação por `ticker` e o refinamento são realizados no Glue e resultarão em objetos como `data/refined/date=YYYY-MM-DD/ticker=VALE3.SA/...` — ver seção 'Transformações / Glue' para detalhes.

### Execução para S3 (Produção)

Com o `.env` configurado, basta rodar:

```powershell
# Ingestão diária (1d) para todos os ativos eleitos
poetry run python ingestor.py --period 1d
```

> Observação: o particionamento por `ticker` e as agregações exigidas pelo challenge são executadas no **Glue (zona refined)** — o raw é consolidado por data; o Glue escreve `refined/date=YYYY-MM-DD/ticker=.../` para otimizar consultas via Athena.

## ⏱️ Intraday / `--interval` — comportamento e compatibilidade (IMPORTANTE)

- Armazenamento padrão: **timestamps são gravados em UTC** como `TIMESTAMP` (milissegundos) — este é o formato recomendado e compatível com AWS Glue / Spark. Os arquivos continuam **particionados por dia** em `dt=YYYY-MM-DD` (calendar day).
- O `yfinance` suporta intraday via `--interval` (ex.: `1m`, `5m`, `60m`); disponibilidade histórica varia por intervalo e ticker — intervals intradiários frequentemente têm histórico limitado.
- O `ingestor.py` mantém **a granularidade intradiária** nas linhas (ex.: vários registros no mesmo `dt` com horas diferentes) e **normaliza para UTC antes de gravar**.

### Exemplos de uso 📌

- Baixar barras horárias (últimos 7 dias) e gravar localmente:

```powershell
poetry run python ingestor.py VALE3.SA --period 7d --interval 60m --local
```

- Exemplo diário (comportamento legado permanece):

```powershell
poetry run python ingestor.py --period 30d --local
```

### Como o timestamp é exposto aos consumidores (Spark/Glue) 🔧

- Armazenamos um `trade_date` do tipo `TIMESTAMP` (valores representam instantes em UTC).
- Para consultas/visualização em horário brasileiro (America/Sao_Paulo) use funções de conversão no momento da leitura — não modifique os valores armazenados.

Spark (leitura + conversão para BRT):

```sql
-- Athena / Spark SQL example
SELECT
  from_utc_timestamp(trade_date, 'America/Sao_Paulo') AS trade_date_brt,
  to_date(from_utc_timestamp(trade_date, 'America/Sao_Paulo')) AS trade_day,
  ticker, open, close, volume
FROM parquet_table
WHERE dt = '2026-01-09';
```

PySpark (conversão / extrair dia):

```python
df = spark.read.parquet('s3://.../raw/')
df = df.withColumn('trade_date_brt', F.from_utc_timestamp(F.col('trade_date'), 'America/Sao_Paulo'))
df = df.withColumn('trade_day', F.to_date('trade_date_brt'))
```

Pandas (para inspeção local):

```python
import pandas as pd
df['trade_date_brt'] = pd.to_datetime(df['trade_date'], utc=True).dt.tz_convert('America/Sao_Paulo')
```

### Limitações e boas práticas ⚠️

- Intervalos intradiários podem ser truncados pelo Yahoo; valide `period` × `interval` antes de rodar cargas longas. O script já emite avisos para combinações potencialmente incompatíveis.
- Não converta timestamps no ingest — armazene em **UTC** e converta na camada de consumo (Glue/Spark).
- Se precisar de preenchimento horário (holes), faça reindex downstream ou solicite que eu adicione `--fill-hours` como opção no ingestor.

### Testes relacionados ✅

- Há um teste unitário que valida a normalização de timestamps timezone-aware para UTC: `tests/test_timestamps_utc.py`.

## 🛠️ Detalhes Técnicos

- **Multi-Ticker:** O script processa uma lista de ativos sequencialmente.
- **Particionamento Real:** Diferente de scripts que usam a data de execução, este script identifica a data de cada registro no `yfinance` e cria a partição `date=YYYY-MM-DD` correspondente, permitindo cargas históricas (`backfill`) precisas.
- **Tratamento de Colunas:** O script remove MultiIndex de colunas gerados pelo `yfinance` para garantir compatibilidade total com o AWS Glue e Athena.

## ✅ Checklist de aceitação

- [x] Ingestão raw em `raw/dt=YYYY-MM-DD/` (implementado)
- [ ] Glue job que gera `refined/date=.../ticker=.../` com: A (agregação), B (renomear 2 colunas), C (cálculo por data)
- [ ] Lambda acionada por evento S3 que inicia o Glue job (stub/teste)
- [ ] Glue Catalog atualizado e tabelas acessíveis via Athena
- [ ] Queries de validação (partition pruning e métricas calculadas) incluídas na documentação

---
*Este projeto faz parte da Fase 2 do Tech Challenge de Machine Learning Avançado - FIAP.*
