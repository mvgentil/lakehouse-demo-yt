# 🏗️ Data Lake vs Data Lakehouse — Projeto Didático

> 📹 Projeto de demonstração para aula no YouTube sobre as diferenças entre **Data Lake** e **Data Lakehouse**.

## 🎯 Objetivo

Demonstrar na prática a diferença entre um **Data Lake** (Parquet puro) e um **Data Lakehouse** (Delta Table), usando o mesmo dataset e as mesmas transformações, permitindo comparar as abordagens lado a lado.

## 📊 Dataset

**Online Retail Dataset** — Transações de uma loja de varejo online do Reino Unido (2010-2011).

- **Fonte**: [Kaggle](https://www.kaggle.com/datasets/vijayuv/online-retail)
- **Registros**: ~541.000 transações
- **Formato original**: CSV

## 🏛️ Arquitetura

```
data/
├── raw/                    ← CSV original
├── lake/                   ← Data Lake (Parquet puro)
│   ├── 01_bronze/          ← Dados brutos em Parquet
│   ├── 02_silver/          ← Dados limpos (Star Schema)
│   └── 03_gold/            ← Agregações para BI
└── lakehouse/              ← Data Lakehouse (Delta Table)
    ├── 01_bronze/          ← Dados brutos em Delta Table
    ├── 02_silver/          ← Dados limpos (Star Schema)
    └── 03_gold/            ← Agregações para BI
```

### Comparativo

| Feature | Data Lake (Parquet) | Lakehouse (Delta Table) |
|---|---|---|
| Formato | `.parquet` | `.parquet` + `_delta_log/` |
| Transações ACID | ❌ | ✅ |
| Time Travel | ❌ | ✅ |
| Schema Enforcement | ❌ | ✅ |
| Upserts/Merges | Manual | Nativo |

## 🛠️ Tech Stack

- **Python 3.13**
- **DuckDB** — Motor de consulta SQL local
- **Delta Lake** (`deltalake`) — Formato de tabela transacional
- **Pandas / PyArrow** — Manipulação de dados
- **Streamlit + Plotly** — Dashboard interativo
- **KaggleHub** — Download automático do dataset
- **uv** — Gerenciador de pacotes

## 🚀 Como Rodar

### 1. Instalar dependências

```bash
uv sync
```

### 2. Baixar o dataset

**Opção A** — Download automático via KaggleHub:
```bash
uv run src/00_ingest.py
```

**Opção B** — Download manual:
Baixe o [OnlineRetail.csv](https://www.kaggle.com/datasets/vijayuv/onlineretail) e coloque em `data/raw/`.

### 3. Executar os pipelines

```bash
# Pipeline Data Lake (Parquet)
uv run src/lake_pipeline.py

# Pipeline Data Lakehouse (Delta Table)
uv run src/lakehouse_pipeline.py
```

Ou execute cada camada individualmente:

```bash
# Data Lake
uv run src/lake_01_bronze.py
uv run src/lake_02_silver.py
uv run src/lake_03_gold.py

# Data Lakehouse
uv run src/lakehouse_01_bronze.py
uv run src/lakehouse_02_silver.py
uv run src/lakehouse_03_gold.py
```

### 4. Abrir o Dashboard

```bash
uv run streamlit run src/app.py
```

## 📓 Notebooks de Exploração

Na pasta `notebooks/` você encontra notebooks interativos para demonstrar:

| Notebook | Conteúdo |
|---|---|
| `lake_explorations.ipynb` | Exploração do Data Lake + demonstração das **limitações** |
| `lakehouse_exploration.ipynb` | Exploração do Lakehouse + demonstração das **vantagens** (Time Travel, Schema Enforcement, ACID) |

## 📁 Estrutura do Projeto

```
├── data/
│   └── raw/OnlineRetail.csv
├── notebooks/
│   ├── lake_explorations.ipynb
│   └── lakehouse_exploration.ipynb
├── src/
│   ├── 00_ingest.py              # Download automático (KaggleHub)
│   ├── lake_01_bronze.py         # CSV → Parquet
│   ├── lake_02_silver.py         # Limpeza + Star Schema (Parquet)
│   ├── lake_03_gold.py           # Agregações (Parquet)
│   ├── lake_pipeline.py          # Executa todo o pipeline Lake
│   ├── lakehouse_01_bronze.py    # CSV → Delta Table
│   ├── lakehouse_02_silver.py    # Limpeza + Star Schema (Delta)
│   ├── lakehouse_03_gold.py      # Agregações (Delta)
│   ├── lakehouse_pipeline.py     # Executa todo o pipeline Lakehouse
│   └── app.py                    # Dashboard Streamlit
├── pyproject.toml
└── README.md
```
