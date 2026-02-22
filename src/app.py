import streamlit as st
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import os
from deltalake import DeltaTable

st.set_page_config(page_title="Data Lake vs Lakehouse", layout="wide")
st.title("📊 Data Lake vs Data Lakehouse")

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_parquet(path: str) -> pd.DataFrame:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").df()
    con.close()
    return df


def load_delta(path: str) -> pd.DataFrame:
    dt = DeltaTable(path)
    return dt.to_pandas()


def render_dashboard(df_daily: pd.DataFrame, df_country: pd.DataFrame, label: str):
    df_daily = df_daily.copy()
    df_country = df_country.copy()

    df_daily["sales_date"] = pd.to_datetime(df_daily["sales_date"])
    df_daily = df_daily.sort_values("sales_date")
    df_country["country"] = df_country["country"].astype(str)
    df_country = df_country.sort_values("total_revenue", ascending=False).head(10)

    # KPIs
    col1, col2 = st.columns(2)
    col1.metric("Receita Total", f"${df_daily['total_revenue'].sum():,.2f}")
    col2.metric("Total de Pedidos", f"{df_daily['total_orders'].sum():,}")

    # Gráfico 1 — Receita por Dia
    st.subheader("Receita por Dia")
    fig1, ax1 = plt.subplots(figsize=(12, 4))
    ax1.bar(df_daily["sales_date"], df_daily["total_revenue"], width=1, color="#4A90D9")
    ax1.set_xlabel("Data")
    ax1.set_ylabel("Receita")
    ax1.set_title(f"Receita por Dia — {label}")
    fig1.autofmt_xdate()
    fig1.tight_layout()
    st.pyplot(fig1)

    # Gráfico 2 — Top 10 Países
    st.subheader("Top 10 Países por Receita")
    fig2, ax2 = plt.subplots(figsize=(10, 4))
    ax2.bar(df_country["country"], df_country["total_revenue"], color="#5CB85C")
    ax2.set_xlabel("País")
    ax2.set_ylabel("Receita Total")
    ax2.set_title(f"Top 10 Países — {label}")
    plt.xticks(rotation=45, ha="right")
    fig2.tight_layout()
    st.pyplot(fig2)


tab_lake, tab_lakehouse = st.tabs(["🗂️ Data Lake (Parquet)", "🏠 Data Lakehouse (Delta Table)"])

with tab_lake:
    st.header("Data Lake — Pure Parquet Files")
    st.caption("Arquivos `.parquet` puros, sem transação, sem versionamento.")
    try:
        gold = os.path.join(base_dir, "data", "lake", "03_gold")
        df_daily   = load_parquet(os.path.join(gold, "daily_sales.parquet"))
        df_country = load_parquet(os.path.join(gold, "sales_by_country.parquet"))
        render_dashboard(df_daily, df_country, "Data Lake")
    except Exception as e:
        st.error(f"Execute o pipeline do Data Lake primeiro: {e}")

with tab_lakehouse:
    st.header("Data Lakehouse — Delta Tables")
    st.caption("Delta Tables com ACID, schema enforcement e time travel.")
    try:
        gold = os.path.join(base_dir, "data", "lakehouse", "03_gold")
        df_daily   = load_delta(os.path.join(gold, "daily_sales"))
        df_country = load_delta(os.path.join(gold, "sales_by_country"))
        render_dashboard(df_daily, df_country, "Lakehouse")
    except Exception as e:
        st.error(f"Execute o pipeline do Lakehouse primeiro: {e}")

st.sidebar.markdown("## Comparativo")
st.sidebar.markdown("""
| Feature              | Lake       | Lakehouse   |
|----------------------|------------|-------------|
| Formato              | `.parquet` | Delta Table |
| ACID                 | ❌         | ✅          |
| Time Travel          | ❌         | ✅          |
| Schema Enforcement   | ❌         | ✅          |
""")
st.sidebar.info("Ambos os pipelines leem do mesmo CSV em data/raw/.")
