import streamlit as st
import duckdb
import plotly.express as px
import os
from deltalake import DeltaTable

st.set_page_config(page_title="Data Lake vs Lakehouse", layout="wide")

st.title("📊 Data Lake vs Data Lakehouse")

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- Helper Functions ---
def load_parquet(path):
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").df()
    con.close()
    return df

def load_delta(path):
    dt = DeltaTable(path)
    return dt.to_pandas()

def render_dashboard(df_daily, df_country, label):
    total_revenue = df_daily['total_revenue'].sum()
    total_orders = df_daily['total_orders'].sum()

    col1, col2 = st.columns(2)
    col1.metric("Total Revenue", f"${total_revenue:,.2f}")
    col2.metric("Total Orders", f"{total_orders:,}")

    st.subheader("Daily Sales Trend")
    fig_daily = px.line(df_daily, x='sales_date', y='total_revenue',
                        title=f'Revenue over Time ({label})')
    st.plotly_chart(fig_daily, use_container_width=True)

    st.subheader("Top 10 Countries by Revenue")
    top = df_country.head(10)
    fig_country = px.bar(top, x='country', y='total_revenue',
                         title=f'Top 10 Countries ({label})')
    st.plotly_chart(fig_country, use_container_width=True)

# --- Tabs ---
tab_lake, tab_lakehouse = st.tabs(["🗂️ Data Lake (Parquet)", "🏠 Data Lakehouse (Delta Table)"])

with tab_lake:
    st.header("Data Lake — Pure Parquet Files")
    st.caption("Arquivos `.parquet` puros, sem transação, sem versionamento.")
    try:
        lake_gold = os.path.join(base_dir, 'data', 'lake', '03_gold')
        df_daily = load_parquet(os.path.join(lake_gold, 'daily_sales.parquet'))
        df_country = load_parquet(os.path.join(lake_gold, 'sales_by_country.parquet'))
        render_dashboard(df_daily, df_country, "Data Lake")
    except Exception as e:
        st.error(f"Execute o pipeline do Data Lake primeiro: {e}")

with tab_lakehouse:
    st.header("Data Lakehouse — Delta Tables")
    st.caption("Delta Tables com ACID, schema enforcement e time travel.")
    try:
        lh_gold = os.path.join(base_dir, 'data', 'lakehouse', '03_gold')
        df_daily = load_delta(os.path.join(lh_gold, 'daily_sales'))
        df_country = load_delta(os.path.join(lh_gold, 'sales_by_country'))
        render_dashboard(df_daily, df_country, "Lakehouse")
    except Exception as e:
        st.error(f"Execute o pipeline do Lakehouse primeiro: {e}")

# --- Sidebar ---
st.sidebar.markdown("## Comparativo")
st.sidebar.markdown("""
| Feature | Lake | Lakehouse |
|---|---|---|
| Formato | `.parquet` | Delta Table |
| ACID | ❌ | ✅ |
| Time Travel | ❌ | ✅ |
| Schema Enforcement | ❌ | ✅ |
""")
st.sidebar.info("Ambos os pipelines leem do mesmo CSV em data/raw/.")
