import duckdb
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import os

def load_gold():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    silver_path = os.path.join(base_dir, 'data', 'lakehouse', '02_silver', 'fact_sales')
    gold_path = os.path.join(base_dir, 'data', 'lakehouse', '03_gold')

    print("[Lakehouse] Reading from Silver Delta Table...")
    con = duckdb.connect()

    dt = DeltaTable(silver_path)
    con.register('fact_sales', dt.to_pyarrow_dataset())

    # Daily Sales
    print("[Lakehouse] Creating daily_sales...")
    df_daily = con.execute("""
        SELECT
            CAST(invoice_date AS DATE) as sales_date,
            SUM(total_amount) as total_revenue,
            COUNT(DISTINCT invoice_no) as total_orders
        FROM fact_sales
        GROUP BY 1
        ORDER BY 1
    """).df()
    write_deltalake(os.path.join(gold_path, 'daily_sales'), df_daily, mode='overwrite')

    # Sales by Country
    print("[Lakehouse] Creating sales_by_country...")
    df_country = con.execute("""
        SELECT
            country,
            SUM(total_amount) as total_revenue,
            COUNT(DISTINCT invoice_no) as total_orders
        FROM fact_sales
        GROUP BY 1
        ORDER BY 2 DESC
    """).df()
    write_deltalake(os.path.join(gold_path, 'sales_by_country'), df_country, mode='overwrite')

    print("[Lakehouse] Gold layer complete!")
    con.close()

if __name__ == "__main__":
    load_gold()
