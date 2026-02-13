import duckdb
import os

def load_gold():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    silver_path = os.path.join(base_dir, 'data', 'lake', '02_silver', 'fact_sales.parquet')
    gold_path = os.path.join(base_dir, 'data', 'lake', '03_gold')

    os.makedirs(gold_path, exist_ok=True)

    print("[Lake] Reading from Silver Parquet...")
    con = duckdb.connect()

    # Daily Sales
    print("[Lake] Creating daily_sales...")
    con.execute(f"""
        COPY (
            SELECT
                CAST(invoice_date AS DATE) as sales_date,
                SUM(total_amount) as total_revenue,
                COUNT(DISTINCT invoice_no) as total_orders
            FROM read_parquet('{silver_path}')
            GROUP BY 1
            ORDER BY 1
        ) TO '{os.path.join(gold_path, 'daily_sales.parquet')}' (FORMAT PARQUET)
    """)

    # Sales by Country
    print("[Lake] Creating sales_by_country...")
    con.execute(f"""
        COPY (
            SELECT
                country,
                SUM(total_amount) as total_revenue,
                COUNT(DISTINCT invoice_no) as total_orders
            FROM read_parquet('{silver_path}')
            GROUP BY 1
            ORDER BY 2 DESC
        ) TO '{os.path.join(gold_path, 'sales_by_country.parquet')}' (FORMAT PARQUET)
    """)

    print("[Lake] Gold layer complete!")
    con.close()

if __name__ == "__main__":
    load_gold()
