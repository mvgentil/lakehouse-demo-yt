import duckdb
import os

def load_silver():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    bronze_path = os.path.join(base_dir, 'data', 'lake', '01_bronze', 'online_retail.parquet')
    silver_path = os.path.join(base_dir, 'data', 'lake', '02_silver')

    os.makedirs(silver_path, exist_ok=True)

    print("[Lake] Reading from Bronze Parquet...")
    con = duckdb.connect()

    # Fact Sales
    print("[Lake] Creating fact_sales...")
    con.execute(f"""
        COPY (
            SELECT
                InvoiceNo as invoice_no,
                StockCode as stock_code,
                CAST(Quantity AS INTEGER) as quantity,
                strptime(InvoiceDate, '%m/%d/%Y %H:%M') as invoice_date,
                CAST(UnitPrice AS DOUBLE) as unit_price,
                CustomerID as customer_id,
                Country as country,
                (CAST(Quantity AS INTEGER) * CAST(UnitPrice AS DOUBLE)) as total_amount
            FROM read_parquet('{bronze_path}')
            WHERE Quantity > 0
              AND CustomerID IS NOT NULL
        ) TO '{os.path.join(silver_path, 'fact_sales.parquet')}' (FORMAT PARQUET)
    """)

    # Dim Customer
    print("[Lake] Creating dim_customer...")
    con.execute(f"""
        COPY (
            SELECT DISTINCT
                CustomerID as customer_id,
                Country as country
            FROM read_parquet('{bronze_path}')
            WHERE CustomerID IS NOT NULL
        ) TO '{os.path.join(silver_path, 'dim_customer.parquet')}' (FORMAT PARQUET)
    """)

    # Dim Product
    print("[Lake] Creating dim_product...")
    con.execute(f"""
        COPY (
            SELECT
                StockCode as stock_code,
                FIRST(Description) as description
            FROM read_parquet('{bronze_path}')
            WHERE StockCode IS NOT NULL
            GROUP BY StockCode
        ) TO '{os.path.join(silver_path, 'dim_product.parquet')}' (FORMAT PARQUET)
    """)

    print("[Lake] Silver layer complete!")
    con.close()

if __name__ == "__main__":
    load_silver()
