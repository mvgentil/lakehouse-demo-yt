import duckdb
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import os

def load_silver():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    bronze_path = os.path.join(base_dir, 'data', 'lakehouse', '01_bronze', 'online_retail')
    silver_path = os.path.join(base_dir, 'data', 'lakehouse', '02_silver')

    print("[Lakehouse] Reading from Bronze Delta Table...")
    con = duckdb.connect()

    # Register Bronze Delta Table
    dt = DeltaTable(bronze_path)
    con.register('bronze', dt.to_pyarrow_dataset())

    # Fact Sales
    print("[Lakehouse] Creating fact_sales...")
    df_fact = con.execute("""
        SELECT
            InvoiceNo as invoice_no,
            StockCode as stock_code,
            CAST(Quantity AS INTEGER) as quantity,
            strptime(InvoiceDate, '%m/%d/%Y %H:%M') as invoice_date,
            CAST(UnitPrice AS DOUBLE) as unit_price,
            CustomerID as customer_id,
            Country as country,
            (CAST(Quantity AS INTEGER) * CAST(UnitPrice AS DOUBLE)) as total_amount
        FROM bronze
        WHERE Quantity > 0
          AND CustomerID IS NOT NULL
    """).df()
    write_deltalake(os.path.join(silver_path, 'fact_sales'), df_fact, mode='overwrite')

    # Dim Customer
    print("[Lakehouse] Creating dim_customer...")
    df_cust = con.execute("""
        SELECT DISTINCT
            CustomerID as customer_id,
            Country as country
        FROM bronze
        WHERE CustomerID IS NOT NULL
    """).df()
    write_deltalake(os.path.join(silver_path, 'dim_customer'), df_cust, mode='overwrite')

    # Dim Product
    print("[Lakehouse] Creating dim_product...")
    df_prod = con.execute("""
        SELECT
            StockCode as stock_code,
            FIRST(Description) as description
        FROM bronze
        WHERE StockCode IS NOT NULL
        GROUP BY StockCode
    """).df()
    write_deltalake(os.path.join(silver_path, 'dim_product'), df_prod, mode='overwrite')

    print("[Lakehouse] Silver layer complete!")
    con.close()

if __name__ == "__main__":
    load_silver()
