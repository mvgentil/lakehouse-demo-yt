import pandas as pd
from deltalake.writer import write_deltalake
import os

def load_bronze():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_path = os.path.join(base_dir, 'data', 'raw', 'OnlineRetail.csv')
    bronze_path = os.path.join(base_dir, 'data', 'lakehouse', '01_bronze', 'online_retail')

    print(f"[Lakehouse] Reading CSV from: {raw_path}")

    if not os.path.exists(raw_path):
        print(f"ERROR: File not found at {raw_path}")
        return

    df = pd.read_csv(raw_path, encoding='ISO-8859-1', dtype={'CustomerID': str})

    print(f"[Lakehouse] Writing Delta Table to: {bronze_path}")
    write_deltalake(bronze_path, df, mode='overwrite')

    print("[Lakehouse] Bronze layer complete!")

if __name__ == "__main__":
    load_bronze()
