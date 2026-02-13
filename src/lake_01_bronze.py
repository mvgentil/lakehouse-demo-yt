import pandas as pd
import os

def load_bronze():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_path = os.path.join(base_dir, 'data', 'raw', 'OnlineRetail.csv')
    bronze_path = os.path.join(base_dir, 'data', 'lake', '01_bronze')

    print(f"[Lake] Reading CSV from: {raw_path}")

    if not os.path.exists(raw_path):
        print(f"ERROR: File not found at {raw_path}")
        return

    df = pd.read_csv(raw_path, encoding='ISO-8859-1', dtype={'CustomerID': str})

    os.makedirs(bronze_path, exist_ok=True)
    output_path = os.path.join(bronze_path, 'online_retail.parquet')
    df.to_parquet(output_path, index=False)

    print(f"[Lake] Bronze saved to: {output_path}")

if __name__ == "__main__":
    load_bronze()
