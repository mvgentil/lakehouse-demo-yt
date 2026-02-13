import kagglehub
import shutil
import os
import glob

def ingest_data():
    print("Downloading dataset from Kaggle...")
    # Using a reliable Online Retail dataset
    # If this specific one fails, we can swap it. 
    # 'vijayuv/online-retail' is a common one.
    path = kagglehub.dataset_download("vijayuv/online-retail")
    
    print(f"Dataset downloaded to cache: {path}")

    # Define Target Path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(base_dir, 'data', 'raw')
    os.makedirs(raw_dir, exist_ok=True)
    
    # Find CSV in the download path
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    
    if not csv_files:
        print("No CSV found in the downloaded dataset.")
        return

    source_csv = csv_files[0]
    target_csv = os.path.join(raw_dir, 'OnlineRetail.csv')
    
    print(f"Copying {source_csv} to {target_csv}...")
    shutil.copy(source_csv, target_csv)
    
    print("Ingestion Complete. Ready for Bronze Layer.")

if __name__ == "__main__":
    ingest_data()
