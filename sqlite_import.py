import sys
import os
import sqlite3
import pandas as pd
import msoffcrypto
import io
import yaml
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Load configuration from YAML
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

data_file_password = config['data_file_password']
data_file_path_prefix = config['data_file_path_prefix']
file_path_prefix = config['file_path_prefix']
cmp_courses_file = f"{file_path_prefix}/{config['cmp_courses_file_name']}"
db_file = f"{file_path_prefix}/{config['db_file_name']}"

def get_xlsx_files(directory_path):
    directory = Path(directory_path).resolve()
    print(f"Searching {directory} given input path {directory_path}")
    return [str(file.resolve()) for file in directory.rglob("*.xlsx")]

def sanitize_table_name(filename):
    base = os.path.basename(filename)
    name, _ = os.path.splitext(base)
    return ''.join(c if c.isalnum() else '_' for c in name)

def load_excel(file_path):
    print(f"Loading: {file_path}")
    dataframe = None

    if os.path.exists(file_path):
        try:
            dataframe = pd.read_excel(file_path, engine='openpyxl')
        except Exception:
            try:
                with open(file_path, 'rb') as f:
                    office_file = msoffcrypto.OfficeFile(f)
                    office_file.load_key(password=data_file_password)
                    decrypted = io.BytesIO()
                    office_file.decrypt(decrypted)
                    dataframe = pd.read_excel(decrypted, engine='openpyxl')
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
    else:
        print(f"File not found: {file_path}")

    return dataframe

def process_file(file_path):
    """
    Loads an Excel file and returns a tuple: (sanitized_table_name, DataFrame)
    """
    if not file_path.lower().endswith(".xlsx") or not os.path.exists(file_path):
        print(f"Skipping invalid file: {file_path}")
        return None

    df = load_excel(file_path)
    if df is not None:
        table_name = sanitize_table_name(file_path)
        return table_name, df
    else:
        print(f"Skipping file due to read failure: {file_path}")
        return None

def import_to_sqlite(db_conn, df, table_name):
    df.to_sql(table_name, db_conn, if_exists="replace", index=False)
    print(f"Imported into table '{table_name}'.")

def main():
    files = get_xlsx_files(data_file_path_prefix)
    files.append(cmp_courses_file) # Also load course category mapping

    table_data = {}  # Dict to hold {table_name: DataFrame}

    # Process files in parallel (read only)
    with ThreadPoolExecutor(max_workers=64) as executor:
        futures = [executor.submit(process_file, file_path) for file_path in files]
        with tqdm(total=len(futures), desc="Reading Excel files", unit="file") as pbar:
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    table_name, df = result
                    table_data[table_name] = df
                pbar.update(1)

    # Write results to database sequentially (avoids locking)
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("BEGIN TRANSACTION")

    for table_name, df in tqdm(table_data.items(),
                               total=len(table_data),
                               desc="Writing tables to SQLite",
                               unit="table"):
        df.to_sql(table_name, conn, if_exists="replace", index=False, chunksize=1000)

    conn.commit()
    conn.close()

    print(f"All done. Database saved as '{db_file}'.")

if __name__ == "__main__":
    main()
