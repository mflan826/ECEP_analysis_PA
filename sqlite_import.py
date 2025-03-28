import sys
import os
import sqlite3
import pandas as pd
import msoffcrypto
import io
from getpass import getpass
import yaml # pip install pyyaml
from pathlib import Path

# Load configuration from YAML
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

data_file_password = config['data_file_password']
data_file_path_prefix = config['data_file_path_prefix']
cmp_file_path_prefix = config['cmp_file_path_prefix']
cmp_courses_file = f"{cmp_file_path_prefix}/{config['cmp_courses_file_name']}"
db_file = f"{cmp_file_path_prefix}/{config['db_file_name']}"

def get_xlsx_files(directory_path):
    """
    Recursively returns a list of absolute paths to all .xlsx files 
    in the specified directory and its subdirectories.

    Parameters:
    directory_path (str): The relative or absolute path to the directory to search.

    Returns:
    list: A list of absolute paths to .xlsx files in the directory tree.
    """
    directory = Path(directory_path).resolve()
    print(f"Searching {directory} given input path {directory_path}")
    
    return [str(file.resolve()) for file in directory.rglob("*.xlsx")]

# Load input data files
def load_excel(file_path):
    dataframe = None
    print(f"Loading: {file_path}")

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
                dataframe = None
    else:
        print(f"File not found: {file_path}")
        dataframe = None

    return dataframe

def sanitize_table_name(filename):
    """
    Create a valid SQLite table name from the filename.
    """
    base = os.path.basename(filename)
    name, _ = os.path.splitext(base)
    return ''.join(c if c.isalnum() else '_' for c in name)

def import_to_sqlite(db_conn, df, table_name):
    """
    Import a DataFrame into the SQLite database under the given table name.
    """
    df.to_sql(table_name, db_conn, if_exists="replace", index=False)
    print(f"Imported into table '{table_name}'.")

def main():
    conn = sqlite3.connect(db_file)
    print(f"Connected to SQLite database '{db_file}'.")
    
    files = get_xlsx_files(data_file_path_prefix)
    
    files.append(cmp_courses_file) # also read courses file

    for file_path in files:
        if not file_path.lower().endswith(".xlsx") or not os.path.exists(file_path):
            print(f"Skipping invalid file: {file_path}")
            continue

        print(f"Processing file: {file_path}")
        df = load_excel(file_path)

        if df is not None:
            table_name = sanitize_table_name(file_path)
            import_to_sqlite(conn, df, table_name)
        else:
            print(f"Skipping file due to read failure: {file_path}")

    conn.close()
    print(f"All done. Database saved as '{db_file}'.")

if __name__ == "__main__":
    main()
