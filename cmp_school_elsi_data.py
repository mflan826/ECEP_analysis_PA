import sqlite3
import pandas as pd
import yaml
import re
from rapidfuzz import process, fuzz
import concurrent.futures
import threading

# Load configuration from YAML
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Paths
file_path_prefix = config['file_path_prefix']
elsi_school_file = f"{file_path_prefix}/{config['elsi_school_file_name']}"
elsi_district_file = f"{file_path_prefix}/{config['elsi_district_file_name']}"
db_file = f"{file_path_prefix}/{config['db_file_name']}"

# Column names from config
elsi_school_col = config['elsi_school_col']
elsi_district_col = config['elsi_district_col']
elsi_school_id_col = config['elsi_school_id_col']
elsi_district_id_col = config['elsi_district_id_col']
elsi_low_grade_band = config['elsi_low_grade_band']
elsi_high_grade_band = config['elsi_high_grade_band']
output_school_name_col = config['output_school_name_col']
output_district_name_col = config['output_district_name_col']
output_school_id_col = config['output_school_id_col']
output_district_id_col = config['output_district_id_col']
output_low_grade_band_col = config['output_low_grade_band_col']
output_high_grade_band_col = config['output_high_grade_band_col']
data_file_school_name_col = config['data_file_school_name_col']
data_file_district_name_col = config['data_file_district_name_col']

school_lookup = {}
district_lookup = {}

school_match_cache = {}
district_match_cache = {}

def trunc_float(value):
    if isinstance(value, float):
        return str(int(value))
    return value

def normalize_name(name):
    if not name:
        return ''
    name = name.lower()
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

def get_id_from_name_cached(name, lookup, source_df, id_col, cache, low_band_col=None, high_band_col=None):
    if not name:
        return None, None, None

    norm_name = normalize_name(name)

    # Check the cache first
    match_index = cache.get(norm_name)

    # Fallback to lookup if not cached
    if match_index is None:
        match_index = lookup.get(norm_name)
        if match_index is not None:
            cache[norm_name] = match_index  # Cache it now

    if match_index is not None:
        row = source_df.loc[match_index]
        return (
            row[id_col],
            row[low_band_col] if low_band_col else None,
            row[high_band_col] if high_band_col else None
        )

    # Fuzzy fallback if no match
    choices = source_df[elsi_school_col if id_col == elsi_school_id_col else elsi_district_col].astype(str)
    normalized_choices = choices.map(normalize_name).tolist()

    best_match = process.extractOne(norm_name, normalized_choices,
                                    scorer=fuzz.token_sort_ratio,
                                    score_cutoff=70)

    if best_match:
        _, _, fallback_idx = best_match
        cache[norm_name] = fallback_idx
        row = source_df.iloc[fallback_idx]
        return (
            row[id_col],
            row[low_band_col] if low_band_col else None,
            row[high_band_col] if high_band_col else None
        )

    return None, None, None

def batch_update_table(conn, table_name, df):
    print(f"Ensuring columns exist and applying batch UPDATEs to '{table_name}'...")

    output_school_id_col = config['output_school_id_col']
    output_district_id_col = config['output_district_id_col']
    output_low_grade_band_col = config['output_low_grade_band_col']
    output_high_grade_band_col = config['output_high_grade_band_col']

    required_columns = []

    # Determine which output columns should exist in the table
    if data_file_school_name_col in df.columns:
        required_columns += [output_school_id_col, output_low_grade_band_col, output_high_grade_band_col]
    if data_file_district_name_col in df.columns:
        required_columns.append(output_district_id_col)

    # Fetch current columns in the SQLite table
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    existing_columns = {row[1] for row in cursor.fetchall()}

    # Add missing columns
    for col in required_columns:
        if col not in existing_columns:
            print(f" - Adding missing column '{col}' to '{table_name}'")
            cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT')

    # Now determine which of those required columns are present in the df
    available_cols = [col for col in required_columns if col in df.columns]

    if not available_cols:
        print(f"No applicable columns found in '{table_name}' to update.")
        return

    if 'rowid' not in df.columns:
        print(f"'rowid' column not found in data for '{table_name}'; skipping update.")
        return

    set_clause = ", ".join([f'"{col}" = ?' for col in available_cols])
    sql = f'UPDATE "{table_name}" SET {set_clause} WHERE rowid = ?'

    values = []
    for _, row in df.iterrows():
        if pd.isnull(row['rowid']):
            continue
        row_values = [row[col] for col in available_cols] + [row['rowid']]
        values.append(tuple(row_values))

    if not values:
        print(f"No valid rows to update in '{table_name}'.")
        return

    try:
        cursor.executemany(sql, values)
    except Exception as e:
        print(f"Failed to update '{table_name}': {e}")

def update_table_with_elsi_data(table_name, school_df, district_df):
    print(f"[Thread {threading.get_ident()}] Enriching table '{table_name}' with ELSI data...")

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    columns = [row[1] for row in cursor.fetchall()]
    conn.close()

    has_location = data_file_school_name_col in columns
    has_district = data_file_district_name_col in columns

    if not (has_location or has_district):
        print(f"Skipping {table_name} - no {data_file_school_name_col} or {data_file_district_name_col}")
        return table_name, None

    df = pd.read_sql_query(f'SELECT rowid, * FROM "{table_name}"', sqlite3.connect(db_file))

    # Ensure required output columns exist in the DataFrame
    if has_location:
        for col in [output_school_id_col, output_low_grade_band_col, output_high_grade_band_col]:
            if col not in df.columns:
                df[col] = None
                
    if has_district:
        if output_district_id_col not in df.columns:
            df[output_district_id_col] = None

    def enrich_row(row):
        new_row = row.copy()
        school_id = district_id = low_band = high_band = None

        school_name = row.get(data_file_school_name_col)
        district_name = row.get(data_file_district_name_col)

        if has_location and pd.notnull(school_name):
            #print(f"Matching school name: '{school_name}'")
            school_id, low_band, high_band = get_id_from_name_cached(
                school_name,
                school_lookup,
                school_df,
                elsi_school_id_col,
                school_match_cache,
                elsi_low_grade_band,
                elsi_high_grade_band
            )
            school_id = trunc_float(school_id)
            #print(f"Matched school_id: {school_id}, low: {low_band}, high: {high_band}")
        
        if has_district and pd.notnull(district_name):
            #print(f"Matching district name: '{district_name}'")
            district_id, _, _ = get_id_from_name_cached(
                district_name,
                district_lookup,
                district_df,
                elsi_district_id_col,
                district_match_cache
            )
            district_id = trunc_float(district_id)
            #print(f"Matched district_id: {district_id}")

        if has_location:
            new_row[output_school_id_col] = school_id
            new_row[output_low_grade_band_col] = low_band
            new_row[output_high_grade_band_col] = high_band
        if has_district:
            new_row[output_district_id_col] = district_id

        return new_row

    enriched_df = df.apply(enrich_row, axis=1)

    update_cols = ['rowid']
    if has_location:
        update_cols += [output_school_id_col, output_low_grade_band_col, output_high_grade_band_col]
    if has_district:
        update_cols += [output_district_id_col]

    return table_name, enriched_df

def main():
    global school_lookup, district_lookup
    
    conn = sqlite3.connect(db_file)
    print(f"Connected to database: {db_file}")

    school_df = pd.read_excel(elsi_school_file, skiprows=6)
    district_df = pd.read_excel(elsi_district_file, skiprows=6)

    # Normalize school and district names once for lookup efficiency
    school_df["normalized_name"] = school_df[elsi_school_col].astype(str).map(normalize_name)
    district_df["normalized_name"] = district_df[elsi_district_col].astype(str).map(normalize_name)

    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    # Normalize school and district names once for lookup efficiency
    school_df["normalized_name"] = school_df[elsi_school_col].astype(str).map(normalize_name)
    school_lookup = dict(zip(school_df["normalized_name"], school_df.index))

    district_df["normalized_name"] = district_df[elsi_district_col].astype(str).map(normalize_name)
    district_lookup = dict(zip(district_df["normalized_name"], district_df.index))

    # Process tables in parallel with a thread pool
    enriched_results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        futures = [executor.submit(update_table_with_elsi_data, table, school_df, district_df)
                   for table in tables]
        for future in concurrent.futures.as_completed(futures):
            table_name, enriched_df = future.result()
            if enriched_df is not None:
                enriched_results[table_name] = enriched_df

    # Now write all updated tables to SQLite sequentially to avoid locks
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("BEGIN TRANSACTION")
   
    for table_name, df in enriched_results.items():
        print(f"Writing enriched table '{table_name}' to database...")
        batch_update_table(conn, table_name, df)

    conn.commit()
    conn.close()

    print("Done.")

if __name__ == "__main__":
    main()
