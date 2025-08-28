#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import pandas as pd
import yaml
import re
import traceback
from rapidfuzz import process, fuzz
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Optional, Tuple
import numpy as np

# ============================
# Configuration & Constants
# ============================

try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except Exception as e:
    print(f"[config] Failed to load config.yaml: {e}")
    traceback.print_exc()
    raise

# Paths
file_path_prefix       = config['file_path_prefix']
elsi_school_file       = f"{file_path_prefix}/{config['elsi_school_file_name']}"
elsi_district_file     = f"{file_path_prefix}/{config['elsi_district_file_name']}"
db_file                = f"{file_path_prefix}/{config['db_file_name']}"

# Column names from config
elsi_school_col        = config['elsi_school_col']        # e.g., "School Name"
elsi_district_col      = config['elsi_district_col']      # e.g., "Agency Name"
elsi_school_id_col     = config['elsi_school_id_col']     # e.g., "School ID (12-digit) - NCES Assigned [Public School] Latest available year"
elsi_district_id_col   = config['elsi_district_id_col']   # e.g., "District ID (7-digit) - NCES Assigned [Public School] Latest available year"
elsi_low_grade_band    = config.get('elsi_low_grade_band')    # optional, e.g., "Low Grade"
elsi_high_grade_band   = config.get('elsi_high_grade_band')   # optional, e.g., "High Grade"

# Where to write results in your DB tables
output_school_name_col    = config['output_school_name_col']      # not required to exist; informational/reference
output_district_name_col  = config['output_district_name_col']    # not required to exist; informational/reference
output_school_id_col      = config['output_school_id_col']        # destination NCES ID (school)
output_district_id_col    = config['output_district_id_col']      # destination NCES ID (district)
output_low_grade_band_col = config['output_low_grade_band_col']   # destination low grade
output_high_grade_band_col= config['output_high_grade_band_col']  # destination high grade

# Source columns (names) inside your SQLite tables
data_file_school_name_col   = config['data_file_school_name_col']   # column containing the school name
data_file_district_name_col = config['data_file_district_name_col'] # column containing the district name

# Fuzzy match score cutoff; mirrors your Excel script's >=60
MATCH_SCORE_CUTOFF = int(config.get('match_score_cutoff', 60))

# Thread-safe caches (name -> ELSI row index)
school_match_cache   = {}
district_match_cache = {}

# Precomputed choice lists for extractOne (built in main)
_school_choices   = None
_district_choices = None


# ============================
# Utilities
# ============================

def trunc_float(value):
    """
    Return a clean string for NCES IDs and other numeric values:
      - None/NaN/empty -> None
      - floats like 123456.0 -> "123456"
      - strings like "123456.0" -> "123456"
      - otherwise, str(value) unchanged
    (We do NOT left-pad; we preserve whatever digits are present.)
    """
    if value is None:
        return None
    # Handle pandas NA / numpy NaN
    try:
        import numpy as _np
        if (isinstance(value, float) and (_np.isnan(value) or not _np.isfinite(value))) or pd.isna(value):
            return None
    except Exception:
        # if numpy not available or other edge: fall through
        if pd.isna(value):
            return None

    # Already a string?
    s = str(value).strip()
    if s == "":
        return None

    # If the Python object is a float like 123456.0, drop .0
    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    # If it's a string that *looks* like an integer float, drop .0
    if re.fullmatch(r"\d+\.0", s):
        return s[:-2]

    return s

def normalize_name(name: Optional[str]) -> str:
    """Kept for potential future use; matching here intentionally uses raw names like Excel script."""
    if not name:
        return ''
    name = name.lower()
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

def ensure_required_cols_present(df: pd.DataFrame, required: list, context: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{context}] Expected columns are missing: {missing}")

def rf_extract_one_with_cutoff(
    name: str,
    choices: list,
    score_cutoff: int
):
    """
    Try WRatio first (Excel-script default). If no match, fall back to token_set_ratio.
    Returns (matched_name, score, index) or None.
    """
    # 1. First attempt: WRatio (matches Excel fuzzywuzzy default)
    match = process.extractOne(name, choices, scorer=fuzz.WRatio, score_cutoff=score_cutoff)
    if match:
        return match

    # 2. Fallback: token_set_ratio (good when one name is a superset of the other)
    return process.extractOne(name, choices, scorer=fuzz.token_set_ratio, score_cutoff=score_cutoff)

def get_id_from_name_cached(
    name: Optional[str],
    source_df: pd.DataFrame,
    id_col: str,
    cache: dict,
    low_band_col: Optional[str] = None,
    high_band_col: Optional[str] = None,
    is_school: bool = True
):
    """
    Apply the Excel-script logic:
      - If name blank -> no match
      - fuzzy match against ELSI names with score >= MATCH_SCORE_CUTOFF
      - if matched, return (id, low_band, high_band) for schools; districts ignore bands
    """
    try:
        if not name or not isinstance(name, str):
            return None, None, None

        name = name.strip()
        if not name:
            return None, None, None

        # Fast cache
        if name in cache:
            idx = cache[name]
            row = source_df.iloc[idx]
            return (
                trunc_float(row.get(id_col, None)),
                row.get(low_band_col, None) if (is_school and low_band_col) else None,
                row.get(high_band_col, None) if (is_school and high_band_col) else None
            )

        # Choose correct pool of names
        global _school_choices, _district_choices
        choices = _school_choices if is_school else _district_choices

        best_match = rf_extract_one_with_cutoff(name, choices, MATCH_SCORE_CUTOFF)
        if best_match:
            matched_name, score, index = best_match
            cache[name] = index
            row = source_df.iloc[index]
            return (
                trunc_float(row.get(id_col, None)),
                row.get(low_band_col, None) if (is_school and low_band_col) else None,
                row.get(high_band_col, None) if (is_school and high_band_col) else None
            )

        return None, None, None

    except Exception as e:
        print(f"[get_id_from_name_cached] {e}")
        traceback.print_exc()
        return None, None, None


# ============================
# SQLite Keying & Updates
# ============================

def get_table_update_key(conn, table_name):
    """
    Determine the best key to use for UPDATEs.
    Returns a tuple: (key_type, key_name)
      - ('rowid', 'rowid') if rowid is usable
      - ('pk', <pk_colname>) if a single-column primary key exists
      - ('view', None) if it's a view (skip)
      - (None, None) if neither is available (cannot safely update)
    """
    try:
        cur = conn.cursor()

        cur.execute("SELECT type FROM sqlite_master WHERE name = ?;", (table_name,))
        row = cur.fetchone()
        if not row:
            return (None, None)
        if row[0] == 'view':
            return ('view', None)

        # Get CREATE TABLE SQL to detect WITHOUT ROWID
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name = ?;", (table_name,))
        sql_row = cur.fetchone()
        table_sql = sql_row[0] if sql_row and sql_row[0] else ""
        without_rowid = "WITHOUT ROWID" in table_sql.upper()

        if not without_rowid:
            # Most normal tables have a usable rowid; prefer it for speed/simplicity
            return ('rowid', 'rowid')

        # WITHOUT ROWID -> must use a primary key column (single-column only)
        cur.execute(f"PRAGMA table_info('{table_name}')")
        cols = cur.fetchall()
        pk_cols = [c[1] for c in cols if c[5] == 1]  # c[5] is pk flag
        if len(pk_cols) == 1:
            return ('pk', pk_cols[0])

        # No single PK; cannot safely update
        return (None, None)

    except Exception as e:
        print(f"[get_table_update_key:{table_name}] {e}")
        traceback.print_exc()
        return (None, None)

def batch_update_table(conn, table_name, df):
    """
    Ensure required columns exist in SQLite, then apply batch UPDATEs.
    Uses the appropriate key (rowid or single-column PK) per table.
    """
    try:
        print(f"Ensuring columns exist and applying batch UPDATEs to '{table_name}'...")

        # Skip if df is empty
        if df is None or df.empty:
            print(f" - No rows to update for '{table_name}'.")
            return

        # Detect the key we previously selected for this table
        key_type = df.attrs.get('__update_key_type__', None)
        key_name = df.attrs.get('__update_key_name__', None)
        if key_type is None or key_name is None:
            # Attempt a fresh detection as a fallback
            kt, kn = get_table_update_key(conn, table_name)
            if kt in ('view', None):
                print(f" - Skipping '{table_name}' during writeback; unusable key or a view.")
                return
            key_type, key_name = kt, kn

        key_col_in_df = '__key__'  # we selected it as __key__ in the SELECT earlier
        if key_col_in_df not in df.columns:
            print(f" - Missing key column __key__ in update DataFrame for '{table_name}'.")
            return

        has_school   = data_file_school_name_col   in df.columns
        has_district = data_file_district_name_col in df.columns

        required_columns = []
        if has_school:
            required_columns += [output_school_id_col, output_low_grade_band_col, output_high_grade_band_col]
        if has_district:
            required_columns.append(output_district_id_col)

        # Fetch current SQLite columns and add any missing
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        existing_columns = {row[1] for row in cursor.fetchall()}

        for col in required_columns:
            if col not in existing_columns:
                print(f" - Adding missing column '{col}' to '{table_name}'")
                cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT')

        # Make sure destination columns are in df; if not, add as None
        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        # Only update columns that we actually have values for in df
        available_cols = [col for col in required_columns if col in df.columns]
        if not available_cols:
            print(f" - No applicable columns found in '{table_name}' to update.")
            return

        # Build UPDATE
        set_clause = ", ".join([f'"{col}" = ?' for col in available_cols])
        where_col  = key_name if key_type == 'pk' else 'rowid'
        sql = f'UPDATE "{table_name}" SET {set_clause} WHERE "{where_col}" = ?'

        values = []
        for _, row in df.iterrows():
            key_val = row.get(key_col_in_df)
            if pd.isnull(key_val):
                continue
            row_values = [row.get(col) for col in available_cols]
            if all((v is None) or (str(v).strip() == "") for v in row_values):
                continue
            row_values.append(key_val)
            values.append(tuple(row_values))

        if not values:
            print(f" - No valid rows to update in '{table_name}'.")
            return

        cursor.executemany(sql, values)

    except Exception as e:
        print(f"[batch_update_table:{table_name}] {e}")
        traceback.print_exc()


# ============================
# Per-table enrichment
# ============================

def update_table_with_elsi_data(table_name, school_df, district_df, elsi_exact_index):
    """
    Enrich a single SQLite table with NCES IDs (and grade bands for schools),
    using the Excel-style thresholded fuzzy matching logic, and selecting a reliable key.
    """
    try:
        print(f"[Thread {threading.get_ident()}] Enriching table '{table_name}' with ELSI data...")

        # Open a short-lived connection to discover columns and keying
        conn = sqlite3.connect(db_file)
        cur  = conn.cursor()

        key_type, key_name = get_table_update_key(conn, table_name)
        if key_type == 'view':
            print(f" - Skipping '{table_name}': it is a VIEW (cannot update/alter).")
            conn.close()
            return table_name, None
        if key_type is None:
            print(f" - Skipping '{table_name}': no usable key (neither rowid nor single-column PK).")
            conn.close()
            return table_name, None

        cur.execute(f"PRAGMA table_info('{table_name}')")
        columns = [row[1] for row in cur.fetchall()]
        conn.close()

        has_school_col   = data_file_school_name_col   in columns
        has_district_col = data_file_district_name_col in columns

        if not (has_school_col or has_district_col):
            print(f" - Skipping '{table_name}' (no {data_file_school_name_col} or {data_file_district_name_col}).")
            return table_name, None

        # Pull the table with the chosen key; alias as __key__ for downstream code
        key_select = key_name if key_type == 'pk' else 'rowid'
        conn2 = sqlite3.connect(db_file)
        df = pd.read_sql_query(f'SELECT {key_select} AS __key__, * FROM "{table_name}"', conn2)
        conn2.close()
        
        has_school_col   = data_file_school_name_col   in df.columns
        has_district_col = data_file_district_name_col in df.columns

        school_series = (
            df[data_file_school_name_col].astype(str).fillna("").str.strip()
            if has_school_col else pd.Series([""] * len(df))
        )
        district_series = (
            df[data_file_district_name_col].astype(str).fillna("").str.strip()
            if has_district_col else None
        )

        unique_school_names   = sorted(set(n for n in school_series if n)) if has_school_col else []
        unique_district_names = sorted(set(n for n in district_series if n)) if district_series is not None else []
        
        # Exact cache: reuse your existing school_match_cache/district_match_cache first
        exact_school_hits = {n: school_match_cache[n] for n in unique_school_names if n in school_match_cache}
        exact_district_hits = {n: district_match_cache[n] for n in unique_district_names if n in district_match_cache}

        # Ensure output columns exist in the DataFrame view (added later to SQLite in batch_update_table)
        if has_school_col:
            for col in [output_school_id_col, output_low_grade_band_col, output_high_grade_band_col]:
                if col not in df.columns:
                    df[col] = None
        if has_district_col and output_district_id_col not in df.columns:
            df[output_district_id_col] = None

        # Row-wise enrichment using the Excel-like thresholded fuzzy match
        def enrich_row(row):
            new_row = row.copy()

            def preserve(existing, new):
                """Keep existing non-empty value; otherwise use new."""
                if pd.notnull(existing) and str(existing).strip() != "":
                    return existing
                return new

            # Pull names
            school_name   = str(row.get(data_file_school_name_col)   or "").strip()
            district_name = str(row.get(data_file_district_name_col) or "").strip()

            school_id = low_band = high_band = district_id = None

            # ---- 1) Try exact composite key match first: "District | School"
            if district_name or school_name:
                composite = f"{district_name} | {school_name}"
                idx = elsi_exact_index.get(composite)
                if idx is not None:
                    srow = school_df.iloc[idx]
                    school_id = trunc_float(srow.get(elsi_school_id_col, None))
                    low_band  = srow.get(elsi_low_grade_band, None)  if elsi_low_grade_band  else None
                    high_band = srow.get(elsi_high_grade_band, None) if elsi_high_grade_band else None

            # ---- 2) If no exact match, fall back to current fuzzy school & district lookups
            if school_id is None and school_name:
                school_id, low_band2, high_band2 = get_id_from_name_cached(
                    school_name, school_df, elsi_school_id_col, school_match_cache,
                    elsi_low_grade_band, elsi_high_grade_band, is_school=True
                )
                # Use bands if we didn’t get them from exact
                if low_band is None:  low_band  = low_band2
                if high_band is None: high_band = high_band2

            if district_name:
                district_id, _, _ = get_id_from_name_cached(
                    district_name, district_df, elsi_district_id_col, district_match_cache,
                    None, None, is_school=False
                )

            # ---- 3) Preserve any existing non-empty values already in the table
            if data_file_school_name_col in row:
                new_row[output_school_id_col]       = preserve(row.get(output_school_id_col),       school_id)
                new_row[output_low_grade_band_col]  = preserve(row.get(output_low_grade_band_col),  low_band)
                new_row[output_high_grade_band_col] = preserve(row.get(output_high_grade_band_col), high_band)

            if data_file_district_name_col in row:
                new_row[output_district_id_col]     = preserve(row.get(output_district_id_col),     district_id)

            return new_row

        def _best_one(q: str, choices: list, scorer, cutoff: int):
            """Return (q, (idx, score)) or (q, None) if below cutoff."""
            if not q:
                return q, None
            m = process.extractOne(q, choices, scorer=scorer, score_cutoff=cutoff)
            if not m:
                return q, None
            # m -> (choice_string, score, index)
            _, score, idx = m
            return q, (idx, score)

        def batch_best_parallel(names, choices, scorer, cutoff, workers=8):
            """
            Returns dict: query_name -> (index_in_choices, score)
            Uses thread sharding; each task calls extractOne once. Robust across RapidFuzz versions.
            """
            names = [n for n in names if n]
            if not names:
                return {}
            out = {}
            chunksize = max(1, len(names) // max(1, workers))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(lambda chunk: [ _best_one(q, choices, scorer, cutoff) for q in chunk ],
                                     names[i:i+chunksize])
                           for i in range(0, len(names), chunksize)]
                for f in futures:
                    for q, res in f.result():
                        if res is not None:
                            out[q] = res
            return out           

        # Build choices arrays once
        school_choices = school_df[elsi_school_col].fillna("").tolist()
        district_choices = district_df[elsi_district_col].fillna("").tolist()

        # 1st pass: WRatio (mirrors your Excel script)
        unmatched_schools = [n for n in unique_school_names if n not in exact_school_hits]
        school_hits_1 = batch_best_parallel(unmatched_schools, school_choices, fuzz.WRatio, MATCH_SCORE_CUTOFF)

        # 2nd pass fallback: token_set_ratio (higher cutoff to keep precision)
        still_unmatched = [n for n in unmatched_schools if n not in school_hits_1]
        school_hits_2 = batch_best_parallel(still_unmatched, school_choices, fuzz.token_set_ratio, max(MATCH_SCORE_CUTOFF, 75))

        # Merge results and update caches
        school_hits = {**{k:(exact_school_hits[k], 100) for k in exact_school_hits}, **school_hits_1, **school_hits_2}
        for name,(idx,_) in school_hits.items():
            school_match_cache[name] = idx  # persistent across tables
            
        # Map from school name -> (id, low, high)
        def school_tuple(idx):
            row = school_df.iloc[idx]
            return (
                trunc_float(row.get(elsi_school_id_col)),
                row.get(elsi_low_grade_band)  if elsi_low_grade_band  else None,
                row.get(elsi_high_grade_band) if elsi_high_grade_band else None
            )

        school_lookup = {name: school_tuple(idx) for name,(idx,_) in school_hits.items()}

        # Build result columns (preserving existing non-empty values)
        def preserve(existing, new):
            return existing if (pd.notnull(existing) and str(existing).strip() != "") else new

        if data_file_school_name_col in df.columns:
            school_ids   = []
            low_grades   = []
            high_grades  = []
            for name, cur_id, cur_low, cur_high in zip(
                school_series.tolist(),
                df.get(output_school_id_col, pd.Series([None]*len(df))).tolist(),
                df.get(output_low_grade_band_col, pd.Series([None]*len(df))).tolist(),
                df.get(output_high_grade_band_col, pd.Series([None]*len(df))).tolist(),
            ):
                tup = school_lookup.get(name)
                if tup is None:
                    school_ids.append(cur_id); low_grades.append(cur_low); high_grades.append(cur_high)
                else:
                    sid, lo, hi = tup
                    school_ids.append(preserve(cur_id, sid))
                    low_grades.append(preserve(cur_low, lo))
                    high_grades.append(preserve(cur_high, hi))
            df[output_school_id_col]       = school_ids
            df[output_low_grade_band_col]  = low_grades
            df[output_high_grade_band_col] = high_grades

        # Do the same pattern for district IDs if you populate them:
        if data_file_district_name_col in df.columns:
            unmatched_districts = [n for n in unique_district_names if n not in exact_district_hits]
            dist_hits_1 = batch_best_parallel(unmatched_districts, district_choices, fuzz.WRatio, MATCH_SCORE_CUTOFF)
            dist_hits_2 = batch_best_parallel([n for n in unmatched_districts if n not in dist_hits_1], district_choices, fuzz.token_set_ratio, max(MATCH_SCORE_CUTOFF, 75))
            dist_hits = {**{k:(exact_district_hits[k], 100) for k in exact_district_hits}, **dist_hits_1, **dist_hits_2}
            for name,(idx,_) in dist_hits.items():
                district_match_cache[name] = idx
            district_lookup = {name: trunc_float(district_df.iloc[idx].get(elsi_district_id_col)) for name,(idx,_) in dist_hits.items()}

            existing = df.get(output_district_id_col, pd.Series([None]*len(df))).tolist()
            df[output_district_id_col] = [preserve(cur, district_lookup.get(name)) for name, cur in zip(district_series.tolist(), existing)]
            
        df.attrs['__update_key_type__'] = key_type
        df.attrs['__update_key_name__'] = key_name
        
        return table_name, df
    except Exception as e:
        print(f"[update_table_with_elsi_data:{table_name}] {e}")
        traceback.print_exc()
        return table_name, None


# ============================
# Main
# ============================

def main():
    # Basic DB connectivity check
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to database: {db_file}")
        conn.close()
    except Exception as e:
        print(f"[main:connect] {e}")
        traceback.print_exc()
        return

    # Load ELSI data exactly as in the Excel script (skip first 6 rows so data starts at row 8)
    try:
        school_df = pd.read_excel(elsi_school_file, skiprows=6, dtype=str)
        district_df = pd.read_excel(elsi_district_file, skiprows=6, dtype=str)
    except Exception as e:
        print(f"[main:read_excel] {e}")
        traceback.print_exc()
        return

    # Validate expected columns
    try:
        school_required = [elsi_school_col, elsi_school_id_col]
        if elsi_low_grade_band:  school_required.append(elsi_low_grade_band)
        if elsi_high_grade_band: school_required.append(elsi_high_grade_band)
        ensure_required_cols_present(school_df, school_required, "ELSI Schools")

        ensure_required_cols_present(district_df, [elsi_district_col, elsi_district_id_col], "ELSI Districts")
    except Exception as e:
        print(f"[main:validate_elsi] {e}")
        traceback.print_exc()
        return

    # Build choice lists once (Excel script creates 'elsi_names' once)
    global _school_choices, _district_choices
    _school_choices   = school_df[elsi_school_col].fillna("").tolist()
    _district_choices = district_df[elsi_district_col].fillna("").tolist()
    
    # If the configured district column is missing from the ELSI school sheet, add it as blank
    if elsi_district_col not in school_df.columns:
        print(f"[warn] ELSI school sheet missing '{elsi_district_col}', adding empty column.")
        school_df[elsi_district_col] = ""    
    
    # Build composite keys for exact matching: "District | School"
    school_df["_key_elsi"] = (
        school_df[elsi_district_col].fillna("").str.strip() + " | " +
        school_df[elsi_school_col].fillna("").str.strip()
    )
    elsi_exact_index = {k: i for i, k in enumerate(school_df["_key_elsi"]) if k}

    # Enumerate DB tables
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        print(f"[main:list_tables] {e}")
        traceback.print_exc()
        return

    # Process tables in parallel; collect enriched DataFrames
    enriched_results = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=128) as executor:
            futures = [executor.submit(update_table_with_elsi_data, table, school_df, district_df, elsi_exact_index) for table in tables]
            for future in concurrent.futures.as_completed(futures):
                table_name, enriched_df = future.result()
                if enriched_df is not None:
                    enriched_results[table_name] = enriched_df
    except Exception as e:
        print(f"[main:thread_pool] {e}")
        traceback.print_exc()

    # Write updates back to SQLite (sequentially to minimize locking)
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("BEGIN TRANSACTION")

        for table_name, df in enriched_results.items():
            print(f"Writing enriched table '{table_name}' to database...")
            batch_update_table(conn, table_name, df)

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[main:writeback] {e}")
        traceback.print_exc()
        return

    print("Done.")

if __name__ == "__main__":
    main()
