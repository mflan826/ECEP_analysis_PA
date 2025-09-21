#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import pandas as pd
import yaml
import re
import traceback
from rapidfuzz import process, fuzz
import concurrent.futures
import threading
from typing import Optional
from tqdm import tqdm

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
file_path_prefix = config['file_path_prefix']
db_file         = f"{file_path_prefix}/{config['db_file_name']}"

# EDNA cache CSV (produced by your EDNA postprocess)
edna_cache_csv      = f"{file_path_prefix}/{config['edna_cache']}"

# Destination columns in your DB tables
output_school_name_col      = config.get('output_school_name_col')      # optional, if present we won't modify it
output_district_name_col    = config.get('output_district_name_col')    # optional, if present we won't modify it
output_school_id_col        = config['output_school_id_col']            # the 12-digit NCES school code
output_district_id_col      = config['output_district_id_col']          # the 7-digit district code
output_low_grade_band_col  = config['output_low_grade_band_col']
output_high_grade_band_col = config['output_high_grade_band_col']

# Source columns (names) inside your SQLite tables
data_file_school_name_col   = config['data_file_school_name_col']   # column containing the school name
data_file_district_name_col = config['data_file_district_name_col'] # column containing the district name

# Fuzzy match controls (EDNA-only)
ENABLE_FUZZY_MATCH = False  # set to False to disable fuzzy matching
MATCH_SCORE_CUTOFF = 60

# Global fuzzy choices (built from EDNA once)
_pair_choices = None   # list[str], each is "district | school" exact (un-normalized, for display)
_pair_choice_keys = None  # list[str], same positions, but strict-normalized keys used for matching cache


# ============================
# Utilities
# ============================

def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", str(s or ""))

def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()

def _normalize_strict(s: str) -> str:
    """Strict compare normalization: lowercase, collapse spaces, strip, truncate to 40 chars (EDNA script parity)."""
    if not s:
        return ""
    s_norm = re.sub(r"\s+", " ", str(s)).strip().lower()
    return s_norm[:40]

def _pair_key(school: str, district: str) -> str:
    """Strict-normalized key used for EDNA exact matching."""
    return f"{_normalize_strict(school)}||{_normalize_strict(district)}"

def trunc_float(value):
    """Return a clean string for integer-like values, else None/str as appropriate."""
    if value is None:
        return None
    try:
        import numpy as _np
        if (isinstance(value, float) and (_np.isnan(value) or not _np.isfinite(value))) or pd.isna(value):
            return None
    except Exception:
        if pd.isna(value):
            return None
    s = str(value).strip()
    if s == "":
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if re.fullmatch(r"\d+\.0", s):
        return s[:-2]
    return s

def ensure_required_cols_present(df: pd.DataFrame, required: list, context: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{context}] Expected columns are missing: {missing}")

def rf_extract_one_with_cutoff(name: str, choices: list[str], score_cutoff: int):
    """Try WRatio first; if none, fall back to token_set_ratio."""
    match = process.extractOne(name, choices, scorer=fuzz.WRatio, score_cutoff=score_cutoff)
    if match:
        return match
    return process.extractOne(name, choices, scorer=fuzz.token_set_ratio, score_cutoff=score_cutoff)

def _parse_low_high_from_grades(grades_str: str) -> tuple[Optional[str], Optional[str]]:
    """
    Given a comma- or space-separated 'Grades' string (e.g., 'PK, K, 1, 2, 3, 12'),
    return a (low, high) tuple using semantic grade ordering: PK < K < 1 < ... < 12.

    Returns (None, None) if nothing usable is found.
    """
    if not grades_str:
        return (None, None)

    # Tokenize: accept PK, K, and integers (1..12+; we clamp semantics at 12 for ordering)
    tokens = re.findall(r"\b(?:PK|K|\d{1,2})\b", str(grades_str), flags=re.IGNORECASE)
    if not tokens:
        return (None, None)

    # Normalize: PK/K uppercase; numerics as-is
    norm = []
    for t in tokens:
        t = t.strip()
        if not t:
            continue
        if t.upper() in ("PK", "K"):
            norm.append(t.upper())
        else:
            # keep numeric as given
            norm.append(t)

    if not norm:
        return (None, None)

    # Map to sortable keys: PK=-1, K=0, numeric=int
    def _to_key(tok: str) -> Optional[int]:
        if tok == "PK":
            return -1
        if tok == "K":
            return 0
        try:
            return int(tok)
        except Exception:
            return None

    keys = [_to_key(t) for t in norm]
    keys = [k for k in keys if k is not None]

    if not keys:
        return (None, None)

    kmin = min(keys)
    kmax = max(keys)

    def _from_key(k: int) -> str:
        if k == -1:
            return "PK"
        if k == 0:
            return "K"
        return str(k)

    return (_from_key(kmin), _from_key(kmax))

# ============================
# SQLite Keying & Updates
# ============================

def get_table_update_key(conn, table_name):
    """
    Determine the best key to use for UPDATEs.
    Returns: (key_type, key_name)
      - ('rowid','rowid') if rowid usable
      - ('pk', <pk_colname>) if single-column PK
      - ('view', None) if it's a view
      - (None, None) if neither
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT type FROM sqlite_master WHERE name = ?;", (table_name,))
        row = cur.fetchone()
        if not row:
            return (None, None)
        if row[0] == 'view':
            return ('view', None)

        cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name = ?;", (table_name,))
        sql_row = cur.fetchone()
        table_sql = sql_row[0] if sql_row and sql_row[0] else ""
        without_rowid = "WITHOUT ROWID" in table_sql.upper()

        if not without_rowid:
            return ('rowid', 'rowid')

        cur.execute(f"PRAGMA table_info('{table_name}')")
        cols = cur.fetchall()
        pk_cols = [c[1] for c in cols if c[5] == 1]
        if len(pk_cols) == 1:
            return ('pk', pk_cols[0])

        return (None, None)

    except Exception as e:
        print(f"[get_table_update_key:{table_name}] {e}")
        traceback.print_exc()
        return (None, None)

def batch_update_table(conn, table_name, df):
    """Ensure required columns exist in SQLite, then apply batch UPDATEs."""
    try:
        print(f"Ensuring columns exist and applying batch UPDATEs to '{table_name}'...")

        if df is None or df.empty:
            print(f" - No rows to update for '{table_name}'.")
            return

        key_type = df.attrs.get('__update_key_type__', None)
        key_name = df.attrs.get('__update_key_name__', None)
        if key_type is None or key_name is None:
            kt, kn = get_table_update_key(conn, table_name)
            if kt in ('view', None):
                print(f" - Skipping '{table_name}' during writeback; unusable key or a view.")
                return
            key_type, key_name = kt, kn

        key_col_in_df = '__key__'
        if key_col_in_df not in df.columns:
            print(f" - Missing key column __key__ in update DataFrame for '{table_name}'.")
            return

        has_school   = data_file_school_name_col   in df.columns
        has_district = data_file_district_name_col in df.columns

        # Build required columns set:
        required_columns = []
        if has_school:
            required_columns.append(output_school_id_col)
        if has_district:
            required_columns.append(output_district_id_col)

        # NEW: always require low/high grade band columns
        required_columns.append(output_low_grade_band_col)
        required_columns.append(output_high_grade_band_col)

        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        existing_columns = {row[1] for row in cursor.fetchall()}

        # Add missing columns as TEXT
        for col in required_columns:
            if col not in existing_columns:
                print(f" - Adding missing column '{col}' to '{table_name}'")
                cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT')

        # Ensure DataFrame has the required columns
        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        available_cols = [col for col in required_columns if col in df.columns]
        if not available_cols:
            print(f" - No applicable columns found in '{table_name}' to update.")
            return

        set_clause = ", ".join([f'"{col}" = ?' for col in available_cols])
        where_col  = key_name if key_type == 'pk' else 'rowid'
        sql = f'UPDATE "{table_name}" SET {set_clause} WHERE "{where_col}" = ?'

        values = []
        for _, row in df.iterrows():
            key_val = row.get(key_col_in_df)
            if pd.isnull(key_val):
                continue
            row_values = [row.get(col) for col in available_cols]

            # If absolutely nothing to write, skip row
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
# EDNA-only enrichment
# ============================

def update_table_with_edna_data(table_name, edna_pair_to_12, edna_pair_to_dist7,
                                edna_pair_to_low, edna_pair_to_high):
    """
    For each row in 'table_name', write:
      - NCES 12-digit (school) and 7-digit (district), and
      - Low/High grade band strings based on EDNA 'Grades' (if available)

    Resolution order:
      1) Exact strict-normalized pair match on (school, district).
      2) If ENABLE_FUZZY_MATCH=True and no exact hit:
         - Fuzzy match *district | school* against EDNA pair list and use that pair.

    Notes:
      - Low/high grade bands are simple strings like "PK", "K", "1", ..., "12".
      - If the EDNA cache lacks 'Grades' or a particular pair lacks grade info,
        low/high remain None for that row.
    """
    try:
        print(f"[Thread {threading.get_ident()}] Enriching table '{table_name}' with EDNA data...")

        # Inspect structure/keying
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

        # Pull rows
        key_select = key_name if key_type == 'pk' else 'rowid'
        conn2 = sqlite3.connect(db_file)
        df = pd.read_sql_query(f'SELECT {key_select} AS __key__, * FROM "{table_name}"', conn2)
        conn2.close()

        has_school_col   = data_file_school_name_col   in df.columns
        has_district_col = data_file_district_name_col in df.columns

        # Ensure destination columns exist in the DataFrame
        if has_school_col and output_school_id_col not in df.columns:
            df[output_school_id_col] = None
        if has_district_col and output_district_id_col not in df.columns:
            df[output_district_id_col] = None

        # NEW: always create low/high grade band columns in the DataFrame
        if output_low_grade_band_col not in df.columns:
            df[output_low_grade_band_col] = None
        if output_high_grade_band_col not in df.columns:
            df[output_high_grade_band_col] = None

        school_series   = df[data_file_school_name_col].astype(str).fillna("").str.strip() if has_school_col else pd.Series([""] * len(df))
        district_series = df[data_file_district_name_col].astype(str).fillna("").str.strip() if has_district_col else pd.Series([""] * len(df))

        def preserve(existing, new):
            return existing if (pd.notnull(existing) and str(existing).strip() != "") else new

        out_school12 = df.get(output_school_id_col, pd.Series([None]*len(df))).tolist()
        out_dist7    = df.get(output_district_id_col, pd.Series([None]*len(df))).tolist()
        out_low      = df.get(output_low_grade_band_col, pd.Series([None]*len(df))).tolist()
        out_high     = df.get(output_high_grade_band_col, pd.Series([None]*len(df))).tolist()

        # Prepare fuzzy choices once (EDNA pair strings "district | school")
        global _pair_choices, _pair_choice_keys
        do_fuzzy = ENABLE_FUZZY_MATCH and (_pair_choices is not None) and (len(_pair_choices) > 0)

        rows = list(zip(school_series.tolist(), district_series.tolist()))
        for i, (sname, dname) in enumerate(
                tqdm(rows, desc=f"EDNA enrich: {table_name}", unit="row", leave=False)):
            cur12    = out_school12[i]
            curDist7 = out_dist7[i]
            curLow   = out_low[i]
            curHigh  = out_high[i]

            best12 = None
            bestDist7 = None
            bestLow = None
            bestHigh = None

            # 1) Exact strict-normalized pair
            k = _pair_key(sname, dname)
            if k in edna_pair_to_12 or k in edna_pair_to_dist7 or k in edna_pair_to_low or k in edna_pair_to_high:
                best12    = edna_pair_to_12.get(k)
                bestDist7 = edna_pair_to_dist7.get(k)
                bestLow   = edna_pair_to_low.get(k)
                bestHigh  = edna_pair_to_high.get(k)

            # 2) Optional fuzzy pair matching
            if (best12 is None and bestDist7 is None and bestLow is None and bestHigh is None) and do_fuzzy:
                composite = f"{dname} | {sname}".strip()
                query_candidates = [composite] if composite != " | " else []
                if not dname and sname:
                    query_candidates.append(sname)
                elif dname and not sname:
                    query_candidates.append(dname)

                for qc in query_candidates:
                    match = rf_extract_one_with_cutoff(qc, _pair_choices, MATCH_SCORE_CUTOFF)
                    if match:
                        matched_label, _score, idx = match
                        nk = _pair_choice_keys[idx]
                        best12    = edna_pair_to_12.get(nk, best12)
                        bestDist7 = edna_pair_to_dist7.get(nk, bestDist7)
                        bestLow   = edna_pair_to_low.get(nk, bestLow)
                        bestHigh  = edna_pair_to_high.get(nk, bestHigh)
                        # keep first acceptable fuzzy hit
                        if any([best12, bestDist7, bestLow, bestHigh]):
                            break

            # If only 12-digit is known, back-derive district 7
            if (not bestDist7) and best12:
                d = _digits_only(best12)
                if len(d) == 12:
                    bestDist7 = d[:7]

            out_school12[i] = preserve(cur12, best12)
            out_dist7[i]    = preserve(curDist7, bestDist7)
            out_low[i]      = preserve(curLow, bestLow)
            out_high[i]     = preserve(curHigh, bestHigh)

        # Assign columns back
        if has_school_col:
            df[output_school_id_col] = out_school12
        if has_district_col:
            df[output_district_id_col] = out_dist7
        df[output_low_grade_band_col]  = out_low
        df[output_high_grade_band_col] = out_high

        # Preserve keying info for writeback
        df.attrs['__update_key_type__'] = key_type
        df.attrs['__update_key_name__'] = key_name
        return table_name, df

    except Exception as e:
        print(f"[update_table_with_edna_data:{table_name}] {e}")
        traceback.print_exc()
        return table_name, None


# ============================
# Main
# ============================

def main():
    # 0) Basic DB connectivity check
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to database: {db_file}")
        conn.close()
    except Exception as e:
        print(f"[main:connect] {e}")
        traceback.print_exc()
        return

    # 1) Load EDNA cache and build strict-normalized pair maps
    edna_pair_to_12    = {}
    edna_pair_to_dist7 = {}
    # NEW: maps for low/high grade bands (strings like "PK" or "12")
    edna_pair_to_low   = {}
    edna_pair_to_high  = {}

    pair_labels        = []  # for fuzzy: "district | school"
    pair_keys          = []  # normalized keys aligned with pair_labels

    try:
        edna = pd.read_csv(edna_cache_csv, dtype=str).fillna("")
        # Expected columns (as in your EDNA postprocess):
        #   "School Name", "District Name", "NCES 12-digit (District+Branch)", "District NCES"
        ensure_required_cols_present(
            edna,
            ["School Name", "District Name", "NCES 12-digit (District+Branch)", "District NCES"],
            "EDNA cache CSV"
        )

        # Grades column is optional but recommended
        has_grades_col = "Grades" in edna.columns
        if not has_grades_col:
            print("[main:edna_lookup] WARNING: 'Grades' column not found in EDNA cache; grade bands will be skipped.")

        for _, r in tqdm(edna.iterrows(),
                         total=len(edna),
                         desc="Indexing EDNA pairs",
                         unit="row"):
            sname = r.get("School Name", "")
            dname = r.get("District Name", "")
            k = _pair_key(sname, dname)

            code12 = _digits_only(r.get("NCES 12-digit (District+Branch)", ""))
            dist7  = _digits_only(r.get("District NCES", ""))

            if k and code12:
                edna_pair_to_12[k] = code12
            if k and len(dist7) == 7:
                edna_pair_to_dist7[k] = dist7

            # Build fuzzy label alignment
            label = f"{_normalize_space(dname)} | {_normalize_space(sname)}".strip()
            pair_labels.append(label)
            pair_keys.append(k)

            # NEW: parse Grades -> (low, high)
            if has_grades_col:
                grades_str = r.get("Grades", "")
                low, high = _parse_low_high_from_grades(grades_str)
                if k:
                    if low is not None:
                        edna_pair_to_low[k] = low
                    if high is not None:
                        edna_pair_to_high[k] = high

        print(f"[main] EDNA pairs loaded: {len(edna_pair_to_12)} with 12-digit; {len(edna_pair_to_dist7)} with district 7")

        # Prepare global fuzzy choice arrays if enabled
        global _pair_choices, _pair_choice_keys
        if ENABLE_FUZZY_MATCH and len(pair_labels) > 0:
            _pair_choices     = pair_labels
            _pair_choice_keys = pair_keys
        else:
            _pair_choices     = []
            _pair_choice_keys = []

    except Exception as e:
        print(f"[main:edna_lookup] Unable to read or validate '{edna_cache_csv}': {e}")
        traceback.print_exc()
        # Proceeding is allowed; enrichment may be partial.

    # 2) Enumerate tables
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

    # 3) Process tables in parallel
    enriched_results = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=128) as executor:
            futures = [
                executor.submit(
                    update_table_with_edna_data,
                    table,
                    edna_pair_to_12,
                    edna_pair_to_dist7,
                    # NEW: pass grade-band maps
                    edna_pair_to_low,
                    edna_pair_to_high,
                )
                for table in tables
            ]
            with tqdm(total=len(futures), desc="Enriching tables (parallel)", unit="table") as pbar:
                for future in concurrent.futures.as_completed(futures):
                    table_name, enriched_df = future.result()
                    if enriched_df is not None:
                        enriched_results[table_name] = enriched_df
                    pbar.update(1)

    except Exception as e:
        print(f"[main:thread_pool] {e}")
        traceback.print_exc()

    # 4) Write back sequentially
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("BEGIN TRANSACTION")

        for table_name, df in tqdm(enriched_results.items(),
                                   total=len(enriched_results),
                                   desc="Writing tables to SQLite",
                                   unit="table"):
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
