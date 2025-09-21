import sqlite3
from jinja2 import Template
import yaml  # pip install pyyaml
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
import os
import traceback

# Load configuration from YAML
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

school_year_dash = config['years']
school_year_splat = [yr.replace('-', '_') for yr in school_year_dash]
file_path_prefix = config['file_path_prefix']
cmp_output_file = f"{file_path_prefix}/{config['cmp_output_file_name']}"
db_file = f"{file_path_prefix}/{config['db_file_name']}"

# ELSI fields from config
elsi_school_id_col = config['elsi_school_id_col']
elsi_district_id_col = config['elsi_district_id_col']
elsi_low_grade_band = config['elsi_low_grade_band']
elsi_high_grade_band = config['elsi_high_grade_band']

SCHEMA_CS = [
    "School Year",
    "School Number (NCES)", "School Number (State)",
    "District Number (NCES)", "District Number (State)",
    "Category",
    "Basic_Courses", "Basic_Total",
    "Adv_Courses", "Adv_Total"
]

SCHEMA_POP = [
    "School Year",
    "School Number (NCES)", "School Number (State)",
    "District Number (NCES)", "District Number (State)",
    "Lowest Grade Level Served", "Highest Grade Level Served",
    "Girls", "Boys", "Gender X", "Total"
]

def _canon(name: str) -> str:
    s = str(name) if name is not None else ""
    s = s.replace("\xa0", " ")
    s = " ".join(s.split())
    return s.strip().lower()
    
def _ensure_parent_dir(path: str):
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

def init_workbook(filepath: str,
                  sheets_to_headers: dict[str, list[str]],
                  header_row: int = 1,
                  overwrite: bool = True):
    """
    Create (or overwrite) an .xlsx workbook with specified sheets and headers.
    - Freezes panes under the header row.
    - Applies an AutoFilter to the header row.
    - Writes headers exactly as provided, in order.
    """
    _ensure_parent_dir(filepath)
    if overwrite and os.path.exists(filepath):
        os.remove(filepath)

    wb = Workbook()
    first = True
    for sheet_name, headers in sheets_to_headers.items():
        if first:
            ws = wb.active
            ws.title = sheet_name
            first = False
        else:
            ws = wb.create_sheet(title=sheet_name)

        # Write headers
        for idx, h in enumerate(headers, start=1):
            ws.cell(row=header_row, column=idx, value=h)

        # Freeze panes below header
        ws.freeze_panes = f"A{header_row+1}"

        # AutoFilter
        last_col = get_column_letter(len(headers))
        ws.auto_filter.ref = f"A{header_row}:{last_col}{header_row}"

    wb.save(filepath)    

def normalize_school_pop_df(df: pd.DataFrame, school_year_value: str) -> pd.DataFrame:
    try:
        df = df.copy()
        df['School Year'] = school_year_value

        # Mirror NCES -> State
        if 'School Number (NCES)' in df.columns:
            df['School Number (State)'] = df['School Number (NCES)']
        else:
            if 'School Number (State)' not in df.columns:
                df['School Number (State)'] = pd.NA

        if 'District Number (NCES)' in df.columns:
            df['District Number (State)'] = df['District Number (NCES)']
        else:
            if 'District Number (State)' not in df.columns:
                df['District Number (State)'] = pd.NA

        # Robust numeric helper: always return a Series aligned to df.index
        def _num(col: str) -> pd.Series:
            if col in df.columns:
                return pd.to_numeric(df[col], errors='coerce').fillna(0)
            # column missing -> Series of zeros with same index
            return pd.Series(0, index=df.index, dtype='float64')

        # Total = Girls + Boys + Gender X
        total = _num('Girls') + _num('Boys') + _num('Gender X')
        # If you want plain ints where possible but allow NA if any:
        df['Total'] = total.round().astype('Int64')

        return df
    except Exception as e:
        print(f"[normalize_school_pop_df] {e}")
        traceback.print_exc()
        return df

def normalize_school_cs_df(df: pd.DataFrame, school_year_value: str) -> pd.DataFrame:
    try:
        df = df.copy()
        df['School Year'] = school_year_value

        # Authoritative mirroring NCES -> State to avoid blanks
        for col in ['School Number (NCES)', 'District Number (NCES)',
                    'School Number (State)', 'District Number (State)']:
            if col not in df.columns:
                df[col] = pd.NA
        if 'School Number (NCES)' in df.columns:
            df['School Number (State)'] = df['School Number (NCES)']
        if 'District Number (NCES)' in df.columns:
            df['District Number (State)'] = df['District Number (NCES)']

        if 'Category' not in df.columns:
            df['Category'] = pd.NA

        # Filter rows where all four are zero: Basic_Courses, Basic_Total, Adv_Courses, Adv_Total
        def _num(col: str) -> pd.Series:
            if col in df.columns:
                return pd.to_numeric(df[col], errors='coerce').fillna(0)
            return pd.Series(0, index=df.index, dtype='float64')

        basic_courses = _num('Basic_Courses')
        basic_total   = _num('Basic_Total')
        adv_courses   = _num('Adv_Courses')
        adv_total     = _num('Adv_Total')

        keep = ~((basic_courses == 0) & (basic_total == 0) &
                 (adv_courses == 0)   & (adv_total == 0))
        df = df[keep].reset_index(drop=True)

        return df
    except Exception as e:
        print(f"[normalize_school_cs_df] {e}")
        traceback.print_exc()
        return df
        
def write_dataframes_to_excel(filepath: str,
                              sheet_name: str,
                              dataframes: list[pd.DataFrame],
                              header_row: int = 1,
                              start_row: int = 2,
                              declared_headers: list[str] | None = None):
    """
    Write multiple DataFrames into an existing sheet, clearing previous data rows.
    - Uses the sheet's header row (created by init_workbook) as the source of truth.
    - If DataFrames contain columns that are not in the header, they are appended
      to the right and written thereafter.
    - All writes use exact column positions—no shifting.

    Parameters:
      declared_headers: the initial schema we created for this sheet. Used to
                        keep the "core" headers left-anchored; any extras will
                        be appended to the right in first-seen order.
    """
    def escape_for_excel(val):
        if pd.isnull(val):
            return ''
        s = str(val)
        if '-' in s or '/' in s:
            return f'="{s}"'
        return s

    try:
        wb = load_workbook(filepath)
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Sheet '{sheet_name}' not found in '{filepath}'.")

        ws = wb[sheet_name]

        # Build header maps (original + canonical)
        header_cells = list(ws.iter_rows(min_row=header_row, max_row=header_row, values_only=False))[0]
        header_index = {}         # original text -> col index
        header_index_canon = {}   # canonical text -> col index
        for idx, cell in enumerate(header_cells, start=1):
            if cell.value is None:
                continue
            original = str(cell.value)
            canon = _canon(original)
            header_index[original] = idx
            if canon not in header_index_canon:
                header_index_canon[canon] = idx

        # Determine columns present in the incoming data that the header doesn't yet contain
        # (we will append these to the right, once, preserving first-seen order)
        extras = []
        seen_canon = set(header_index_canon.keys())
        for df in (dataframes or []):
            for col in df.columns:
                c = _canon(col)
                if c not in seen_canon:
                    extras.append(col)       # original DF spelling
                    seen_canon.add(c)

        # If there are extras, append their headers to the sheet, then rebuild maps
        if extras:
            start_col = ws.max_column + 1
            for offset, col_name in enumerate(extras):
                ws.cell(row=header_row, column=start_col + offset, value=col_name)

            header_cells = list(ws.iter_rows(min_row=header_row, max_row=header_row, values_only=False))[0]
            header_index.clear()
            header_index_canon.clear()
            for idx, cell in enumerate(header_cells, start=1):
                if cell.value is None:
                    continue
                original = str(cell.value)
                canon = _canon(original)
                header_index[original] = idx
                if canon not in header_index_canon:
                    header_index_canon[canon] = idx

        # Now clear old data rows
        if ws.max_row > (start_row - 1):
            ws.delete_rows(start_row, ws.max_row - (start_row - 1))

        # Prepare the column write order:
        #   - core = declared_headers that actually exist in the sheet
        #   - then the extras we just appended (in the order we saw them)
        if declared_headers is None:
            core_order = [str(c.value) for c in header_cells if c.value is not None]
        else:
            core_order = [h for h in declared_headers if _canon(h) in header_index_canon]

        extra_order = [e for e in extras if _canon(e) in header_index_canon]

        write_order = core_order + [e for e in extra_order if e not in core_order]

        # Write rows from all DataFrames sequentially
        r = start_row
        for df in (dataframes or []):
            # Decide which columns from this df we can write (intersection with sheet headers)
            df_cols = list(df.columns)
            writable_cols = [col for col in write_order if _canon(col) in {_canon(c) for c in df_cols}]
            # map canonical col -> df original name for quick lookup
            canon_map = { _canon(c): c for c in df_cols }

            for _, row in df.iterrows():
                for col in writable_cols:
                    col_idx = header_index_canon.get(_canon(col))
                    if col_idx is None:
                        continue
                    src_col = canon_map.get(_canon(col))
                    val = row.get(src_col, '')
                    val = escape_for_excel(val)
                    cell = ws.cell(row=r, column=col_idx, value=val)
                    cell.number_format = '@'
                r += 1

        wb.save(filepath)
        print(f"Successfully wrote sheet '{sheet_name}' in '{filepath}'.")

    except Exception as e:
        print(f"[write_dataframes_to_excel] {e}")
        traceback.print_exc()
        raise

cs_dfs = []
pop_dfs = []

conn = sqlite3.connect(db_file)

for script in ['school_cs_jinja.sql', 'school_pop_jinja.sql']:
    with open(script) as file:
        template = Template(file.read())

    for i in range(len(school_year_dash)):
        dash = school_year_dash[i]
        splat = school_year_splat[i]
        
        rendered_sql = template.render(school_year_dash=dash, school_year_splat=splat, high_school_only=True) # CMP is grades 9-12 only, so high_school_only = True

        cursor = conn.cursor()
        cursor.execute(rendered_sql)

        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(results, columns=column_names)

        if script == 'school_cs_jinja.sql':
            df = normalize_school_cs_df(df, school_year_value=dash)
            cs_dfs.append(df)
        elif script == 'school_pop_jinja.sql':
            df = normalize_school_pop_df(df, school_year_value=dash)
            pop_dfs.append(df)
        else:
            print("Warning: No matching script array for appending the dataframe for writing.")

conn.close()

init_workbook(
    cmp_output_file,
    sheets_to_headers={
        "School CS Data": SCHEMA_CS,
        "School Pop. Data": SCHEMA_POP
    },
    header_row=1,
    overwrite=True  # start clean each run; set to False if you prefer to keep prior file
)

write_dataframes_to_excel(
    filepath=cmp_output_file,
    sheet_name="School CS Data",
    dataframes=cs_dfs,
    header_row=1,
    start_row=2,
    declared_headers=SCHEMA_CS
)

write_dataframes_to_excel(
    filepath=cmp_output_file,
    sheet_name="School Pop. Data",
    dataframes=pop_dfs,
    header_row=1,
    start_row=2,
    declared_headers=SCHEMA_POP
)

print("Done.")