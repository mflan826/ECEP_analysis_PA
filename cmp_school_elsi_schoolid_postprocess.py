import pandas as pd
from fuzzywuzzy import process
from openpyxl import load_workbook
from tqdm import tqdm

# File paths
cmp_filename = "CMP Data Template (long format)_PA.xlsx"
elsi_filename = "ELSI_excel_export_6387876530335112514653.xlsx"

# Sheet names
cmp_sheets = ["School Pop. Data", "School CS Data"]
elsi_sheet = "ELSI Export"

# Load ELSI data from row 8 (skip first 7 rows)
elsi_df = pd.read_excel(elsi_filename, sheet_name=elsi_sheet, skiprows=6, dtype=str)

# Ensure necessary columns are present
if "School Name" not in elsi_df.columns or \
   "School ID (12-digit) - NCES Assigned [Public School] Latest available year" not in elsi_df.columns:
    raise ValueError("Expected columns are missing from ELSI file.")

# Create lookup data
elsi_names = elsi_df["School Name"].fillna("").tolist()
elsi_nces_ids = elsi_df["School ID (12-digit) - NCES Assigned [Public School] Latest available year"].fillna("")

# Load CMP Excel file with openpyxl engine to preserve formats
wb = load_workbook(cmp_filename)

for sheet in cmp_sheets:
    print(f"Processing sheet: {sheet}")
    df = pd.read_excel(cmp_filename, sheet_name=sheet, dtype=str)

    # Ensure the "School Number (NCES)" column exists
    if "School Number (NCES)" not in df.columns:
        df["School Number (NCES)"] = ""

    # Progress bar for fuzzy matching
    for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"Matching schools in {sheet}"):
        school_name = str(row["School Name"]) if pd.notnull(row["School Name"]) else ""
        if not school_name.strip():
            continue

        best_match, score = process.extractOne(school_name, elsi_names)
        if score >= 60:
            match_row = elsi_df[elsi_df["School Name"] == best_match]
            if not match_row.empty:
                nces_id = match_row.iloc[0]["School ID (12-digit) - NCES Assigned [Public School] Latest available year"]
                df.at[idx, "School Number (NCES)"] = nces_id

    # Progress bar for writing updated data back to the workbook
    for col_idx, col_name in enumerate(df.columns, start=1):
        for row_idx, val in tqdm(enumerate(df[col_name], start=2),
                                 total=len(df),
                                 desc=f"Writing to workbook ({sheet}, col: {col_name})",
                                 leave=False):
            wb[sheet].cell(row=row_idx, column=col_idx).value = val

# Save the updated workbook
wb.save("CMP Data Template (long format)_PA - Updated.xlsx")
print("File saved as 'CMP Data Template (long format)_PA - Updated.xlsx'")
