import sqlite3
from jinja2 import Template
import yaml # pip install pyyaml
import pandas as pd
import openpyxl # pip install openpyxl
from openpyxl import load_workbook

conn = sqlite3.connect("imported_excel.db")

# Load configuration from YAML
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)
    
school_year_dash = config['years']
school_year_splat = [yr.replace('-', '_') for yr in school_year_dash]
cmp_file_path_prefix = config['cmp_file_path_prefix']
cmp_output_file = f"{cmp_file_path_prefix}/{config['cmp_output_file_name']}"

def clear_and_append_dataframes_to_excel(filepath, sheet_name, dataframes):
    """
    Clears all rows (except the header) in the given sheet, then appends filtered DataFrames 
    starting at row 2 using openpyxl. Any DataFrame columns not found in the sheet header are skipped and logged.
    Compatible with pandas 1.3.5 and avoids using df.to_excel().
    """
    try:
        book = load_workbook(filepath)
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{filepath}' not found.")

    if sheet_name not in book.sheetnames:
        raise ValueError(f"Sheet '{sheet_name}' not found in the workbook.")

    sheet = book[sheet_name]

    # Read existing header (row 1)
    header = [cell.value for cell in sheet[1] if cell.value]
    if not header:
        raise ValueError(f"No header found in row 1 of sheet '{sheet_name}'.")

    # Clear all rows except the header
    max_row = sheet.max_row
    if max_row > 1:
        sheet.delete_rows(2, max_row - 1)

    # Start writing at row 2 (Excel is 1-indexed)
    start_row = 2

    for i, df in enumerate(dataframes):
        df_cols = list(df.columns)
        common_columns = [col for col in header if col in df_cols]
        skipped_columns = [col for col in df_cols if col not in header]

        if skipped_columns:
            print(f"[DataFrame {i+1}] Skipped columns not in '{sheet_name}': {skipped_columns}")

        if not common_columns:
            print(f"[DataFrame {i+1}] No matching columns found for sheet '{sheet_name}'. Skipping.")
            continue
        else:
            print(f"Writing columns to [DataFrame {i+1}] in '{sheet_name}': {common_columns}")

        # Build a mapping from column name to Excel column index
        header_column_map = {col_name: idx + 1 for idx, col_name in enumerate(header)}

        # Write each cell to the correct column by name
        for r_idx, row in df.iterrows():
            for col in common_columns:
                value = row.get(col, '')
                col_idx = header_column_map[col]  # get the correct column index
                sheet.cell(row=start_row + r_idx, column=col_idx, value=value)

    # Save changes to the workbook
    book.save(filepath)

cs_dfs = []
pop_dfs = []

for script in ['school_cs_jinja.sql', 'school_pop_jinja.sql']:
    # Load the SQL template
    with open(script) as file:
        template = Template(file.read())

    for i in range(len(school_year_dash)):
        dash = school_year_dash[i]
        splat = school_year_splat[i]
        
        # Render the SQL with actual values
        rendered_sql = template.render(school_year_dash=dash, school_year_splat=splat)

        cursor = conn.cursor()

        cursor.execute(rendered_sql)
        
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # Save to CSV using pandas
        df = pd.DataFrame(results, columns=column_names)
        df.to_csv(f"{script}-{splat}.csv", index=False)
        
        if script == 'school_cs_jinja.sql':
            cs_dfs.append(df)
        elif script == 'school_pop_jinja.sql':
            pop_dfs.append(df)
        else:
            print("Warning: No matching script array for appending the dataframe for writing.")

conn.close()

clear_and_append_dataframes_to_excel(cmp_output_file, "School CS Data", cs_dfs)
clear_and_append_dataframes_to_excel(cmp_output_file, "School Pop. Data", pop_dfs)
