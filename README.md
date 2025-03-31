# ECEP_analysis_PA
ECEP and PA (code.org) CMP Data Request

## Configuration

Edit `config.yaml` and set:

```
file_path_prefix: '..'

db_file_name: imported_excel.db

years:
  - "18-19"
  - "19-20"
  - "20-21"
  - "21-22"
  - "22-23"
  - "23-24"

data_file_path_prefix: '../PDE EXPORTS'

data_file_school_name_col: "LOCATION_NAME"
data_file_district_name_col: "DISTRICT_NAME"

data_file_password: 'YOUR PASSWORD HERE'
```

### CMP

Also add these lines to `config.yaml`:

```
cmp_output_file_name: CMP_Data_Populated.xlsx

cmp_courses_file_name: Courses.xlsx

elsi_school_file_name: ELSI_excel_export_6387876108154778256164.xlsx
elsi_district_file_name: ELSI_excel_export_6387858886530040709219.xlsx

output_school_name_col: "School Name"
output_district_name_col: "District Name"
output_school_id_col: "School Number (NCES)"
output_district_id_col: "District Number (NCES)"
output_low_grade_band_col: "Lowest Grade Level Served"
output_high_grade_band_col: "Highest Grade Level Served"

elsi_school_col: "School Name"
elsi_district_col: "Agency Name"
elsi_school_id_col: "School ID (12-digit) - NCES Assigned [Public School] Latest available year"
elsi_district_id_col: "Agency ID - NCES Assigned [District] Latest available year"
elsi_low_grade_band: "Lowest Grade Offered [Public School] 2023-24"
elsi_high_grade_band: "Highest Grade Offered [Public School] 2023-24"
```

### PA (code.org)

Also add these lines to `config.yaml`:

```
pa_output_file_name: PA_Data_Populated.xlsx
```

## Usage

### Initial Setup and Data Import
Run `sqlite_import.py` script, then `cmp_school_elsi_data.py` data script

### CMP

Run `cmp_populate_jinja.py` script

### PA State code.org Report

Run `pa_populate_jinja.py` script
