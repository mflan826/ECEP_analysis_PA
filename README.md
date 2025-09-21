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

edna_cache: "edna_output.csv"
```

### PA (code.org)

Also add these lines to `config.yaml`:

```
pa_school_output_file_name: PA_School_Data_Populated.xlsx
pa_statewide_output_file_name: PA_Statewide_Data_Populated.xlsx
```

## Usage

### Initial Setup and Data Import
Run the `sqlite_import.py` script.

### Augment with External Data Reports

1. ELSI Data: Obtain ELSI NCES code data from [https://nces.ed.gov/ccd/elsi/](https://nces.ed.gov/ccd/elsi/)
2. School Fast Facts (PA AUN Numbers): obtain `SchoolFastFacts_20232024.xlsx - School Fast Facts.csv`
3. Run `python aun_to_nces.py SchoolFastFacts_20232024.xlsx\ -\ School\ Fast\ Facts.csv AUN Schl` to pre-populate the edna school data cache

#### Data augmentation with School Location Data and Grade Bands
Run the `cmp_school_elsi_data.py` data script (fuzzy matching using ELSI reports) **or** the `cmp_school_edna_data.py` data script (no fuzzy matching using EDNA/AUN data cache).

### CMP

Run the `cmp_populate_jinja.py` script.

#### Post-Processing

Run `cmp_school_elsi_schoolid_postprocess.py` to fuzzy match against the ELSI school data report for NCES codes, **or** run `cmp_school_edna_schoolid_postprocess.py` to match against online EDNA school data for NCES codes.

Copy the `CMP_Data_Populated - Updated.xlsx` sheets into the CMP Data Template file.

### PA State code.org Report

Run `pa_school_populate_jinja.py` script and `pa_statewide_populate_jinja.py` script