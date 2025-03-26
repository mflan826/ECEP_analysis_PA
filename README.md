# ECEP_analysis_PA
ECEP CMP Data Request

## Configuration

Edit `config.yaml` and set:

```
cmp_file_path_prefix: '..'
cmp_output_file_name: CMP_Data_Populated.xlsx

elsi_school_file_name: ELSI_excel_export_6387858826061371807671.xlsx
elsi_district_file_name: ELSI_excel_export_6387858886530040709219.xlsx

years:
  - "18-19"
  - "19-20"
  - "20-21"
  - "21-22"
  - "22-23"
  - "23-24"

data_file_path_prefix: '../EXPORTS'

data_file_password: 'EXCEL FILE PASSWORD HERE'
```

## Usage

Run import py script, then jinja py script, then school number script

