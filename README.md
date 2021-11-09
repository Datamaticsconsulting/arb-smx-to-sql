# SMX to BTEQ

SMX to BTEQ is a Python project for translating Excel based SMX to BTEQ executables. The project runs on a batch of input parameters and generates bulk BTEQ scripts.

## Built With

The project mainly makes use of the following python Libraries

* [Pandas](https://github.com/pandas-dev/pandas)
* [Openpyxl](https://openpyxl.readthedocs.io/en/stable/)
* [Pyyaml](https://pyyaml.org/)

# Getting Started
The following can be treated as a guide to setup the project locally

## Prerequisites
The required dependencies for the project are
* pandas

    ```bash
    pip install pandas
    ```
* openpyxl

    ```bash
    pip install openpyxl
    ```
* pyyaml

    ```bash
    pip install pyyaml
    ```
## Installation

Use the git url to clone the repo on local machine

```bash
git clone https://github.com/Datamaticsconsulting/arb-smx-to-sql.git
```

## Usage

Projects starting point is the config file where directories for required files are configured. Also the rule engine resides within the config file.

```yaml
## SCRIPT TEMPLATES ##
SQL_TEMPLATES:
  RECYCLE: C:\Users\HP\Desktop\DMC\AlRajhi\py-projects\templates\template_recycle.txt
  RECYCLE_NO: C:\Users\HP\Desktop\DMC\AlRajhi\py-projects\templates\template.txt
  SP_CALL_TEMPLATE: C:\Users\HP\Desktop\DMC\AlRajhi\py-projects\templates\sp_call_template.txt

LOGGERINFO:
  LOG_PATH : /home/cornelius/kafka_py/logs/

## PROJECT DIRECTORY ##
PD_PATH:
  SQL : C:\Users\HP\Desktop\DMC\AlRajhi\py-projects\SQL_SCRIPTS_RRA\
  MAIN: C:\Users\HP\Desktop\DMC\AlRajhi\py-projects\
  JOBS: C:\Users\HP\Desktop\DMC\AlRajhi\py-projects\excel_files\jobs_execution.xlsx
  JOBS_STATUS: C:\Users\HP\Desktop\DMC\AlRajhi\py-projects\excel_files\jobs_execution_status.xlsx
  SMX: C:\Users\HP\Desktop\DMC\AlRajhi\py-projects\excel_files\SMX_RRA.xlsx

## RULE ENGINE ##  
RULES_SMX:
  RULE_2:
    COND: >
      rule == 'HIGH DATE'
    SELECT_STATEMENT: >
        "CAST ('9999-12-31 00:00:00' AS TIMESTAMP(0) ) AS " + tgt_column
    RULE_TYPE: HIGH_DATE   
```
where different keys refer to the following
### SQL_TEMPLATES
Defines path to where templates reside. The main class makes use of these templates and dynamically interpolates strings.

### LOGGERINFO 
Path to logging root directory. The logger generates log folders on the basis of date and log files with in them.

### PD_PATH
Project directory path and nested paths to operational files:

#### SQL
Path to directory where the automated scripts will be generated.

#### JOBS
Path to excel file containing jobs to run information. The main class executes based on the input ingested from this file. [RAW FILE](https://github.com/Datamaticsconsulting/arb-smx-to-sql/blob/main/excel_files/jobs_execution.xlsx)

#### JOBS_STATUS
An additional file is maintained by the program to update each job's status along with any failures if any. This is the path to job's status excel file. [RAW FILE](https://github.com/Datamaticsconsulting/arb-smx-to-sql/blob/main/excel_files/jobs_execution_status.xlsx)

#### SMX
Path to Excel SMX file to read all the mappings from.

### RULES_SMX
The core of the program lies here and all the rules are configured from here. Existing rules can be modified and new rules can be added. The main class makes use of these rule to transform SMX to BTEQ logic.

#### COND
The condition in Rules column of SMX.

#### SELECT_STATEMENT
Transformation of the Rule to BTEQ logic.

#### RULE_TYPE
A key to distinguish all RULES.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.
