# Python ETL Utilities

This project provides a flexible, template-driven ETL (Extract, Transform, Load) utility script written in Python. It allows users to define ETL processes using JSON configuration files, specifying input sources, transformations, and output destinations.

## Project Structure

```
etl_utilities/
├── convert.py                 # Main script to run ETL jobs
├── etl_lib/                   # Core library for ETL operations
│   ├── __init__.py
│   ├── readers.py             # Modules for reading various file formats (CSV, JSON)
│   ├── transformers.py        # Modules for data transformation logic
│   └── writers.py             # Modules for writing various file formats (CSV, JSON)
├── templates/                 # Directory for ETL job configuration templates
│   └── retail_csv_to_json.json # Example template: CSV input to JSON output
├── sample_data/               # Directory for sample input data
│   └── 2025-05-05.csv         # Example input CSV file
├── output_data/               # Default directory for output files (gitignored)
├── .gitignore                 # Specifies intentionally untracked files that Git should ignore
└── README.md                  # This file: Project overview and instructions
```

## Features

* **Template-Driven:** Define ETL jobs using simple JSON configuration files.
* **Flexible Input/Output:**
    * Currently supports CSV and JSON file formats.
    * Easily extensible for other formats (e.g., Excel, Parquet, databases).
* **Data Transformations:**
    * Column mapping/renaming.
    * Data type conversion.
    * Row filtering based on conditions.
    * Creation of new columns based on expressions or concatenations.
* **Modular Design:** Separated logic for reading, transforming, and writing data.
* **Command-Line Interface:** Easy to run ETL jobs from the terminal.

## Prerequisites

* Python 3.7+
* pip (Python package installer)
* Git (for version control, optional for just running the script)

## Setup

1. **Clone the repository (if obtained from Git):**
   ```bash
   git clone <repository_url>
   cd etl_utilities
   ```

2. **Install dependencies:**
   This project primarily uses pandas for data manipulation.
   ```bash
   pip install pandas
   ```
   It's highly recommended to use a virtual environment:
   ```bash
   python -m venv venv
   # On Windows
   .\venv\Scripts\activate
   # On macOS/Linux
   source venv/bin/activate
   pip install pandas
   ```

## Usage

The main script for running ETL jobs is convert.py.

**Command:**

```bash
python convert.py --template <path_to_template_json> --input <path_to_input_file> --output <path_for_output_file> [--log-level <LEVEL>]
```

**Arguments:**

* `--template`: (Required) Path to the JSON template file that defines the ETL process.
* `--input`: (Required) Path to the input data file.
* `--output`: (Required) Path for the output data file. The script will create the output directory if it doesn't exist.
* `--log-level`: (Optional) Set the logging level. Choices: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default is INFO.

**Example:**

To run the provided sample ETL job:

```bash
python convert.py --template templates/retail_csv_to_json.json --input sample_data/2025-05-05.csv --output output_data/monthly_sales.json
```

This will:
1. Read data from sample_data/2025-05-05.csv.
2. Apply transformations defined in templates/retail_csv_to_json.json.
3. Write the transformed data to output_data/monthly_sales.json.

## JSON Template Structure

The JSON template file defines the three main stages of the ETL process:

1. **"input"**: Specifies the source data.
   * `"file_type"`: (e.g., "csv", "json")
   * Other options specific to the file type (e.g., "delimiter", "encoding", "has_header" for CSV; "orient" for JSON).
   * `"columns"`: (Optional for CSV with header, required for CSV without header if names are needed) An array of expected column names. If has_header is true and columns are provided, CSV columns will be renamed.

2. **"transformations"**: (Optional) Defines a sequence of data transformations.
   * `"column_mappings"`: Array of {"from": "old_name", "to": "new_name"} objects.
   * `"filters"`: Array of filter conditions, e.g., {"column": "age", "condition": ">", "value": 30}.
   * `"new_columns"`: Array of definitions for new columns, e.g., {"name": "full_name", "operation": "concat", "sources": ["first", "last"], "separator": " "} or {"name": "discounted_price", "operation": "eval", "expression": "price * 0.9"}.
   * `"data_type_conversions"`: Object mapping column names to target types, e.g., {"age": "int", "price": "float"}.

3. **"output"**: Specifies the destination for the transformed data.
   * `"file_type"`: (e.g., "csv", "json")
   * Other options specific to the file type (e.g., "delimiter", "encoding", "include_header" for CSV; "orient", "indent" for JSON).
   * `"columns"`: (Optional) An array specifying which columns to include in the output and their order.

Refer to the example templates/retail_csv_to_json.json for a practical implementation.

## Extending the Utility

* **New File Formats:**
    * Add new functions to etl_lib/readers.py and etl_lib/writers.py.
    * Update the read_data and write_data dispatcher functions.
* **New Transformations:**
    * Add new transformation functions to etl_lib/transformers.py.
    * Update the transform_data function to include them in the processing pipeline.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs, feature requests, or improvements.
Ensure code is well-commented and, if possible, include or update relevant examples or templates.

## License

This project is open-source. (Consider adding a specific license like MIT if you intend to share it widely).
