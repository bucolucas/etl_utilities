# etl_lib/readers.py
import csv
import json
import pandas as pd
import logging # Import logging

# Get a logger for this module
logger = logging.getLogger(__name__)

def read_csv(file_path, input_options):
    """
    Reads a CSV file into a pandas DataFrame based on provided options.
    """
    logger.debug(f"Attempting to read CSV: {file_path} with options: {input_options}")
    try:
        delimiter = input_options.get('delimiter', ',')
        encoding = input_options.get('encoding', 'utf-8')
        skip_header_rows = input_options.get('skip_header', 0) # Number of rows to skip at the beginning
        # 'columns' in template means user *expects* these column names after reading.
        # 'has_header' in template indicates if the CSV *file* has a header row.
        template_columns = input_options.get('columns')
        csv_has_header = input_options.get('has_header', True)

        # Determine pandas header parameter and names parameter
        if csv_has_header:
            # CSV has a header. If template_columns are also given, they will be used to rename.
            header_param = 'infer' # Pandas reads the first line as header
            names_param = None
        else:
            # CSV does not have a header.
            header_param = None # No header row in CSV
            names_param = template_columns # Use template_columns as column names if provided

        df = pd.read_csv(
            file_path,
            delimiter=delimiter,
            encoding=encoding,
            skiprows=skip_header_rows,
            header=header_param,
            names=names_param
        )
        logger.info(f"Successfully read {len(df)} rows from CSV: {file_path}")

        # If CSV had a header and template_columns are provided, rename CSV columns to match template_columns
        if csv_has_header and template_columns:
            if len(df.columns) == len(template_columns):
                logger.debug(f"Renaming columns from {list(df.columns)} to {template_columns}")
                df.columns = template_columns
            else:
                logger.warning(
                    f"CSV file '{file_path}' has {len(df.columns)} columns, but template specifies {len(template_columns)} columns. "
                    f"Column renaming based on template 'columns' list will be skipped. Using header from CSV."
                )

        # Validate if specified columns exist (after potential renaming)
        if input_options.get('validate_columns') and template_columns:
            missing_cols = [col for col in template_columns if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing expected columns in CSV after processing: {missing_cols}")
                raise ValueError(f"Missing expected columns in CSV after processing: {missing_cols}")
        return df
    except FileNotFoundError:
        logger.error(f"Input CSV file not found: {file_path}")
        raise
    except pd.errors.EmptyDataError:
        logger.warning(f"Input CSV file is empty: {file_path}")
        return pd.DataFrame() # Return empty DataFrame
    except Exception as e:
        logger.error(f"Error reading CSV file '{file_path}': {e}", exc_info=True)
        raise Exception(f"Error reading CSV file {file_path}: {e}")


def read_json(file_path, input_options):
    """
    Reads a JSON file into a pandas DataFrame based on provided options.
    """
    logger.debug(f"Attempting to read JSON: {file_path} with options: {input_options}")
    try:
        orient = input_options.get('orient', 'records')
        encoding = input_options.get('encoding', 'utf-8')
        lines = input_options.get('lines', False) # For JSON Lines format (ndjson)

        df = pd.read_json(file_path, orient=orient, encoding=encoding, lines=lines)
        logger.info(f"Successfully read {len(df)} records from JSON: {file_path}")
        return df
    except FileNotFoundError:
        logger.error(f"Input JSON file not found: {file_path}")
        raise
    except ValueError as e: # Catches JSON decoding errors and other value errors from pandas
        logger.error(f"Error reading JSON file '{file_path}': Invalid JSON format or structure. Details: {e}", exc_info=True)
        raise ValueError(f"Error reading JSON file {file_path}: Invalid JSON format or structure. {e}")
    except Exception as e:
        logger.error(f"Error reading JSON file '{file_path}': {e}", exc_info=True)
        raise Exception(f"Error reading JSON file {file_path}: {e}")

def read_data(file_path, input_options):
    """
    Generic data reader dispatcher. Determines the file type from options and calls the appropriate reader.
    """
    file_type = input_options.get('file_type', '').lower()
    logger.info(f"Dispatching read operation for file type: '{file_type}'")

    if not file_type:
        logger.error("Input 'file_type' not specified in template.")
        raise ValueError("Input 'file_type' must be specified in the template's input section.")

    if file_type == 'csv':
        return read_csv(file_path, input_options)
    elif file_type == 'json':
        return read_json(file_path, input_options)
    # Future: Add other file types like 'excel', 'parquet'
    # elif file_type == 'excel':
    #     return read_excel(file_path, input_options) # Placeholder for Excel reader
    else:
        logger.error(f"Unsupported input file type: {file_type}")
        raise ValueError(f"Unsupported input file type: '{file_type}'. Supported types: 'csv', 'json'.")
