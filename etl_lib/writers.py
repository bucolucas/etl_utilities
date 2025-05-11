# etl_lib/writers.py
import pandas as pd
import os
import json # For JSON specific handling if needed beyond pandas
import logging # Import logging

# Get a logger for this module
logger = logging.getLogger(__name__)

def write_csv(df, file_path, output_options):
    """
    Writes a pandas DataFrame to a CSV file based on provided options.
    """
    logger.debug(f"Attempting to write CSV: {file_path} with options: {output_options}")
    try:
        delimiter = output_options.get('delimiter', ',')
        encoding = output_options.get('encoding', 'utf-8')
        include_index = output_options.get('include_index', False)
        include_header = output_options.get('include_header', True)
        # Optional: specify which columns to write and their order
        columns_to_write_order = output_options.get('columns')

        df_to_write = df
        if columns_to_write_order:
            # Filter out any columns specified in template that are not in the DataFrame
            actual_columns_to_write = [col for col in columns_to_write_order if col in df.columns]
            missing_template_cols = [col for col in columns_to_write_order if col not in df.columns]
            if missing_template_cols:
                logger.warning(f"Output columns specified in template but not found in data: {missing_template_cols}. They will be omitted from output.")
            if not actual_columns_to_write:
                logger.error(f"No specified output columns found in the data for CSV '{file_path}'. Writing empty file or all columns if behavior desired.")
                # Decide behavior: write empty, write all, or raise error. For now, let pandas handle it (writes all if columns=[]).
                # To write all if actual_columns_to_write is empty:
                # if not actual_columns_to_write: df_to_write = df
                # else: df_to_write = df[actual_columns_to_write]
            df_to_write = df[actual_columns_to_write]


        df_to_write.to_csv(
            file_path,
            sep=delimiter,
            encoding=encoding,
            index=include_index,
            header=include_header
        )
        logger.info(f"Data successfully written to CSV: {file_path} ({len(df_to_write)} rows)")
    except Exception as e:
        logger.error(f"Error writing CSV file '{file_path}': {e}", exc_info=True)
        raise Exception(f"Error writing CSV file {file_path}: {e}")

def write_json(df, file_path, output_options):
    """
    Writes a pandas DataFrame to a JSON file based on provided options.
    """
    logger.debug(f"Attempting to write JSON: {file_path} with options: {output_options}")
    try:
        orient = output_options.get('orient', 'records')
        encoding = output_options.get('encoding', 'utf-8') # Though to_json uses it for file writing, not internal string encoding
        indent_level = output_options.get('indent') # None for compact, integer for pretty print (e.g., 2)
        json_lines = output_options.get('lines', False) # For JSON Lines format (ndjson)
        # Optional: specify which columns to write. Pandas to_json doesn't directly take a columns list for selection.
        # So, we subset the DataFrame *before* calling to_json.
        columns_to_write_order = output_options.get('columns')

        df_to_write = df
        if columns_to_write_order:
            actual_columns_to_write = [col for col in columns_to_write_order if col in df.columns]
            missing_template_cols = [col for col in columns_to_write_order if col not in df.columns]
            if missing_template_cols:
                logger.warning(f"Output columns specified in template but not found in data: {missing_template_cols}. They will be omitted from output.")
            if not actual_columns_to_write and columns_to_write_order: # only log error if columns were specified but none found
                 logger.error(f"None of the specified output columns {columns_to_write_order} found in the data for JSON '{file_path}'. Writing all columns or empty if df is empty.")
                 # if actual_columns_to_write is empty, df_to_write remains the original df, so all columns are written
            elif actual_columns_to_write:
                 df_to_write = df[actual_columns_to_write]


        # Pandas to_json handles file writing directly.
        # default_handler=str is useful for types like datetime that are not directly JSON serializable by default.
        df_to_write.to_json(
            file_path,
            orient=orient,
            indent=indent_level,
            lines=json_lines,
            force_ascii=False, # Allows non-ASCII characters (e.g., UTF-8) directly
            default_handler=str, # Converts non-serializable objects (like datetime) to strings
            date_format='iso' # Common format for dates, or specify 'epoch' for milliseconds
        )
        logger.info(f"Data successfully written to JSON: {file_path} ({len(df_to_write)} records)")
    except Exception as e:
        logger.error(f"Error writing JSON file '{file_path}': {e}", exc_info=True)
        raise Exception(f"Error writing JSON file {file_path}: {e}")


def write_data(df, file_path, output_options):
    """
    Generic data writer dispatcher. Determines the file type from options and calls the appropriate writer.
    """
    file_type = output_options.get('file_type', '').lower()
    logger.info(f"Dispatching write operation for file type: '{file_type}' to path: '{file_path}'")

    if not file_type:
        logger.error("Output 'file_type' not specified in template.")
        raise ValueError("Output 'file_type' must be specified in the template's output section.")

    # Directory creation is handled in convert.py's main function before this call.

    if file_type == 'csv':
        write_csv(df, file_path, output_options)
    elif file_type == 'json':
        write_json(df, file_path, output_options)
    # Future: Add other writers like write_parquet, write_excel etc.
    # elif file_type == 'excel':
    #     write_excel(df, file_path, output_options) # Placeholder
    else:
        logger.error(f"Unsupported output file type: {file_type}")
        raise ValueError(f"Unsupported output file type: '{file_type}'. Supported types: 'csv', 'json'.")
