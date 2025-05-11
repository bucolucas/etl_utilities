# etl_lib/transformers.py
import pandas as pd
import logging # Import logging

# Get a logger for this module
logger = logging.getLogger(__name__)

def map_columns(df, column_mappings):
    """
    Renames columns based on 'from'-'to' mappings.
    Only columns present in the 'from' field of mappings and existing in the DataFrame are processed.
    The resulting DataFrame will only contain the successfully renamed 'to' columns.
    """
    if not column_mappings:
        logger.debug("No column mappings provided. Returning DataFrame as is.")
        return df

    logger.debug(f"Applying column mappings: {column_mappings}")
    rename_map = {}
    # final_columns_order = [] # To maintain order as specified in mappings

    for mapping_entry in column_mappings:
        if not isinstance(mapping_entry, dict) or 'from' not in mapping_entry or 'to' not in mapping_entry:
            logger.warning(f"Invalid column mapping entry: {mapping_entry}. Must be a dict with 'from' and 'to' keys. Skipping.")
            continue

        old_name = mapping_entry['from']
        new_name = mapping_entry['to']

        if old_name in df.columns:
            if old_name != new_name: # Only add to rename_map if names are different
                 rename_map[old_name] = new_name
            # final_columns_order.append(new_name) # Add the 'to' name
        else:
            logger.warning(f"Column '{old_name}' specified in mappings not found in DataFrame. It will be skipped.")

    # Columns to select are the 'from' names that were found in the DataFrame
    columns_to_select_original = [m['from'] for m in column_mappings if m['from'] in df.columns]

    if not columns_to_select_original:
        logger.warning("No columns to map were found in the DataFrame based on provided mappings. Returning original DataFrame.")
        return df

    # Select only the columns that are part of the mapping (original names)
    mapped_df = df[columns_to_select_original].copy() # Use .copy() to avoid SettingWithCopyWarning

    # Rename these selected columns
    mapped_df.rename(columns=rename_map, inplace=True)

    logger.info(f"Columns mapped. Resulting columns: {list(mapped_df.columns)}")
    return mapped_df


def convert_data_types(df, type_conversions):
    """
    Converts column data types as specified.
    Example: {"column_name": "int", "other_column": "datetime"}
    Supported types: 'int', 'float', 'str', 'datetime', 'bool'.
    Handles potential errors during conversion by logging and continuing.
    """
    if not type_conversions:
        logger.debug("No data type conversions provided.")
        return df

    logger.debug(f"Applying data type conversions: {type_conversions}")
    for column, new_type in type_conversions.items():
        if column in df.columns:
            try:
                original_dtype = df[column].dtype
                logger.debug(f"Converting column '{column}' from {original_dtype} to '{new_type}'")
                if new_type == 'int':
                    # pd.to_numeric can create floats if NaNs are present, then convert to Int64 (nullable int)
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                elif new_type == 'float':
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
                elif new_type == 'str':
                    df[column] = df[column].astype(str)
                elif new_type == 'datetime':
                    # errors='coerce' will turn unparseable dates into NaT (Not a Time)
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                elif new_type == 'bool':
                    # More robust boolean conversion
                    # Handles common string representations and numeric ones.
                    # NaNs will become False with .astype(bool) after mapping, handle explicitly if needed
                    map_to_bool = {'true': True, 'True': True, 'TRUE': True, '1': True, 1: True,
                                   'false': False, 'False': False, 'FALSE': False, '0': False, 0: False}
                    # Apply mapping, then convert to boolean. Unmapped values become NaN, then False by astype(bool).
                    df[column] = df[column].map(map_to_bool)
                    # If you want to preserve NaNs for bools, use pandas' Nullable Boolean type:
                    # df[column] = df[column].astype('boolean') # This keeps pd.NA
                    df[column] = df[column].astype(bool) # This converts NaNs to False
                else:
                    logger.warning(f"Unsupported data type '{new_type}' for column '{column}'. Skipping conversion.")
            except Exception as e:
                logger.warning(f"Could not convert column '{column}' to type '{new_type}'. Error: {e}. Column remains {df[column].dtype}.")
        else:
            logger.warning(f"Column '{column}' for data type conversion not found in DataFrame. Skipping.")
    return df

def filter_rows(df, filters):
    """
    Filters rows based on a list of filter conditions.
    Each filter is a dict: {"column": "name", "condition": "op", "value": val}
    Supported conditions: '>', '<', '>=', '<=', '==', '!=', 'isin', 'notin', 'isnull', 'notnull', 'contains', 'startswith', 'endswith'.
    """
    if not filters:
        logger.debug("No filters provided.")
        return df

    logger.debug(f"Applying row filters: {filters}")
    filtered_df = df.copy() # Work on a copy to avoid modifying original df during iteration
    for f_idx, f in enumerate(filters):
        column = f.get('column')
        condition = f.get('condition')
        value = f.get('value') # Value might not be needed for isnull/notnull

        logger.debug(f"Applying filter #{f_idx+1}: Column='{column}', Condition='{condition}', Value='{value}'")

        if column not in filtered_df.columns:
            logger.warning(f"Column '{column}' for filtering not found. Skipping this filter.")
            continue

        try:
            series = filtered_df[column]
            if condition == '>':
                filtered_df = filtered_df[series > value]
            elif condition == '<':
                filtered_df = filtered_df[series < value]
            elif condition == '>=':
                filtered_df = filtered_df[series >= value]
            elif condition == '<=':
                filtered_df = filtered_df[series <= value]
            elif condition == '==':
                filtered_df = filtered_df[series == value]
            elif condition == '!=':
                filtered_df = filtered_df[series != value]
            elif condition == 'isin':
                if not isinstance(value, list):
                    logger.warning(f"'isin' filter for column '{column}' requires a list value. Got {type(value)}. Skipping.")
                    continue
                filtered_df = filtered_df[series.isin(value)]
            elif condition == 'notin':
                if not isinstance(value, list):
                    logger.warning(f"'notin' filter for column '{column}' requires a list value. Got {type(value)}. Skipping.")
                    continue
                filtered_df = filtered_df[~series.isin(value)]
            elif condition == 'isnull':
                filtered_df = filtered_df[series.isnull()]
            elif condition == 'notnull':
                filtered_df = filtered_df[series.notnull()]
            # String specific conditions - ensure column is string type or handle errors
            elif condition in ['contains', 'startswith', 'endswith']:
                if not pd.api.types.is_string_dtype(series) and not pd.api.types.is_object_dtype(series):
                     logger.warning(f"String condition '{condition}' on non-string column '{column}' (dtype: {series.dtype}). Attempting conversion to string.")
                     series = series.astype(str) # Attempt conversion

                if condition == 'contains':
                    filtered_df = filtered_df[series.str.contains(str(value), na=False)] # na=False treats NaN as not containing
                elif condition == 'startswith':
                    filtered_df = filtered_df[series.str.startswith(str(value), na=False)]
                elif condition == 'endswith':
                    filtered_df = filtered_df[series.str.endswith(str(value), na=False)]
            else:
                logger.warning(f"Unsupported filter condition '{condition}' for column '{column}'. Skipping.")
        except Exception as e:
            logger.warning(f"Error applying filter on column '{column}' with condition '{condition}': {e}. Skipping this filter.")
    logger.info(f"Filtering complete. {len(df) - len(filtered_df)} rows removed.")
    return filtered_df

def add_new_columns(df, new_column_definitions):
    """
    Adds new columns to the DataFrame based on definitions.
    Operations: 'concat', 'eval', 'copy', 'default_value'.
    """
    if not new_column_definitions:
        logger.debug("No new column definitions provided.")
        return df

    logger.debug(f"Adding new columns based on definitions: {new_column_definitions}")
    for col_def in new_column_definitions:
        name = col_def.get('name')
        operation = col_def.get('operation')
        if not name or not operation:
            logger.warning(f"Skipping new column definition due to missing 'name' or 'operation': {col_def}")
            continue

        logger.debug(f"Creating new column '{name}' using operation '{operation}'")
        try:
            if operation == 'concat':
                sources = col_def.get('sources', [])
                separator = col_def.get('separator', '')
                missing_sources = [s for s in sources if s not in df.columns]
                if missing_sources:
                    logger.warning(f"Cannot create column '{name}' by concat: Missing source columns {missing_sources}. Skipping.")
                    continue
                # Ensure all source columns are string type before concatenation
                df[name] = df[sources].astype(str).agg(separator.join, axis=1)
            elif operation == 'eval': # Use pandas eval for simple arithmetic or string operations
                expression = col_def.get('expression')
                if not expression:
                    logger.warning(f"Cannot create column '{name}' by eval: 'expression' not provided. Skipping.")
                    continue
                # df.eval() operates in-place on the DataFrame's context
                # For safety and clarity, assign to new column
                df[name] = df.eval(expression, engine='python') # 'python' engine is more flexible
            elif operation == 'copy':
                source_col = col_def.get('source_column')
                if not source_col:
                    logger.warning(f"Cannot create column '{name}' by copy: 'source_column' not provided. Skipping.")
                    continue
                if source_col in df.columns:
                    df[name] = df[source_col]
                else:
                    logger.warning(f"Cannot create column '{name}' by copy: Source column '{source_col}' not found. Skipping.")
            elif operation == 'default_value':
                value = col_def.get('value') # value can be None, so check presence with 'in'
                if 'value' not in col_def:
                    logger.warning(f"Cannot create column '{name}' with default_value: 'value' not provided. Skipping.")
                    continue
                df[name] = value # This will broadcast the value to all rows
            else:
                logger.warning(f"Unsupported new column operation '{operation}' for column '{name}'. Skipping.")
        except Exception as e:
            logger.warning(f"Error creating new column '{name}' with operation '{operation}': {e}. Skipping this column.")
    return df


def transform_data(df, transform_options):
    """
    Applies a sequence of transformations to the DataFrame.
    The order of operations is: column_mappings, filters, new_columns, data_type_conversions.
    """
    if not transform_options:
        logger.info("No transformation options provided. Returning original DataFrame.")
        return df

    logger.info("Starting data transformation process...")
    current_df = df.copy() # Work on a copy

    # 1. Map/select columns first
    if 'column_mappings' in transform_options:
        logger.info("Step 1: Applying column mappings...")
        current_df = map_columns(current_df, transform_options['column_mappings'])
        logger.debug(f"DataFrame shape after column mapping: {current_df.shape}")

    # 2. Filter rows
    if 'filters' in transform_options:
        logger.info("Step 2: Applying row filters...")
        current_df = filter_rows(current_df, transform_options['filters'])
        logger.debug(f"DataFrame shape after filtering: {current_df.shape}")

    # 3. Add new columns
    if 'new_columns' in transform_options:
        logger.info("Step 3: Adding new columns...")
        current_df = add_new_columns(current_df, transform_options['new_columns'])
        logger.debug(f"DataFrame shape after adding new columns: {current_df.shape}")

    # 4. Convert data types (often best done after new columns are created and filters applied)
    if 'data_type_conversions' in transform_options:
        logger.info("Step 4: Applying data type conversions...")
        current_df = convert_data_types(current_df, transform_options['data_type_conversions'])
        logger.debug(f"DataFrame dtypes after type conversion:\n{current_df.dtypes}")

    logger.info("Data transformation process completed.")
    return current_df
