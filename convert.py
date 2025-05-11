# convert.py
import argparse
import json
import sys
import os
import logging

# Add etl_lib to Python path if it's not installed as a package
# This ensures the script can find the etl_lib modules when run from the project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from etl_lib import readers
from etl_lib import transformers
from etl_lib import writers

# Setup basic logging
# Configure logging to show timestamp, level, and message
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)] # Ensure logs go to stdout
)

def load_template(template_path):
    """Loads the JSON template file."""
    try:
        # Open template file with UTF-8 encoding for broader compatibility
        with open(template_path, 'r', encoding='utf-8') as f:
            template = json.load(f)
        logging.info(f"Template '{template_path}' loaded successfully.")
        return template
    except FileNotFoundError:
        logging.error(f"Error: Template file not found at {template_path}")
        raise # Re-raise the exception to be caught by the main error handler
    except json.JSONDecodeError as e:
        logging.error(f"Error: Invalid JSON in template file {template_path}. Details: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading the template '{template_path}': {e}")
        raise

def main():
    """Main function to orchestrate the ETL process."""
    parser = argparse.ArgumentParser(description="ETL utility to convert and transform data based on a JSON template.")
    parser.add_argument('--template', required=True, help="Path to the JSON template file defining the ETL process.")
    parser.add_argument('--input', required=True, help="Path to the input data file.")
    parser.add_argument('--output', required=True, help="Path for the output data file. The directory will be created if it doesn't exist.")
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the logging level (default: INFO).'
    )

    args = parser.parse_args()

    # Set logging level from command line argument
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        logging.error(f'Invalid log level: {args.log_level}')
        sys.exit(1)
    logging.getLogger().setLevel(numeric_level)

    logging.info("--- Starting ETL Process ---")
    logging.debug(f"Arguments received: Template='{args.template}', Input='{args.input}', Output='{args.output}', LogLevel='{args.log_level}'")

    try:
        # 1. Load Template
        logging.info(f"Loading template from: {args.template}")
        template = load_template(args.template)

        # Basic template validation
        if 'input' not in template or 'output' not in template:
            logging.error("Template is invalid: must contain 'input' and 'output' sections.")
            sys.exit(1)
        logging.debug("Template structure validated (input/output sections exist).")

        input_options = template.get('input', {})
        transform_options = template.get('transformations', {}) # Optional
        output_options = template.get('output', {})

        # 2. Extract: Read data
        logging.info(f"Reading data from '{args.input}' using type '{input_options.get('file_type', 'N/A')}'...")
        data_df = readers.read_data(args.input, input_options)
        logging.info(f"Successfully read {len(data_df)} rows and {len(data_df.columns)} columns from input.")
        if data_df.empty:
            logging.warning("Input data is empty. Subsequent steps might produce empty output.")
        logging.debug(f"Input DataFrame head:\n{data_df.head().to_string()}")


        # 3. Transform: Apply transformations
        if transform_options:
            logging.info("Applying transformations as defined in the template...")
            transformed_df = transformers.transform_data(data_df, transform_options)
            logging.info(f"Transformations applied. Transformed data has {len(transformed_df)} rows and {len(transformed_df.columns)} columns.")
            if transformed_df.empty and not data_df.empty:
                 logging.warning("Data became empty after transformations (e.g., due to filters). Check filter logic if this is unintended.")
        else:
            logging.info("No transformations specified in the template. Using data as is from input.")
            transformed_df = data_df # Pass data through if no transformations
        logging.debug(f"Transformed DataFrame head:\n{transformed_df.head().to_string()}")


        # 4. Load: Write data
        # Ensure output directory exists before attempting to write the file
        output_dir = os.path.dirname(args.output)
        if output_dir and not os.path.exists(output_dir): # Check if output_dir is not empty (e.g. for output in current dir)
            try:
                os.makedirs(output_dir)
                logging.info(f"Created output directory: {output_dir}")
            except OSError as e:
                logging.error(f"Could not create output directory '{output_dir}': {e}")
                sys.exit(1)

        logging.info(f"Writing transformed data to '{args.output}' using type '{output_options.get('file_type', 'N/A')}'...")
        writers.write_data(transformed_df, args.output, output_options)
        logging.info(f"ETL process completed successfully. Output written to: {args.output}")

    except FileNotFoundError as e:
        logging.error(f"File operation error: {e}")
        sys.exit(1)
    except ValueError as e: # Catch specific errors like unsupported file types or bad data
        logging.error(f"Data validation or processing error: {e}")
        sys.exit(1)
    except KeyError as e: # Catch errors from missing keys in template or data
        logging.error(f"Configuration error: Missing key {e} in template or data structure.")
        sys.exit(1)
    except Exception as e: # Catch-all for any other unexpected errors
        logging.error(f"An unexpected error occurred during the ETL process: {e}", exc_info=True) # exc_info=True logs stack trace
        sys.exit(1)
    finally:
        logging.info("--- ETL Process Ended ---")

if __name__ == '__main__':
    main()
