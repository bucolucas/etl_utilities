# etl_lib/__init__.py
# This file makes the etl_lib directory a Python package.

# Import key functions/classes for easier access if desired, e.g.:
# from .readers import read_data
# from .writers import write_data
# from .transformers import transform_data

# Or, allow modules to be imported directly:
# import etl_lib.readers
# import etl_lib.writers
# import etl_lib.transformers

# For now, keep it simple. Users will do rom etl_lib import readers etc.
import logging

# Configure a basic logger for the library if no handlers are configured by the application
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logger.addHandler(logging.NullHandler()) # Avoid "No handler found" warnings by default
