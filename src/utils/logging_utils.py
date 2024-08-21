import logging
import os
from src.utils.spark_utils import load_config
from datetime import datetime


def setup_logging(level=logging.INFO):
    """Sets up logging configuration for the application."""
    # Define the format for logging
    config = load_config()
    logging_format = '%(asctime)s - %(levelname)s - %(message)s'

    log_directory = config["logging"]["path"]
    os.makedirs(log_directory, exist_ok=True)

    date_path = datetime.now().strftime("%Y%m%d")
    # Define the log file path
    log_file_path = os.path.join(log_directory, f'application_{date_path}.log')

    # Configure the logging
    logging.basicConfig(
        level=level,
        format=logging_format,
        handlers=[
            logging.FileHandler(log_file_path),  # Outputs logs to a file
            logging.StreamHandler()  # Outputs logs to the console
        ]
    )


def get_logger(name):
    """Returns a logger with the specified name."""
    return logging.getLogger(name)
