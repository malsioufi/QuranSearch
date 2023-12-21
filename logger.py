# logger.py
import logging


def configure_logger():
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Configure a file handler for the logger
    file_handler = logging.FileHandler('app_log.log')
    file_handler.setLevel(logging.ERROR)  # Log only errors to the file
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    return logger
