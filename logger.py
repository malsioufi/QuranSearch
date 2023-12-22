# logger.py
import logging


class LoggerSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LoggerSingleton, cls).__new__(cls)
            cls._instance._configure_logger()
        return cls._instance

    def _configure_logger(self):
        # Set up logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # Configure a file handler for the logger
        file_handler = logging.FileHandler("app_log.log")
        file_handler.setLevel(logging.ERROR)  # Log only errors to the file
        file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)


# Create an instance of the LoggerSingleton
logger_singleton = LoggerSingleton()
logger = logger_singleton.logger
