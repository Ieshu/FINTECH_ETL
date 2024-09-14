import logging
import os

def setup_logging(log_file_path, log_level=logging.INFO):
    """Set up logging to file and console."""
    # Create logger
    logger = logging.getLogger(__name__)

    # Avoid adding duplicate handlers if logger is called multiple times
    if not logger.hasHandlers():
        # Set log level
        logger.setLevel(log_level)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        # Create file handler that logs to the specified file
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(log_level)

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)

        # Create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
