import logging
import os

# Ensure logs directory exists
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../logs"))
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger("custom_logger")
logger.setLevel(logging.DEBUG)

def setup_logger(task):
    """Setup logger with a file based on the task argument."""
    log_filename = "Data_processed.txt" if task == "process_data" else "Data_processed_all.txt"
    log_file = os.path.join(log_dir, log_filename)

    if not logger.hasHandlers():
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)

        # Formatter to include time, level, and message
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # Add handler to logger
        logger.addHandler(file_handler)

# Function to log informational messages
def log_info(message):
    logger.info(message)

# Function to log error messages
def log_error(message):
    logger.error(message)
