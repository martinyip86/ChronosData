import logging
import sys
import os
from logging.handlers import RotatingFileHandler

def setup_logger(name="swiss-quant",log_file=None):
    """
    Standardized Logging Factory.
    Configures a unified logging format for both stdout and rotating files, 
    ensuring observability across distributed quant modules.
    """
    # 1. Initialize Logger Object
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)# # Default to INFO for production-level verbosity

    # 2. Define Log Format (Timestamp | Level | Module | File:Line | Message)
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Prevent duplicate handlers if the logger is already initialized (e.g., in Airflow/Multiprocessing)
    if logger.handlers:
        has_file_handler = any(isinstance(h,logging.FileHandler) for h in logger.handlers)
        if log_file and not has_file_handler:
            _add_file_handler(logger, log_file, formatter)
        return logger

    # 3. Configure Console Output (Stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 4. Configure File Output (Optional)
    if log_file:
        _add_file_handler(logger, log_file, formatter)

    return logger

def _add_file_handler(logger, log_file, formatter):
    """
    Internal helper to attach a Rotating File Handler.
    Implements log rotation to prevent disk exhaustion.
    """
    try:
        # Ensure log directory exists
        os.makedirs(os.path.dirname(log_file),exist_ok=True)

        # Log Rotation: 10MB per file, keeping 5 historical backups.
        file_handler = RotatingFileHandler(log_file,mode='a',maxBytes=10*1024*1024,backupCount=5,encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        # Fallback to console print if file system is read-only or permission denied
        print(f"🚨 [FATAL] Failed to set up file logging: {e}")
    