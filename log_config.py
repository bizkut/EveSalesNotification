import logging
import os
import sys

def setup_logging():
    """
    Configures logging for the application based on the LOG_LEVEL environment variable.
    This function is designed to be idempotent, so it can be called from multiple
    entry points without causing duplicate log handlers.
    """
    log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Get the root logger
    logger = logging.getLogger()

    # Only configure handlers if they haven't been configured already.
    # This prevents duplicate log messages if this function is called more than once.
    if not logger.handlers:
        logger.setLevel(log_level)

        # Create a new stream handler that writes to stdout
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Add the new handler to the root logger
        logger.addHandler(handler)

        logging.info(f"Root logger configured with level: {log_level_str}")
    else:
        logging.debug(f"Logger already configured. Current level: {logging.getLevelName(logger.level)}. Requested level: {log_level_str}")
        # Optionally, you could still adjust the level if a higher level is requested.
        # For simplicity, we'll stick to configuring only once.
        pass