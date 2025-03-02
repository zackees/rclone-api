import logging
import sys

_INITIALISED = False


def setup_default_logging():
    """Set up default logging configuration if none exists."""
    global _INITIALISED
    if _INITIALISED:
        return
    if not logging.root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(sys.stdout),
                # Uncomment to add file logging
                # logging.FileHandler('rclone_api.log')
            ],
        )


def configure_logging(level=logging.INFO, log_file=None):
    """Configure logging for the rclone_api package.

    Args:
        level: The logging level (default: logging.INFO)
        log_file: Optional path to a log file
    """
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,  # Override any existing configuration
    )


# Call setup_default_logging when this module is imported
setup_default_logging()
