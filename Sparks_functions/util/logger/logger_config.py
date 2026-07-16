# logger_config.py
import logging
from pathlib import Path
import sys
import warnings

_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logging(
    log_dir: str = "logs",
    log_file: str = "sparks.log",
    verbosity: str = "INFO",
    capture_warnings: bool = True
) -> logging.Logger:

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logfile_path = Path(log_dir) / log_file

    file_handler = logging.FileHandler(logfile_path, mode="w")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "[%(asctime)s] %(name)s %(levelname)-8s %(message)s", datefmt=_TIME_FORMAT
    )
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(verbosity)
    console_formatter = logging.Formatter("%(levelname)-8s %(message)s")
    console_handler.setFormatter(console_formatter)

    # your package logger
    logger = logging.getLogger("sparks")
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = True  # don't bubble up

    if capture_warnings:
        logging.captureWarnings(True)
        warnings_logger = logging.getLogger("py.warnings")
        warnings_logger.setLevel(logging.WARNING)
        warnings_logger.addHandler(console_handler)

    logger.info(f"Logging initialized. Log file: {logfile_path.resolve()}")
    return logger
