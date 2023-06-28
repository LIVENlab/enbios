import logging.config
import json
import os
from pathlib import Path

import appdirs

from enbios2.const import PROJECT_PATH

default_log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "raw": {
            "format": "%(message)s"
        },
        "simple": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        }
    },
    "loggers": {
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"]
    }
}

new_logger_default_config = {
    "level": "DEBUG",
    "handlers": ["console"],
    "propagate": False
}


class EnbiosLogger:
    initialized = False
    log_config_file: Path = None
    config_data: dict = None

    @classmethod
    def init_logger(cls):
        cls.log_config_file = Path(appdirs.user_config_dir("enbios2")) / "logging.json"
        # check if  exists
        if not cls.log_config_file.exists():
            print(f"Creating logging config file at: {cls.log_config_file}")
            # if not, create it
            cls.log_config_file.parent.mkdir(parents=True, exist_ok=True)
            cls.log_config_file.write_text(json.dumps(default_log_config, ensure_ascii=False, indent=2),
                                           encoding="utf-8")

        cls.reload_config()

    @classmethod
    def reload_config(cls):
        cls.config_data = json.loads(cls.log_config_file.read_text(encoding="utf-8"))
        logging.config.dictConfig(cls.config_data)

    @classmethod
    def add_logger(cls, name):
        cls.config_data["loggers"][name] = new_logger_default_config
        # write to file
        cls.log_config_file.write_text(json.dumps(cls.config_data, ensure_ascii=False, indent=2), encoding="utf-8")

    @classmethod
    def get_or_create_logger(cls, name: str) -> logging.Logger:
        if not cls.initialized:
            cls.init_logger()
        if name not in cls.config_data["loggers"]:
            cls.add_logger(name)
            logging.config.dictConfig(cls.config_data)
        return logging.getLogger(name)


def get_module_name(file_path: str) -> str:
    """Get the module name based on the file's location in the project."""
    relative_path = os.path.relpath(file_path, PROJECT_PATH)
    module_name = os.path.splitext(relative_path)[0]
    module_name = module_name.replace(os.sep, '.')
    return module_name


def get_logger(file_path: str) -> logging.Logger:
    """
    Get a logger for the given module, based on its file path.
    use like this:
    Takes the logging config from logging.json.
    Creates a new entry in that file if it does not exist yet with 'new_logger_default_config'.
    logger = get_logger(__file__)
    :param file_path:  absolute file path
    :return: logger for the module...
    """
    return EnbiosLogger.get_or_create_logger(get_module_name(file_path))
