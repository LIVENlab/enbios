import logging.config
import json
from pathlib import Path

import appdirs

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
        "level": "DEBUG",
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
        cls._config_file = Path(appdirs.user_config_dir("enbios2")) / "logging.json"
        # check if  exists
        if not cls.log_config_file.exists():
            # if not, create it
            cls.log_config_file.parent.mkdir(parents=True, exist_ok=True)
            cls.log_config_file.write_text(json.dumps(default_log_config, ensure_ascii=False, indent=2),
                                           encoding="utf-8")
        cls.config_data = json.loads(cls.log_config_file.read_text(encoding="utf-8"))

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


def get_logger(name: str) -> logging.Logger:
    return EnbiosLogger.get_or_create_logger(name)
