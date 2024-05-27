import logging
from logging import getLogger


logger = getLogger("test-logger")
logger.addHandler(logging.StreamHandler())

