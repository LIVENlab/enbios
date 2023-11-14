import logging
from logging import getLogger

import pytest

from enbios.const import BASE_DATA_PATH

logger = getLogger("test-logger")
logger.addHandler(logging.StreamHandler())

