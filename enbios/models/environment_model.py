from typing import Optional

from pydantic.v1 import BaseSettings


class Settings(BaseSettings):
    CONFIG_FILE: Optional[str] = None
    RUN_SCENARIOS: Optional[list[str]] = None
