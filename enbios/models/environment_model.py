from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    CONFIG_FILE: Optional[str] = None
    RUN_SCENARIOS: Optional[list[str]] = None
