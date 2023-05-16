from typing import Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    huggingface_token: Optional[str] = None

    class Config:
        # `.env.prod` takes priority over `.env`
        env_file = '.env', 'template.env'


settings = Settings()
