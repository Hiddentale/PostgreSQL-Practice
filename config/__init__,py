import os
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from . import development, production, test


class DataBaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_", case_sensitive=True, frozen=True, env_file=".env")

    host: str = Field(..., alias="HOST")
    database: str = Field(..., alias="NAME")
    user: str = Field(..., alias="USERNAME")
    password: SecretStr = Field(..., alias="PASSWORD")
    port: int = Field(default=5432, alias="PORT")
    min_connections: int = Field(default=1, alias="MIN_CONNECTIONS")
    max_connections: int = Field(default=10, alias="MAX_CONNECTIONS")
    
    @classmethod
    def get_environment(cls) -> str:
        return os.environ.get("DB_ENVIRONMENT", default="development")
    
    @classmethod
    def get_config(cls) -> "DataBaseSettings":
        environment = DataBaseSettings.get_environment()
        if environment == "production":
            return production.get_settings()
        elif environment == "development":
            return development.get_settings()
        else:
            return test.get_settings()
        
__all__ = ['DataBaseSettings']