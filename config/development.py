# Development environment settings
from . import DataBaseSettings


def get_settings() -> "DataBaseSettings":
    return DataBaseSettings(HOST="localhost", NAME="dev_db", USERNAME="dev_user", PASSWORD="dev_password")