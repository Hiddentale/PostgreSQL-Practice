# Production environment settings
from . import DataBaseSettings


def get_settings() -> "DataBaseSettings":
    return DataBaseSettings(HOST="localhost", NAME="prod_db", USERNAME="prod_user", PASSWORD="prod_password")