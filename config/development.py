# Development environment settings
from settings import DataBaseSettings


def get_settings() -> "DataBaseSettings":
    return DataBaseSettings(HOST="localhost", NAME="northwind", USERNAME="postgres", PASSWORD="mF4Z2gm&f")