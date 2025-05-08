# Test environment settings
from . import DataBaseSettings


def get_settings() -> "DataBaseSettings":
    return DataBaseSettings(HOST="localhost", NAME="test_db", USERNAME="test_user", PASSWORD="test_password")