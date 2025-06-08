# Test environment settings

def get_settings() -> "DataBaseSettings":
    from . import DataBaseSettings
    return DataBaseSettings(HOST="localhost", NAME="test_db", USERNAME="test_user", PASSWORD="test_password")