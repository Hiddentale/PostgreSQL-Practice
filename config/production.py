# Production environment settings

def get_settings() -> "DataBaseSettings":
    from . import DataBaseSettings
    return DataBaseSettings(HOST="localhost", NAME="prod_db", USERNAME="prod_user", PASSWORD="prod_password")