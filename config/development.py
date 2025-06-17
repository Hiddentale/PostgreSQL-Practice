# Development environment settings

def get_settings() -> "DataBaseSettings":
    from .settings import DataBaseSettings
    return DataBaseSettings(HOST="localhost", NAME="postgres", USERNAME="postgres", PASSWORD="mF4Z2gm&f")