#import psycopg2
import logging
import os
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv() 
logger = logging.getLogger(__name__)

try:
    from config import DataBaseSettings
    print("Successfully imported DataBaseSettings")
except ImportError as e:
    print(f"Import error: {e}")

database_config = DataBaseSettings.get_config()

connection_parameters = {
    "host": database_config.host,
    "database": database_config.database,
    "user": database_config.user,
    "password": database_config.password.get_secret_value()
}

connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, **connection_parameters)

connection = connection_pool.getconn()

connection.close()