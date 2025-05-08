import psycopg2
import logging
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from config import DataBaseSettings

load_dotenv() 
logger = logging.getLogger(__name__)

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