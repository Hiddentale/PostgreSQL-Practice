import os
import logging

from psycopg2 import pool, OperationalError
from dotenv import load_dotenv

from config import DataBaseSettings

load_dotenv() 
logger = logging.getLogger(__name__)
logging.basicConfig(filename='example.log', encoding='utf-8', 
                    level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


class Singleton(type):
    def __init__(self, *args, **kwargs):
        self.instance = None
        super().__init__(*args, **kwargs)
    
    def __call__(self, *args, **kwargs):
        if self.__instance is None:
            self.__instance = super().__call__(*args, **kwargs)
        else:
            return self.__instance


class ConnectionPool(metaclass=Singleton): # Needs better name
    def __init__(self):
        self.connection_pool = None

    def __enter__(self):
        database_config = DataBaseSettings.get_config()
        connection_parameters = {
        "host": database_config.host,
        "database": database_config.database,
        "user": database_config.user,
        "password": database_config.password.get_secret_value(),
        }
        try:
            self.connection_pool = pool.SimpleConnectionPool(database_config.min_connections, 
                                                             database_config.max_connections, **connection_parameters)
            return self.connection_pool
        except OperationalError as error:
            logging.warning(f"Failed to create a connection pool: {error}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection_pool is not None:
            self.connection_pool.close()


class Connection: # Needs better name
    def __init__(self, connection_pool):
        self.connection = None
        self.connection_pool = connection_pool

    def __enter__(self):
        try:
            self.connection = self.connection_pool.getconn()
            return self.connection
        except Exception:
            logging.warning("Failed to acquire connection from connection pool.")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection_pool.putconn(self.connection)