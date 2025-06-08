import logging

import psycopg2
import psycopg2.pool
from dotenv import load_dotenv
from stamina import retry

from config import DataBaseSettings
from .exceptions import ConnectionError, ConfigurationError, OutOfResourcesError, DatabaseError, AdminInterventionError

load_dotenv() 
logger = logging.getLogger(__name__)
logging.basicConfig(filename='example.log', encoding='utf-8', 
                    level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


class Singleton(type):
    """
    A metaclass that ensures only one instance of a class exists.
    """
    def __init__(self, *args, **kwargs):
        self.__instance = None
        super().__init__(*args, **kwargs)
    
    def __call__(self, *args, **kwargs):
        if self.__instance is None:
            self.__instance = super().__call__(*args, **kwargs)
        return self.__instance


# Look at Python libraries like pybreaker or circuitbreaker.
class PostgreSQLConnectionPool(metaclass=Singleton):
    """
    Manages a pool of PostgreSQL database connections as a singleton.
    
    This class provides efficient reuse of database connections through connection pooling.
    It implements the context manager protocol for clean resource management.
    
    Example:
        with PostgreSQLConnectionPool() as pool:
            # Use the connection pool
    """
    def __init__(self):
        self.connection_pool = None
        # Todo:
        # self.checkout_times = {}  # Track when connections were checked out
        # self.connection_ages = {}  # Track when connections were created
        # self.wait_times = []      # Recent wait times for connection acquisition
        # self.error_counts = {}    # Errors by connection

    def __enter__(self):
        database_config = DataBaseSettings.get_config()
        connection_parameters = {
        "host": database_config.host,
        "database": database_config.database,
        "user": database_config.user,
        "password": database_config.password.get_secret_value(),
        }
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(database_config.min_connections, 
                                                             database_config.max_connections, **connection_parameters)
            logger.info("Connection pool was succesfully created.")
            return self.connection_pool
        except psycopg2.Error as postgres_error:
            custom_error = DatabaseError.from_postgres_exception(postgres_error)
            raise custom_error from postgres_error

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection_pool is not None:
            self.connection_pool.close()
    
    @retry(on=psycopg2.OperationalError, attempts=5, timeout=30.0, wait_initial=0.1, wait_max=5.0)
    def get_valid_connection(self):
        """
        Get a connection from the pool and ensure it's valid, using
        stamina to make sure retrying does not clog up network traffic.
    
        If the connection fails validation, it's discarded and a new one 
        is created to replace it.
    
        Returns:
            A valid database connection
        """
        connection = None
        if self.connection_pool is not None:
            logger.info("Acquiring connection from connection pool.")
            try:
                connection = self.connection_pool.getconn()
                if self.is_connection_alive(connection):
                    logger.info("Connection acquired.")
                    return connection
                else:
                    self.connection_pool.putconn(connection, close=True)
                    logger.info("Chosen connection was no longer active, retrying.")
            except psycopg2.Error as postgres_error:
                custom_error = DatabaseError.from_postgres_exception(postgres_error)
                raise custom_error from postgres_error
        else:
            raise ConfigurationError("Connection pool is missing.")
        
    def is_connection_alive(self, connection):
        """
        Verify if a database connection is still active and usable.
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except psycopg2.Error as postgres_error:
            custom_error = DatabaseError.from_postgres_exception(postgres_error)
            raise custom_error from postgres_error


class PooledDatabaseConnection:
    """
    Manages a single connection obtained from a connection pool.
    
    Example:
        with PostgreSQLConnectionPool() as pool:
            with PooledDatabaseConnection(pool) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM users")
    """
    def __init__(self, connection_pool):
        self.connection = None
        self.connection_pool = connection_pool

    def __enter__(self):
        try:
            self.connection = self.connection_pool.get_valid_connection()
            return self.connection
        except ConnectionError:
            logger.warning("Failed to acquire connection from connection pool.")
            raise
        except OutOfResourcesError:
            logger.warning("Database is out of resources. Please try again later.")
            # Maybe add implementation here for retry after a while?
            raise
        except AdminInterventionError:
            logger.warning("Admin has intervened.")
            raise
        except ConfigurationError:
            logger.warning("Connection pool is missing")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection is not None:
            self.connection_pool.putconn(self.connection)
