import psycopg2
import polars as pl
from src.database import PostgreSQLConnectionPool, PooledDatabaseConnection

with PostgreSQLConnectionPool() as pool:
    with PooledDatabaseConnection(pool) as conn:
        tables = pl.read_database(
            query="""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            """,
            connection=conn
        )
        print("Available tables:")
        print(tables)
