import psycopg2
import polars as pl
from src.database import PostgreSQLConnectionPool, PooledDatabaseConnection, QueryBuilder

with PostgreSQLConnectionPool() as pool:
    with PooledDatabaseConnection(pool) as conn:
        query = QueryBuilder().select("table_name").from_table("information_schema.tables").where("table_schema = 'public'")
        
        tables = pl.read_database(
            query=query,
            connection=conn
        )
        print("Available tables:")
        print(tables)