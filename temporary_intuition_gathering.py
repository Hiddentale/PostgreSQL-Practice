import psycopg2
import polars as pl
from src.database import PostgreSQLConnectionPool, PooledDatabaseConnection, QueryBuilder

with PostgreSQLConnectionPool() as pool:
    with PooledDatabaseConnection(pool) as conn:
        query = QueryBuilder().select("table_name").from_table("information_schema.tables").where("table_schema = 'public'")
        #query = QueryBuilder().select("name", "email").from_table("users").where("active = %s", [True])
        
        tables = pl.read_database(
            query=str(query),
            connection=conn
        )
        print("Available tables:")
        print(tables)
