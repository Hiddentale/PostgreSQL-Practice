# Query execution utilities
# Represents SQL queries as objects instead of strings, 
# this improves maintainability, type safety, and reduces SQL injection risks.
# Look at SQLAlchemy Core or Python's pypika for examples.
from typing import List, Optional


class QueryBuilder:

    def __init__(self):
        self.table: None
        self.columns: Optional[List] = None

    def __str__(self):
        if not self.table:
            raise ValueError("Table must be specified")
        
        sql_string = f"SELECT {self.columns} FROM {self.table}"
        if self.columns:
            pass
        pass
        # Proper SQL Query format here

    def select(self, *columns):
        self.columns = columns
    
    def from_table(self, table: str): # "Explicitely disallow comma notation and table functions for security.
        self.table = table

    def where():
        pass


# possible use case
query = QueryBuilder().select("name", "email").from_table("users").where("active = %s", [True])