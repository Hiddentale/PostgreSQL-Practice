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
        # Proper SQL Query format here
        pass

    def select(self, columns):
        self.columns = columns
    
    def from_table(self, table):
        pass

    def where():
        pass


# possible template
query = QueryBuilder().select("name", "email").from_table("users").where("active = %s", [True])