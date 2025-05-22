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

    # # ______________________________Core Query Operations________________________________
    def count(self):
        pass

    def delete(self):
        pass

    def distinct(self):
        pass

    def exists(self):
        pass

    def first(self):
        pass

    def insert(self):
        pass

    def select(self, *columns):
        self.columns = columns

    def update(self):
        pass

    # ______________________________Joins________________________________
    def cross_join(self):
        pass

    def full_outer_join(self):
        pass

    def inner_join(self):
        pass

    def join(self):
        pass

    def left_join(self):
        pass

    def right_join(self):
        pass
# ______________________________Query Structure________________________________
    def from_table(self, table: str): # "Explicitely disallow comma notation and table functions for security.
        self.table = table
    
    def group_by(self):
        pass

    def having(self):
        pass   

    def limit(self):
        pass

    def offset(self):
        pass

    def order_by(self):
        pass

# ______________________________Set operation________________________________
    def _except(self):
        pass
    
    def intersect(self):
        pass        

    def union(self):
        pass

# ______________________________Advanced Features________________________________
    def on_conflict(self):
        pass

    def returning(self):
        pass

    def with_cte(self):
        pass

# ______________________________Utility/Execution________________________________
    def execute(self):
        pass

    def get_params(self):
        pass

    def get_sql(self):
        pass

# ______________________________Where Conditions________________________________
    def and_where(self):
        pass

    def case(self):
        pass

    def or_where(self):
        pass

    def where(self):
        pass


# possible use case
query = QueryBuilder().select("name", "email").from_table("users").where("active = %s", [True])