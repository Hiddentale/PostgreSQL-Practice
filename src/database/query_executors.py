from typing import List, Optional
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select
from sqlalchemy.sql import text


class QueryBuilder:

    def __init__(self):
        self.table = None
        self.columns: Optional[List] = None
        self.where_string = None

    def __str__(self):
        if not self.table:
            raise ValueError("Table must be specified")
        
        sql_string = []
        if self.columns:
            if len(self.columns) == 1:
                columns_str = self.columns[0]
            else:
                columns_str = ",".join(self.columns)
        else:
            columns_str = "*"
        sql_string.append(f"SELECT {columns_str}")

        sql_string.append(f"FROM {self.table}")

        if self.where_string:
            where_str_str = ",".join(self.where_string)
            sql_string.append(f"WHERE {where_str_str}")
        
        print(sql_string)

        return "\n".join(sql_string)
    
        # Proper SQL Query format here

    # # ______________________________Core Query Operations________________________________
    def count(self):
        return self

    def delete(self):
        return self

    def distinct(self):
        return self

    def exists(self):
        return self

    def first(self):
        return self

    def insert(self):
        return self

    def select(self, *columns):
        self.columns = columns
        return self

    def update(self):
        return self

    # ______________________________Joins________________________________
    def cross_join(self):
        return self

    def full_outer_join(self):
        return self

    def inner_join(self):
        return self

    def join(self):
        return self

    def left_join(self):
        return self

    def right_join(self):
        return self
# ______________________________Query Structure________________________________
    def from_table(self, table: str): # "Explicitely disallow comma notation and table functions for security.
        self.table = table
        return self
    
    def group_by(self):
        return self

    def having(self):
        return self   

    def limit(self):
        return self

    def offset(self):
        return self

    def order_by(self):
        return self

# ______________________________Set operation________________________________
    def _except(self):
        return self
    
    def intersect(self):
        return self        

    def union(self):
        return self

# ______________________________Advanced Features________________________________
    def on_conflict(self):
        return self

    def returning(self):
        return self

    def with_cte(self):
        return self

# ______________________________Utility/Execution________________________________
    def execute(self):
        return self

    def get_params(self):
        return self

    def get_sql(self):
        return self

# ______________________________Where Conditions________________________________
    def and_where(self):
        return self

    def case(self):
        return self

    def or_where(self):
        return self

    def where(self, *where):
        self.where_string = where
        return self
