from typing import List, Optional
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select
from sqlalchemy.sql import text


class QueryBuilder:

    def __init__(self):
        self.table = None
        self.columns: Optional[List] = None
        self.where_string = None
        self.in_join = None
        self.l_join = None
        self.r_join = None
        self.fo_join = None
        self.crss_join = None

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

        if self.in_join:
            in_join_str = f"INNER JOIN {self.in_join[0]} ON {self.in_join[1]}"
            sql_string.append(in_join_str)

        if self.l_join:
            l_join_str = f"LEFT JOIN {self.l_join[0]} ON {self.l_join[1]}"
            sql_string.append(l_join_str)
        
        if self.r_join:
            r_join_str = f"RIGHT JOIN {self.r_join[0]} ON {self.r_join[1]}"
            sql_string.append(r_join_str)

        if self.fo_join:
            fo_join_str = f"FULL OUTER JOIN {self.fo_join[0]} ON {self.fo_join[1]}"
            sql_string.append(fo_join_str)

        if self.crss_join:
            crss_join_str = f"CROSS JOIN {self.crss_join}"
            sql_string.append(crss_join_str)

        return "\n".join(sql_string)

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
    def cross_join(self, crss_join: str):
        self.crss_join = crss_join
        return self

    def full_outer_join(self, fo_join_1: str, fo_join_2: str):
        self.fo_join = (fo_join_1, fo_join_2)
        return self

    def inner_join(self, in_join_1: str, in_join_2: str):
        self.in_join = (in_join_1, in_join_2)
        return self

    def join(self):
        return self

    def left_join(self, l_join_1: str, l_join_2: str):
        self.l_join = (l_join_1, l_join_2)
        return self

    def right_join(self, r_join_1: str, r_join_2: str):
        self.r_join = (r_join_1, r_join_2)
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
