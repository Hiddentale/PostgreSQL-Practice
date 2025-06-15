from typing import List, Optional


class QueryBuilder:

    def __init__(self):
        self.table = None
        self.columns: Optional[List] = None
        self.where_string = None
        self.joins = []
        self.grp_by = []
        self.hving = []
        self._count = None

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
            columns_str = ""
            if self._count:
                columns_str.join(f"COUNT({self._count})")
            else:
                columns_str = "*"
        sql_string.append(f"SELECT {columns_str}")

        sql_string.append(f"FROM {self.table}")

        if self.where_string:
            where_str_str = ",".join(self.where_string) # Bug here, make sure to fix!
            sql_string.append(f"WHERE {where_str_str}")

        for join in self.joins:
            sql_string.append(join)

        if self.grp_by:
            grp_by_string = []
            if len(self.grp_by) == 1:
                grp_by_string = self.grp_by[0]
            else:
                grp_by_string = ", ".join(self.grp_by)
            sql_string.append(f"GROUP BY {grp_by_string}")

            if self.hving:
                hving_string = []
                if len(self.hving) == 1:
                    hving_string = self.hving[0]
                else:
                    hving_string = " AND ".join(self.hving)
                sql_string.append(f"HAVING {hving_string}")

        return "\n".join(sql_string)

    # # ______________________________Core Query Operations________________________________
    def count(self, column=None):
        if column is None:
            self._count = "*"
        else:
            self._count = column
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
    def cross_join(self, table_name: str):
        self.joins.append(f"CROSS JOIN {table_name}")
        return self

    def full_outer_join(self, table_name: str, join_condition: str):
        self.joins.append(f"FULL OUTER JOIN {table_name} ON {join_condition}")
        return self

    def inner_join(self, table_name: str, join_condition: str):
        self.joins.append(f"INNER JOIN {table_name} ON {join_condition}")
        return self

    def join(self, table_name: str, join_condition: str):
        self.joins.append(f"JOIN {table_name} ON {join_condition}")
        return self

    def left_join(self, table_name: str, join_condition: str):
        self.joins.append(f"LEFT JOIN {table_name} ON {join_condition}")
        return self

    def right_join(self, table_name: str, join_condition: str):
        self.joins.append(f"RIGHT JOIN {table_name} ON {join_condition}")
        return self
# ______________________________Query Structure________________________________
    def from_table(self, table: str): # "Explicitely disallow comma notation and table functions for security.
        self.table = table
        return self
    
    def group_by(self, *grp_by):
        self.grp_by = grp_by
        return self

    def having(self, *hving):
        self.hving = hving
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
