from typing import List, Optional


class QueryBuilder:

    def __init__(self):
        self._table = None
        self._where = None
        self._count = None
        self._limit = None
        self._offset = None

        self._and_where = []
        self._or_where = []
        self._joins = []
        self._group_by = []
        self._having = []
        self._order_by = []
        self._insert = []

        self._columns: Optional[List] = None

        self._distinct = False

    def __str__(self):
        if not self._table:
            raise ValueError("Table must be specified")
        

        
        sql_string = []
        if self._insert:
            template = self._insert[0]
            intermediary_string = ", ".join(template)
            appending_string = "(" + intermediary_string + ")"
            sql_string.append(f"INSERT INTO {self._table} {appending_string}")
            sql_string.append(f"VALUES")
            

        if self._columns:
            if self._distinct:
                columns_str = ",".join(self._columns)
                sql_string.append(f"SELECT DISTINCT {columns_str}")
            else:
                columns_str = ",".join(self._columns)
                sql_string.append(f"SELECT {columns_str}")
        elif self._count:
            columns_str = f"COUNT({self._count})"
            sql_string.append(f"SELECT {columns_str}")
        else:
            columns_str = "*"
            sql_string.append(f"SELECT {columns_str}")

        sql_string.append(f"FROM {self._table}")

        if self._where:
            sql_string.append(self.where_statement())

        for join in self._joins:
            sql_string.append(join)

        if self._group_by:
            group_by_string = ", ".join(self._group_by)
            sql_string.append(f"GROUP BY {group_by_string}")

            if self._having:
                having_string = " AND ".join(self._having)
                sql_string.append(f"HAVING {having_string}")
        
        if self._order_by:
            order_by_string = ", ".join(self._order_by)
            sql_string.append(f"ORDER BY {order_by_string}")

        if self._limit:
            sql_string.append(f"LIMIT {self._limit}")
            if self._offset:
                sql_string.append(f"OFFSET {self._offset}")

        return "\n".join(sql_string)
    
    def where_statement(self):
        where_string = " AND ".join(self._where)
        if self._and_where:
            and_where_string = " AND ".join(self._and_where)
            where_string += " AND " + and_where_string
        if self._or_where:
            or_where_string = " OR ".join(self._or_where)
            where_string += " OR " + or_where_string
        return f"WHERE {where_string}"

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
        self._distinct = True
        return self

    def exists(self):
        return self

    def first(self):
        return self

    def insert(self, table, *items):
        self._table = table
        self._insert = items
        return self

    def select(self, *columns):
        self._columns = columns
        return self

    def update(self):
        return self

    # ______________________________Joins________________________________
    def cross_join(self, table_name: str):
        self._joins.append(f"CROSS JOIN {table_name}")
        return self

    def full_outer_join(self, table_name: str, join_condition: str):
        self._joins.append(f"FULL OUTER JOIN {table_name} ON {join_condition}")
        return self

    def inner_join(self, table_name: str, join_condition: str):
        self._joins.append(f"INNER JOIN {table_name} ON {join_condition}")
        return self

    def join(self, table_name: str, join_condition: str):
        self._joins.append(f"JOIN {table_name} ON {join_condition}")
        return self

    def left_join(self, table_name: str, join_condition: str):
        self._joins.append(f"LEFT JOIN {table_name} ON {join_condition}")
        return self

    def right_join(self, table_name: str, join_condition: str):
        self._joins.append(f"RIGHT JOIN {table_name} ON {join_condition}")
        return self
# ______________________________Query Structure________________________________
    def from_table(self, table: str): # "Explicitely disallow comma notation and table functions for security.
        self._table = table
        return self
    
    def group_by(self, *group_by):
        self._group_by = group_by
        return self

    def having(self, *having):
        self._having = having
        return self   

    def limit(self, limit):
        self._limit = limit
        return self

    def offset(self, offset):
        self._offset = offset
        return self

    def order_by(self, *order_by):
        self._order_by = order_by
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
    def and_where(self, *and_where):
        for item in and_where:
            self._and_where.append(item)
        return self

    def or_where(self, *or_where):
        for item in or_where:
            self._or_where.append(item)
        return self
    
    def case(self):
        return self

    def where(self, *where):
        self._where = where
        return self
