import pytest
from src.database.query_executors import QueryBuilder

class TestQueryBuilderBasics:
    """Test basic QueryBuilder functionality."""
    
    @pytest.mark.unit
    def test_query_builder_instantiation(self):
        builder = QueryBuilder()
        
        assert builder.table is None
        assert builder.columns is None  
        assert builder.where_string is None

    @pytest.mark.unit
    def test_fluent_interface_returns_self(self):
        builder = QueryBuilder()
        
        assert builder.select("name") is builder
        assert builder.from_table("users") is builder
        assert builder.where("id = 1") is builder

    @pytest.mark.unit
    def test_method_chaining(self):
        builder = QueryBuilder()
        
        result = builder.select("name", "email").from_table("users").where("active = true")
        
        assert result is builder
        assert builder.table == "users"
        assert builder.columns == ("name", "email")
        assert builder.where_string == ("active = true",)


class TestQueryBuilderSelect:
    
    @pytest.mark.unit
    def test_select_single_column(self):
        builder = QueryBuilder()
        builder.select("name")
        
        assert builder.columns == ("name",)

    @pytest.mark.unit
    def test_select_multiple_columns(self):
        builder = QueryBuilder()
        builder.select("name", "email", "created_at")
        
        assert builder.columns == ("name", "email", "created_at")

    @pytest.mark.unit
    def test_select_no_columns_defaults_to_star(self):
        builder = QueryBuilder()
        builder.from_table("users")
        
        query_str = str(builder)
        
        assert "SELECT *" in query_str

    @pytest.mark.unit
    def test_select_overrides_previous_selection(self):
        """Test that calling select again overrides previous column selection."""
        builder = QueryBuilder()
        builder.select("name")
        builder.select("email", "age")
        
        assert builder.columns == ("email", "age")


class TestQueryBuilderFromTable:
    
    @pytest.mark.unit
    def test_from_table_sets_table(self):
        """Test that from_table sets the table name."""
        builder = QueryBuilder()
        builder.from_table("users")
        
        assert builder.table == "users"

    @pytest.mark.unit
    def test_from_table_overrides_previous_table(self):
        """Test that calling from_table again overrides previous table."""
        builder = QueryBuilder()
        builder.from_table("users")
        builder.from_table("orders")
        
        assert builder.table == "orders"


class TestQueryBuilderWhere:
    
    @pytest.mark.unit
    def test_where_single_condition(self):
        """Test adding a single WHERE condition."""
        builder = QueryBuilder()
        builder.where("id = 1")
        
        assert builder.where_string == ("id = 1",)

    @pytest.mark.unit
    def test_where_multiple_conditions(self):
        """Test adding multiple WHERE conditions."""
        builder = QueryBuilder()
        builder.where("id = 1", "active = true")
        
        assert builder.where_string == ("id = 1", "active = true")

    @pytest.mark.unit
    def test_where_overrides_previous_conditions(self):
        """Test that calling where again overrides previous conditions."""
        builder = QueryBuilder()
        builder.where("id = 1")
        builder.where("name = 'John'", "active = true")
        
        assert builder.where_string == ("name = 'John'", "active = true")


class TestQueryBuilderStringGeneration:
    """Test SQL string generation from QueryBuilder."""
    
    @pytest.mark.unit
    def test_simple_select_all_query(self):
        """Test generating a simple SELECT * query."""
        builder = QueryBuilder()
        builder.from_table("users")
        
        query_str = str(builder)
        
        assert "SELECT *" in query_str
        assert "FROM users" in query_str

    @pytest.mark.unit
    def test_select_specific_columns_query(self):
        """Test generating a query with specific columns."""
        builder = QueryBuilder()
        builder.select("name", "email").from_table("users")
        
        query_str = str(builder)
        
        assert "SELECT name,email" in query_str
        assert "FROM users" in query_str

    @pytest.mark.unit
    def test_select_single_column_query(self):
        """Test generating a query with a single column."""
        builder = QueryBuilder()
        builder.select("name").from_table("users")
        
        query_str = str(builder)
        
        assert "SELECT name" in query_str
        assert "FROM users" in query_str

    @pytest.mark.unit
    def test_query_with_where_clause(self):
        """Test generating a query with WHERE clause."""
        builder = QueryBuilder()
        builder.select("name").from_table("users").where("id = 1")
        
        query_str = str(builder)
        
        assert "SELECT name" in query_str
        assert "FROM users" in query_str
        assert "WHERE id = 1" in query_str

    @pytest.mark.unit
    def test_str_without_table_raises_error(self):
        """Test that calling str() without setting table raises ValueError."""
        builder = QueryBuilder()
        builder.select("name")
        
        with pytest.raises(ValueError, match="Table must be specified"):
            str(builder)


class TestQueryBuilderJoins:
    """Test all JOIN operations and their SQL generation."""
    
    @pytest.mark.unit
    def test_inner_join_basic(self):
        """Test basic INNER JOIN functionality."""
        builder = QueryBuilder()
        builder.select("u.name", "o.total").from_table("users u").inner_join("orders o", "u.id = o.user_id")
        
        query_str = str(builder)
        assert "INNER JOIN orders o ON u.id = o.user_id" in query_str
        assert "FROM users u" in query_str

    @pytest.mark.unit
    def test_left_join_with_multiple_conditions(self):
        """Test LEFT JOIN with multiple ON conditions."""
        builder = QueryBuilder()
        builder.select("*").from_table("users u").left_join("profiles p", "u.id = p.user_id AND p.active = true")
        
        query_str = str(builder)
        assert "LEFT JOIN profiles p ON u.id = p.user_id AND p.active = true" in query_str

    @pytest.mark.unit
    def test_right_join(self):
        """Test RIGHT JOIN functionality."""
        builder = QueryBuilder()
        builder.select("*").from_table("orders o").right_join("users u", "o.user_id = u.id")
        
        query_str = str(builder)
        assert "RIGHT JOIN users u ON o.user_id = u.id" in query_str

    @pytest.mark.unit
    def test_full_outer_join(self):
        """Test FULL OUTER JOIN functionality."""
        builder = QueryBuilder()
        builder.select("*").from_table("table_a a").full_outer_join("table_b b", "a.id = b.a_id")
        
        query_str = str(builder)
        assert "FULL OUTER JOIN table_b b ON a.id = b.a_id" in query_str

    @pytest.mark.unit
    def test_cross_join(self):
        """Test CROSS JOIN functionality."""
        builder = QueryBuilder()
        builder.select("*").from_table("colors").cross_join("sizes")
        
        query_str = str(builder)
        assert "CROSS JOIN sizes" in query_str

    @pytest.mark.unit
    def test_multiple_joins_chaining(self):
        """Test chaining multiple different types of joins."""
        builder = QueryBuilder()
        result = (builder.select("*")
                 .from_table("users u")
                 .inner_join("orders o", "u.id = o.user_id")
                 .left_join("order_items oi", "o.id = oi.order_id")
                 .inner_join("products p", "oi.product_id = p.id"))
        
        assert result is builder
        query_str = str(builder)
        assert "INNER JOIN orders o ON u.id = o.user_id" in query_str
        assert "LEFT JOIN order_items oi ON o.id = oi.order_id" in query_str
        assert "INNER JOIN products p ON oi.product_id = p.id" in query_str

    @pytest.mark.unit
    def test_join_generic_method(self):
        """Test the generic join() method that should default to INNER JOIN."""
        builder = QueryBuilder()
        builder.select("*").from_table("users u").join("orders o", "u.id = o.user_id")
        
        query_str = str(builder)
        assert "JOIN orders o ON u.id = o.user_id" in query_str


class TestQueryBuilderAggregation:
    """Test aggregation and grouping functionality."""
    
    @pytest.mark.unit
    def test_group_by_single_column(self):
        """Test GROUP BY with single column."""
        builder = QueryBuilder()
        builder.select("department", "COUNT(*)").from_table("employees").group_by("department")
        
        query_str = str(builder)
        assert "GROUP BY department" in query_str

    @pytest.mark.unit
    def test_group_by_multiple_columns(self):
        """Test GROUP BY with multiple columns."""
        builder = QueryBuilder()
        builder.select("department", "location", "COUNT(*)").from_table("employees").group_by("department", "location")
        
        query_str = str(builder)
        assert "GROUP BY department, location" in query_str

    @pytest.mark.unit
    def test_having_clause(self):
        """Test HAVING clause with GROUP BY."""
        builder = QueryBuilder()
        builder.select("department", "COUNT(*)").from_table("employees").group_by("department").having("COUNT(*) > 5")
        
        query_str = str(builder)
        assert "GROUP BY department" in query_str
        assert "HAVING COUNT(*) > 5" in query_str

    @pytest.mark.unit
    def test_having_multiple_conditions(self):
        """Test HAVING with multiple conditions."""
        builder = QueryBuilder()
        builder.select("*").from_table("orders").group_by("customer_id").having("SUM(total) > 1000", "COUNT(*) > 3")
        
        query_str = str(builder)
        assert "HAVING SUM(total) > 1000 AND COUNT(*) > 3" in query_str

    @pytest.mark.unit
    def test_count_method(self):
        """Test count() method for counting records."""
        builder = QueryBuilder()
        builder.count().from_table("users").where("active = true")
        
        query_str = str(builder)
        assert "SELECT COUNT(*)" in query_str
        assert "FROM users" in query_str
        assert "WHERE active = true" in query_str

    @pytest.mark.unit
    def test_count_with_specific_column(self):
        """Test count() with specific column."""
        builder = QueryBuilder()
        builder.count("email").from_table("users")
        
        query_str = str(builder)
        assert "SELECT COUNT(email)" in query_str

    @pytest.mark.unit
    def test_distinct_method(self):
        """Test DISTINCT functionality."""
        builder = QueryBuilder()
        builder.select("department").distinct().from_table("employees")
        
        query_str = str(builder)
        assert "SELECT DISTINCT department" in query_str


class TestQueryBuilderOrdering:
    """Test ORDER BY and LIMIT/OFFSET functionality."""
    
    @pytest.mark.unit
    def test_order_by_single_column_asc(self):
        """Test ORDER BY with single column ascending."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").order_by("name")
        
        query_str = str(builder)
        assert "ORDER BY name" in query_str

    @pytest.mark.unit
    def test_order_by_single_column_desc(self):
        """Test ORDER BY with single column descending."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").order_by("name DESC")
        
        query_str = str(builder)
        assert "ORDER BY name DESC" in query_str

    @pytest.mark.unit
    def test_order_by_multiple_columns(self):
        """Test ORDER BY with multiple columns."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").order_by("department ASC", "name DESC", "created_at")
        
        query_str = str(builder)
        assert "ORDER BY department ASC, name DESC, created_at" in query_str

    @pytest.mark.unit
    def test_limit_only(self):
        """Test LIMIT without OFFSET."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").limit(10)
        
        query_str = str(builder)
        assert "LIMIT 10" in query_str

    @pytest.mark.unit
    def test_offset_with_limit(self):
        """Test OFFSET with LIMIT."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").limit(10).offset(20)
        
        query_str = str(builder)
        assert "LIMIT 10" in query_str
        assert "OFFSET 20" in query_str

    @pytest.mark.unit
    def test_order_limit_offset_combination(self):
        """Test ORDER BY, LIMIT, and OFFSET together."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").order_by("created_at DESC").limit(5).offset(10)
        
        query_str = str(builder)
        assert "ORDER BY created_at DESC" in query_str
        assert "LIMIT 5" in query_str
        assert "OFFSET 10" in query_str


class TestQueryBuilderWhereConditions:
    """Test complex WHERE clause functionality."""
    
    @pytest.mark.unit
    def test_and_where_chaining(self):
        """Test chaining WHERE conditions with AND."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").where("active = true").and_where("age > 18").and_where("email IS NOT NULL")
        
        query_str = str(builder)
        assert "WHERE active = true AND age > 18 AND email IS NOT NULL" in query_str

    @pytest.mark.unit
    def test_or_where_conditions(self):
        """Test OR WHERE conditions."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").where("role = 'admin'").or_where("role = 'manager'")
        
        query_str = str(builder)
        assert "WHERE role = 'admin' OR role = 'manager'" in query_str

    @pytest.mark.unit
    def test_complex_where_with_and_or(self):
        """Test complex WHERE with both AND and OR."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").where("active = true").and_where("(role = 'admin' OR role = 'manager')").and_where("created_at > '2023-01-01'")
        
        query_str = str(builder)
        expected_conditions = ["active = true", "(role = 'admin' OR role = 'manager')", "created_at > '2023-01-01'"]
        for condition in expected_conditions:
            assert condition in query_str

    @pytest.mark.unit
    def test_case_when_expression(self):
        """Test CASE WHEN expressions in SELECT."""
        builder = QueryBuilder()
        case_expr = "CASE WHEN age < 18 THEN 'minor' WHEN age >= 65 THEN 'senior' ELSE 'adult' END as age_group"
        builder.select("name", case_expr).from_table("users")
        
        query_str = str(builder)
        assert "CASE WHEN age < 18 THEN 'minor'" in query_str
        assert "ELSE 'adult' END as age_group" in query_str


class TestQueryBuilderCRUDOperations:
    """Test INSERT, UPDATE, DELETE operations."""
    
    @pytest.mark.unit
    def test_insert_basic(self):
        """Test basic INSERT statement."""
        builder = QueryBuilder()
        builder.insert("users", {"name": "John Doe", "email": "john@example.com", "active": True})
        
        query_str = str(builder)
        assert "INSERT INTO users" in query_str
        assert "(name, email, active)" in query_str
        assert "VALUES" in query_str

    @pytest.mark.unit
    def test_insert_multiple_rows(self):
        """Test INSERT with multiple rows."""
        builder = QueryBuilder()
        rows = [
            {"name": "John", "email": "john@example.com"},
            {"name": "Jane", "email": "jane@example.com"}
        ]
        builder.insert("users", rows)
        
        query_str = str(builder)
        assert "INSERT INTO users" in query_str
        assert "VALUES" in query_str

    @pytest.mark.unit
    def test_update_with_where(self):
        """Test UPDATE with WHERE clause."""
        builder = QueryBuilder()
        builder.update("users", {"active": False, "updated_at": "NOW()"}).where("last_login < '2023-01-01'")
        
        query_str = str(builder)
        assert "UPDATE users" in query_str
        assert "SET" in query_str
        assert "WHERE last_login < '2023-01-01'" in query_str

    @pytest.mark.unit
    def test_delete_with_where(self):
        """Test DELETE with WHERE clause."""
        builder = QueryBuilder()
        builder.delete().from_table("users").where("active = false").and_where("created_at < '2020-01-01'")
        
        query_str = str(builder)
        assert "DELETE FROM users" in query_str
        assert "WHERE active = false AND created_at < '2020-01-01'" in query_str

    @pytest.mark.unit
    def test_delete_without_where_raises_warning(self):
        """Test that DELETE without WHERE should raise a warning or require confirmation."""
        builder = QueryBuilder()
        with pytest.warns(UserWarning, match="DELETE without WHERE clause"):
            builder.delete().from_table("users")


class TestQueryBuilderSetOperations:
    """Test UNION, INTERSECT, EXCEPT operations."""
    
    @pytest.mark.unit
    def test_union_basic(self):
        """Test UNION operation between two queries."""
        builder1 = QueryBuilder().select("name").from_table("employees")
        builder2 = QueryBuilder().select("name").from_table("contractors")
        
        result = builder1.union(builder2)
        query_str = str(result)
        
        assert "UNION" in query_str
        assert "SELECT name FROM employees" in query_str
        assert "SELECT name FROM contractors" in query_str

    @pytest.mark.unit
    def test_union_all(self):
        """Test UNION ALL operation."""
        builder1 = QueryBuilder().select("email").from_table("customers")
        builder2 = QueryBuilder().select("email").from_table("prospects")
        
        result = builder1.union(builder2, all=True)
        query_str = str(result)
        
        assert "UNION ALL" in query_str

    @pytest.mark.unit
    def test_intersect_operation(self):
        """Test INTERSECT operation."""
        builder1 = QueryBuilder().select("product_id").from_table("sales_2023")
        builder2 = QueryBuilder().select("product_id").from_table("sales_2024")
        
        result = builder1.intersect(builder2)
        query_str = str(result)
        
        assert "INTERSECT" in query_str

    @pytest.mark.unit
    def test_except_operation(self):
        """Test EXCEPT operation."""
        builder1 = QueryBuilder().select("user_id").from_table("all_users")
        builder2 = QueryBuilder().select("user_id").from_table("banned_users")
        
        result = builder1._except(builder2)
        query_str = str(result)
        
        assert "EXCEPT" in query_str


class TestQueryBuilderAdvancedFeatures:
    """Test advanced SQL features."""
    
    @pytest.mark.unit
    def test_with_cte_basic(self):
        """Test Common Table Expression (CTE)."""
        builder = QueryBuilder()
        cte_query = QueryBuilder().select("department", "AVG(salary) as avg_salary").from_table("employees").group_by("department")
        
        builder.with_cte("dept_averages", cte_query).select("*").from_table("dept_averages").where("avg_salary > 50000")
        
        query_str = str(builder)
        assert "WITH dept_averages AS" in query_str
        assert "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department" in query_str

    @pytest.mark.unit
    def test_with_multiple_ctes(self):
        """Test multiple CTEs."""
        builder = QueryBuilder()
        cte1 = QueryBuilder().select("user_id", "COUNT(*) as order_count").from_table("orders").group_by("user_id")
        cte2 = QueryBuilder().select("user_id", "SUM(amount) as total_spent").from_table("payments").group_by("user_id")
        
        builder.with_cte("user_orders", cte1).with_cte("user_payments", cte2).select("*").from_table("user_orders").join("user_payments", "user_orders.user_id = user_payments.user_id")
        
        query_str = str(builder)
        assert "WITH user_orders AS" in query_str
        assert ", user_payments AS" in query_str

    @pytest.mark.unit
    def test_returning_clause(self):
        """Test RETURNING clause for INSERT/UPDATE/DELETE."""
        builder = QueryBuilder()
        builder.insert("users", {"name": "John", "email": "john@example.com"}).returning("id", "created_at")
        
        query_str = str(builder)
        assert "RETURNING id, created_at" in query_str

    @pytest.mark.unit
    def test_on_conflict_do_nothing(self):
        """Test ON CONFLICT DO NOTHING."""
        builder = QueryBuilder()
        builder.insert("users", {"email": "john@example.com", "name": "John"}).on_conflict("email").do_nothing()
        
        query_str = str(builder)
        assert "ON CONFLICT (email) DO NOTHING" in query_str

    @pytest.mark.unit
    def test_on_conflict_do_update(self):
        """Test ON CONFLICT DO UPDATE (UPSERT)."""
        builder = QueryBuilder()
        builder.insert("users", {"email": "john@example.com", "name": "John", "login_count": 1}).on_conflict("email").do_update({"login_count": "users.login_count + 1", "last_login": "NOW()"})
        
        query_str = str(builder)
        assert "ON CONFLICT (email) DO UPDATE SET" in query_str
        assert "login_count = users.login_count + 1" in query_str

    @pytest.mark.unit
    def test_exists_subquery(self):
        """Test EXISTS with subquery."""
        subquery = QueryBuilder().select("1").from_table("orders").where("orders.user_id = users.id").and_where("orders.status = 'completed'")
        
        builder = QueryBuilder()
        builder.select("*").from_table("users").where_exists(subquery)
        
        query_str = str(builder)
        assert "WHERE EXISTS" in query_str
        assert "SELECT 1 FROM orders WHERE orders.user_id = users.id" in query_str

    @pytest.mark.unit
    def test_first_method_adds_limit_1(self):
        """Test that first() method adds LIMIT 1."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").where("active = true").first()
        
        query_str = str(builder)
        assert "LIMIT 1" in query_str


class TestQueryBuilderUtilityMethods:
    """Test utility and execution methods."""
    
    @pytest.mark.unit
    def test_get_sql_returns_string(self):
        """Test get_sql() returns the SQL string."""
        builder = QueryBuilder()
        builder.select("name").from_table("users").where("active = true")
        
        sql = builder.get_sql()
        assert isinstance(sql, str)
        assert "SELECT name FROM users WHERE active = true" in sql

    @pytest.mark.unit
    def test_get_params_returns_parameters(self):
        """Test get_params() returns query parameters."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").where("age > %s AND city = %s", [18, "New York"])
        
        params = builder.get_params()
        assert params == [18, "New York"]

    @pytest.mark.unit
    def test_execute_method_exists(self):
        """Test execute() method for running the query."""
        builder = QueryBuilder()
        builder.select("*").from_table("users")
        
        # execute() should return self for method chaining in builder pattern
        result = builder.execute()
        assert result is builder


class TestQueryBuilderComplexScenarios:
    """Test complex, real-world query scenarios."""
    
    @pytest.mark.unit
    def test_complex_reporting_query(self):
        """Test a complex reporting query with joins, aggregation, and ordering."""
        builder = QueryBuilder()
        builder.select(
            "u.name",
            "u.email", 
            "COUNT(o.id) as order_count",
            "SUM(o.total) as total_spent",
            "AVG(o.total) as avg_order_value"
        ).from_table("users u").left_join("orders o", "u.id = o.user_id").where("u.active = true").group_by("u.id", "u.name", "u.email").having("COUNT(o.id) > 0").order_by("total_spent DESC").limit(100)
        
        query_str = str(builder)
        assert "LEFT JOIN orders o ON u.id = o.user_id" in query_str
        assert "GROUP BY u.id, u.name, u.email" in query_str
        assert "HAVING COUNT(o.id) > 0" in query_str
        assert "ORDER BY total_spent DESC" in query_str
        assert "LIMIT 100" in query_str

    @pytest.mark.unit
    def test_window_function_query(self):
        """Test query with window functions."""
        builder = QueryBuilder()
        builder.select(
            "name",
            "salary",
            "department",
            "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank",
            "LAG(salary) OVER (ORDER BY salary) as prev_salary"
        ).from_table("employees").where("active = true")
        
        query_str = str(builder)
        assert "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC)" in query_str
        assert "LAG(salary) OVER (ORDER BY salary)" in query_str

    @pytest.mark.unit
    def test_recursive_cte_query(self):
        """Test recursive Common Table Expression."""
        builder = QueryBuilder()
        base_query = QueryBuilder().select("id", "name", "manager_id", "1 as level").from_table("employees").where("manager_id IS NULL")
        recursive_query = QueryBuilder().select("e.id", "e.name", "e.manager_id", "org.level + 1").from_table("employees e").inner_join("org_chart org", "e.manager_id = org.id")
        
        builder.with_recursive_cte("org_chart", base_query, recursive_query).select("*").from_table("org_chart")
        
        query_str = str(builder)
        assert "WITH RECURSIVE org_chart AS" in query_str
        assert "UNION ALL" in query_str


class TestQueryBuilderErrorHandling:
    """Test error conditions and edge cases."""
    
    @pytest.mark.unit
    def test_invalid_join_without_condition_raises_error(self):
        """Test that JOIN without ON condition raises error."""
        builder = QueryBuilder()
        with pytest.raises(ValueError, match="JOIN requires ON condition"):
            builder.select("*").from_table("users").inner_join("orders")

    @pytest.mark.unit
    def test_having_without_group_by_raises_warning(self):
        """Test that HAVING without GROUP BY raises warning."""
        builder = QueryBuilder()
        with pytest.warns(UserWarning, match="HAVING clause without GROUP BY"):
            builder.select("*").from_table("users").having("COUNT(*) > 5")

    @pytest.mark.unit
    def test_offset_without_limit_raises_warning(self):
        """Test that OFFSET without LIMIT raises warning."""
        builder = QueryBuilder()
        with pytest.warns(UserWarning, match="OFFSET without LIMIT"):
            builder.select("*").from_table("users").offset(10)

    @pytest.mark.unit
    def test_empty_select_with_no_default_raises_error(self):
        """Test that empty select() call without table raises error."""
        builder = QueryBuilder()
        with pytest.raises(ValueError, match="No columns specified"):
            builder.select().from_table("users")

    @pytest.mark.unit
    def test_duplicate_table_alias_detection(self):
        """Test detection of duplicate table aliases."""
        builder = QueryBuilder()
        with pytest.raises(ValueError, match="Duplicate table alias"):
            builder.select("*").from_table("users u").inner_join("orders u", "u.id = u.user_id")

    @pytest.mark.unit
    def test_sql_injection_prevention_in_table_names(self):
        """Test that table names are validated to prevent SQL injection."""
        builder = QueryBuilder()
        with pytest.raises(ValueError, match="Invalid table name"):
            builder.from_table("users; DROP TABLE users; --")

    @pytest.mark.unit
    def test_parameter_validation_for_limit_offset(self):
        """Test that LIMIT and OFFSET only accept positive integers."""
        builder = QueryBuilder()
        
        with pytest.raises(ValueError, match="LIMIT must be positive integer"):
            builder.select("*").from_table("users").limit(-5)
            
        with pytest.raises(ValueError, match="OFFSET must be non-negative integer"):
            builder.select("*").from_table("users").offset(-10)


class TestQueryBuilderParameterization:
    """Test proper parameter handling for prepared statements."""
    
    @pytest.mark.unit
    def test_parameterized_where_conditions(self):
        """Test WHERE conditions with parameters."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").where("age > %s", [18]).and_where("city = %s", ["New York"])
        
        params = builder.get_params()
        assert params == [18, "New York"]
        
        query_str = str(builder)
        assert "WHERE age > %s AND city = %s" in query_str

    @pytest.mark.unit
    def test_named_parameters(self):
        """Test named parameters in queries."""
        builder = QueryBuilder()
        builder.select("*").from_table("users").where("age > %(min_age)s AND city = %(city)s", {"min_age": 18, "city": "New York"})
        
        params = builder.get_params()
        assert params == {"min_age": 18, "city": "New York"}

    @pytest.mark.unit
    def test_parameterized_insert(self):
        """Test parameterized INSERT statements."""
        builder = QueryBuilder()
        builder.insert("users", {"name": "%s", "email": "%s", "age": "%s"}, ["John Doe", "john@example.com", 30])
        
        params = builder.get_params()
        assert params == ["John Doe", "john@example.com", 30]

    @pytest.mark.unit
    def test_mixed_parameter_styles_raises_error(self):
        """Test that mixing parameter styles raises error."""
        builder = QueryBuilder()
        with pytest.raises(ValueError, match="Cannot mix parameter styles"):
            builder.select("*").from_table("users").where("age > %s", [18]).and_where("city = %(city)s", {"city": "NYC"})