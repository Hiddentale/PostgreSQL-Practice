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


class TestQueryBuilderStubMethods:
    """Test that stub methods exist and return self for future implementation."""
    
    @pytest.mark.unit
    def test_stub_methods_exist_and_return_self(self):
        """Test that all stub methods exist and return self."""
        builder = QueryBuilder()
        
        # Core query operations
        assert builder.count() is builder
        assert builder.delete() is builder
        assert builder.distinct() is builder
        
        # Joins
        assert builder.inner_join() is builder
        assert builder.left_join() is builder
        
        # Query structure
        assert builder.group_by() is builder
        assert builder.order_by() is builder
        assert builder.limit() is builder