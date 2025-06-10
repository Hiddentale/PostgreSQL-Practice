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