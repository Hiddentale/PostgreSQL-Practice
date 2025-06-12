# PostgreSQL Practice

A Python library demonstrating professional database practices with PostgreSQL, featuring connection pooling, query building, and robust error handling.

## Features

- **Connection Pooling**: Singleton-based PostgreSQL connection pool with automatic retry logic
- **Query Builder**: Fluent interface for constructing SQL queries with method chaining
- **Exception Handling**: Custom exception hierarchy mapping PostgreSQL error codes
- **Environment Management**: Configuration support for development, test, and production environments
- **Repository Pattern**: Data access layer abstraction for business logic separation
- **Database Migrations**: Version-controlled schema evolution

## Quick Start

### Installation

```bash
git clone https://github.com/Hiddentale/PostgreSQL-Practice
cd postgresql-practice
pip install -r requirement-dev.txt
```

### Configuration

Create a `.env` file:
```env
DB_ENVIRONMENT=development
DB_HOST=localhost
DB_NAME=northwind
DB_USERNAME=postgres
DB_PASSWORD=your_password
DB_PORT=5432
```

### Basic Usage

#### Connection Pool
```python
from src.database import PostgreSQLConnectionPool, PooledDatabaseConnection

with PostgreSQLConnectionPool() as pool:
    with PooledDatabaseConnection(pool) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM customers LIMIT 5")
        results = cursor.fetchall()
```

#### Query Builder
```python
from src.database import QueryBuilder

# Simple query
query = (QueryBuilder()
    .select("name", "email")
    .from_table("customers")
    .where("active = true", "city = 'New York'")
    .order_by("name"))

print(str(query))
# Output: SELECT name,email FROM customers WHERE active = true AND city = 'New York' ORDER BY name

# Complex query with joins
query = (QueryBuilder()
    .select("c.name", "COUNT(o.id) as order_count")
    .from_table("customers c")
    .left_join("orders o", "c.id = o.customer_id")
    .where("c.active = true")
    .group_by("c.id", "c.name")
    .having("COUNT(o.id) > 0")
    .order_by("order_count DESC")
    .limit(10))
```

#### Exception Handling
```python
from src.database.exceptions import ConnectionError, SQLSyntaxError

try:
    with PostgreSQLConnectionPool() as pool:
        # Database operations
        pass
except ConnectionError as e:
    print(f"Connection failed: {e.message}")
    print(f"Details: {e.details}")
except SQLSyntaxError as e:
    print(f"SQL error: {e.message}")
```

## Project Structure

```
src/
├── database/
│   ├── connection.py      # Connection pooling and management
│   ├── query_executors.py # Query builder implementation
│   └── exceptions.py      # Custom exception hierarchy
├── models/               # Data models and validation
├── repositories/         # Data access layer
└── services/            # Business logic layer

database/
├── migrations/          # Database schema versions
├── schemas/            # Full schema definitions
└── seeds/              # Sample data scripts

tests/
├── unit/               # Unit tests
├── integration/        # Integration tests
└── functional/         # End-to-end tests
```

## Environment Support

- **Development**: Local PostgreSQL with sample data
- **Test**: Isolated test database for automated testing  
- **Production**: Production-ready configuration with security considerations

## Testing

```bash
# Run all tests
pytest

# Run specific test categories
pytest -m unit
pytest -m integration
pytest -m slow
```
