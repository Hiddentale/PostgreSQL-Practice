"""
PostgreSQL database exceptions hierarchy.

This module defines a set of exceptions that map to PostgreSQL error classes,
providing more specific error types for different database failure scenarios.
Each exception corresponds to a specific class of PostgreSQL error codes.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional


class DatabaseError(Exception):
    """Base exception for all database-related errors.
    
    Attributes:
        message (str): Human-readable error description
        details (Dict): Additional context about the error
        timestamp (datetime): When the exception was created
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details

        super().__init__(self, f"{self.message}")
    
    def __str__(self):
        detail_items = [f"{key}={value}" for key, value in self.details.items() if value is not None]
        return f"{self.message} [{', '.join(detail_items)}]"
    
    @classmethod
    def from_postgres_exception(cls, postgres_exception, params=None, query=None):
        """Create the appropriate DatabaseError subclass from a PostgreSQL exception.

        Where postgres_exception is postgresql's error object given by psycopg2.
        """
        if params is not None and not isinstance(params, dict):
            raise ValueError("Database parameters must be provided as a dictionary for proper error handling")
    
        sqlstate = getattr(postgres_exception.diag, "sqlstate", "")
        error_class = sqlstate[:2] if sqlstate else "" # Grab the first 2 numbers of the postgreqsql object's error code

        exception_class = PG_ERROR_MAPPING.get(error_class, DatabaseError)

        message = str(postgres_exception)
        details = {
            "query": query,
            "params": cls.remove_password_and_tokens_from_params(params),
            "sqlstate": sqlstate,
            "message_detail": getattr(postgres_exception.diag, "message_detail"),
            "constraint_name": getattr(postgres_exception.diag, "constraint_name"),
            "schema_name": getattr(postgres_exception.diag, "schema_name"),
            "table_name": getattr(postgres_exception.diag, "table_name"),
            "column_name": getattr(postgres_exception.diag, "column_name"),
            "statement_position": getattr(postgres_exception.diag, "statement_position"),
            "datetime": datetime.time(timezone.utc)
        }
        details = {key: value for key, value in details.items() if value is not None}
        return exception_class(message, details)
    
    @staticmethod
    def remove_password_and_tokens_from_params(params):
        if params is None:
            return None
        
        cleaned_params = params.copy()
        for key in cleaned_params:
            if any(sensitive in key.lower() for sensitive in ['password', 'token', 'secret', 'key']):
                cleaned_params[key] = "[REDACTED]"
        return cleaned_params


class ConnectionError(DatabaseError):
    
    """Connection establishment or maintenance errors.
    
    Raised when the application cannot connect to the database server
    or when an existing connection is disrupted.
    
    Maps to PostgreSQL Class 08 errors (Connection Exception).
    Examples: server shutdown, connection timeout, authentication failure.
    """


class ConfigurationError(DatabaseError):
    """Database Configuration related errors.
    
    Raised when there's an issue with database configuration parameters
    or environment settings. 
    
    Examples: missing connection parameters, invalid credentials format,
    incompatible settings.
    """


class QueryError(DatabaseError):
    """SQL execution errors.
    
    A general class for errors that occur during query execution that
    aren't covered by more specific exception types.
    
    Examples: transaction failures, constraint violations.
    """


class InputDataError(DatabaseError):
    """Invalid data errors.
    
    Raised when the data provided doesn't match what the database expects.
    
    Maps to PostgreSQL Class 22 errors (Data Exception).
    Examples: invalid text encoding, numeric value out of range, 
    division by zero.
    """


class SQLSyntaxError(DatabaseError):
    """SQL syntax and semantic errors.
    
    Raised when a query has syntax errors or references non-existent
    database objects.
    
    Maps to PostgreSQL Class 42 errors (Syntax Error or Access Rule Violation).
    Examples: syntax error in SQL, undefined table, undefined column.
    """


class OutOfResourcesError(DatabaseError):
    """Database is out of resources related errors.
    
     Raised when the database cannot complete an operation due to
    resource limitations.
    
    Maps to PostgreSQL Class 53 errors (Insufficient Resources).
    Examples: disk full, out of memory, too many connections.
    """


class AdminInterventionError(DatabaseError):
    """Admin database intervation errors.
    
    Raised when database administrator actions affect normal operations.
    
    Maps to PostgreSQL Class 57 errors (Operator Intervention).
    Examples: database shutdown, admin-canceled query.
    """

class SystemError(DatabaseError):
    """Errors related to something external to PostgreSQL itself.

    Raised for system-level failures that affect the database.
    
    Maps to PostgreSQL Class 58 errors (System Error).
    Examples: I/O errors, unexpected system call failures.
    """


class IntegrityConstraintViolation(DatabaseError):
    """Constraint violation errors like unique, foreign key, or check constraints."""


class TransactionError(DatabaseError):
    """Transaction rollback errors such as serialization failures or deadlocks."""


class FeatureNotSupportedError(DatabaseError):
    """Errors when attempting to use features not supported by the PostgreSQL server."""


# PostgreSQL-specific error codes mapped to exception classes
PG_ERROR_MAPPING = {
    '08': ConnectionError,
    '22': InputDataError,
    '23': IntegrityConstraintViolation,
    '40': TransactionError,
    '42': SQLSyntaxError,
    '53': OutOfResourcesError,
    '57': AdminInterventionError,
    '58': SystemError,
    '0A': FeatureNotSupportedError,
}

# SQL error codes
# Class 00: Successful Completion
# Class 01: Warning
# Class 02: No Data
# Class 03: SQL Statement Not Yet Complete
# Class 08: Connection Exceptions
# Class 09: Triggered Action Exceptions
# Class 0A: Feature Not Supported
# Class 0B: Invalid Transaction Initiation
# Class 0F: Locator Exception
# Class 0L: Invalid Grantor
# Class 0P: Invalid Role Specification
# Class 0Z: Diagnostics Exception
# Class 20: Case Not Found
# Class 21: Cardinality Violation
# Class 22: Data Exception
# Class 23: Integrity Constraint Violation
# Class 24: Invalid Cursor State
# Class 25: Invalid Transaction State
# Class 26: Invalid SQL Statement Name
# Class 27: Triggered Data Change Violation
# Class 28: Invalid Authorization Specification
# Class 2B: Dependent Privilege Descriptors Still Exist
# Class 2D: Invalid Transaction Termination
# Class 2F: SQL Routine Exception
# Class 34: Invalid Cursor Name
# Class 38: External Routine Exception
# Class 39: External Routine Invocation Exception
# Class 3B: Savepoint Exception
# Class 3D: Invalid Catalog Name
# Class 3F: Invalid Schema Name
# Class 40: Transaction Rollback
# Class 42: Syntax Error or Access Rule Violation
# Class 44: WITH CHECK OPTION Violation
# Class 53: Insufficient Resources
# Class 54: Program Limit Exceeded
# Class 55: Object Not In Prerequisite State
# Class 57: Operator Intervention
# Class 58: System Error
# Class F0: Configuration File Error
# Class HV: Foreign Data Wrapper Error
# Class P0: PL/pgSQL Error
# Class XX: Internal Error
