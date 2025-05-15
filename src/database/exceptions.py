"""
PostgreSQL database exceptions hierarchy.

This module defines a set of exceptions that map to PostgreSQL error classes,
providing more specific error types for different database failure scenarios.
Each exception corresponds to a specific class of PostgreSQL error codes.
"""

from datetime import datetime
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
        self.timestamp = datetime.now()


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

# PostgreSQL-specific error codes mapped to exception classes
PG_ERROR_MAPPING = {
    '08': ConnectionError,
    '22': InputDataError,
    '42': SyntaxError,
    '53': OutOfResourcesError,
    '57': AdminInterventionError,
    '58': SystemError,
}
