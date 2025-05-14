class DatabaseError(Exception):
    """Base exception for all database-related errors."""
    pass


class ConnectionError(DatabaseError):
    """Connection establishment or maintenance errors."""
    pass


class ConfigurationError(DatabaseError):
    """"""
    pass


class QueryError(DatabaseError):
    """SQL execution errors."""
    pass

               # if error.pgcode is not None:
                    #postgres_error_class = error.pgcode[:2]
                    #if postgres_error_class == "08":
                        #logger.warning(f"Connection Exception, {error.pgerror}")
                        #raise Exception("Connection Exception")
                    #elif postgres_error_class == "53":
                        #logger.warning(f"Insufficient Resources, {error.pgerror}")
                        #raise Exception("Insufficient Resources")
                    #elif postgres_error_class == "55":
                        #logger.warning(f"Object Not In Prerequisite State, {error.pgerror}")
                       # raise Exception("Object Not In Prerequisite State")
                   # elif postgres_error_class == "57":
                       # logger.warning(f"Operator Intervention, {error.pgerror}")
                        #raise Exception("Operator Intervention")
                   #elif postgres_error_class == "58":
                        #logger.warning(f"System Error, {error.pgerror}")
                        #raise Exception("System Error")