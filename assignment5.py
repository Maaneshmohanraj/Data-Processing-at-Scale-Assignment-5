import psycopg2
import psycopg2.extras
import logging
from typing import Any, Optional

# Database configuration
DB_CONFIG = {
    "dbname": "assignment5",
    "user": "postgres",
    "password": "postgres",
    "host": "127.0.0.1",
    "port": "5432"
}

class QueryProcessor:
    """Helper class to handle database query operations"""
    
    @staticmethod
    def execute_query(connection: Any, query: str) -> None:
        """Execute a database query with error handling"""
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()
        except Exception as e:
            connection.rollback()
            raise DatabaseError(f"Query execution failed: {str(e)}")

class DatabaseError(Exception):
    """Custom exception for database operations"""
    pass

def point_query(partition_table: str, utc_timestamp: int, result_table: str, 
                db_connection: Any) -> None:
    """
    Execute a point query on a partitioned table to find records matching a specific UTC timestamp.
    
    Args:
        partition_table: Name of the parent partition table
        utc_timestamp: Target UTC timestamp to match
        result_table: Name of the table to store results
        db_connection: Active database connection object
    
    Raises:
        DatabaseError: If query execution fails
    """
    try:
        # Build query to create result table and populate with matching records
        query = f"""
            DROP TABLE IF EXISTS {result_table} CASCADE;
            CREATE TABLE {result_table} AS
            WITH filtered_data AS (
                SELECT DISTINCT *
                FROM {partition_table}
                WHERE created_utc = {utc_timestamp}
            )
            SELECT *
            FROM filtered_data
            ORDER BY created_utc ASC;
        """
        
        QueryProcessor.execute_query(db_connection, query)
        
    except Exception as e:
        logging.error(f"Point query failed: {str(e)}")
        raise DatabaseError(f"Point query operation failed: {str(e)}")

def range_query(partition_table: str, min_utc: int, max_utc: int, 
                result_table: str, db_connection: Any) -> None:
    """
    Execute a range query on a partitioned table to find records within a UTC timestamp range.
    
    Args:
        partition_table: Name of the parent partition table
        min_utc: Lower bound of UTC range (exclusive)
        max_utc: Upper bound of UTC range (inclusive)
        result_table: Name of the table to store results
        db_connection: Active database connection object
    
    Raises:
        DatabaseError: If query execution fails
    """
    try:
        # Build query to create result table and populate with records in UTC range
        query = f"""
            DROP TABLE IF EXISTS {result_table} CASCADE;
            CREATE TABLE {result_table} AS
            WITH range_results AS (
                SELECT DISTINCT *
                FROM {partition_table}
                WHERE created_utc > {min_utc}
                AND created_utc <= {max_utc}
            )
            SELECT *
            FROM range_results
            ORDER BY created_utc ASC;
        """
        
        QueryProcessor.execute_query(db_connection, query)
        
    except Exception as e:
        logging.error(f"Range query failed: {str(e)}")
        raise DatabaseError(f"Range query operation failed: {str(e)}")