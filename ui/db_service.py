"""
SQLite database service for querying historical data.

Provides methods to query output tables for historical event data.
"""

import sqlite3
from typing import Dict, List, Any, Optional
from pathlib import Path


class DatabaseService:
    """Service for querying SQLite databases."""
    
    def __init__(self, db_path: str = "data/static_tables.db"):
        """
        Initialize database service.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
    
    def query_table(self, table_name: str, limit: int = 100, where_clause: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Query a table for historical data.
        
        Args:
            table_name: Name of table to query
            limit: Maximum rows to return
            where_clause: Optional WHERE clause
        
        Returns:
            List of dicts representing rows
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = f"SELECT * FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += f" ORDER BY rowid DESC LIMIT {limit}"
            
            cursor.execute(query)
            rows = cursor.fetchall()
            conn.close()
            
            # Convert Row objects to dicts
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error querying table '{table_name}': {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Name of table
        
        Returns:
            List of column info dicts with 'name' and 'type'
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            conn.close()
            
            return [
                {"cid": col[0], "name": col[1], "type": col[2], "notnull": col[3], "default": col[4], "pk": col[5]}
                for col in columns
            ]
        except Exception as e:
            print(f"Error getting schema for table '{table_name}': {e}")
            return []
    
    def list_tables(self) -> List[str]:
        """
        List all tables in database.
        
        Returns:
            List of table names
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            return tables
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists.
        
        Args:
            table_name: Name of table
        
        Returns:
            True if table exists, False otherwise
        """
        try:
            tables = self.list_tables()
            return table_name in tables
        except Exception:
            return False
    
    def get_row_count(self, table_name: str) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Name of table
        
        Returns:
            Number of rows
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            conn.close()
            
            return count
        except Exception as e:
            print(f"Error getting row count for table '{table_name}': {e}")
            return 0
