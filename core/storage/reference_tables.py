import os
import re
import sqlite3
from typing import Any, Dict, List, Tuple


class ReferenceTableStore:
    """Manage reference/dimension tables"""

    _VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    _TYPE_MAP = {
        "STRING": "TEXT",
        "TEXT": "TEXT",
        "INT": "INTEGER",
        "INTEGER": "INTEGER",
        "FLOAT": "REAL",
        "REAL": "REAL",
        "BOOLEAN": "INTEGER",
        "TIMESTAMP": "TEXT",
    }

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def _validate_identifier(self, identifier: str) -> None:
        if not self._VALID_IDENTIFIER.match(identifier):
            raise ValueError(f"Invalid identifier: {identifier}")

    def _to_sql_type(self, data_type: str) -> str:
        return self._TYPE_MAP.get(data_type.upper(), "TEXT")

    def create_table(self, table_name: str, columns: List[Tuple[str, str]]) -> None:
        self._validate_identifier(table_name)
        if not columns:
            raise ValueError("At least one column is required")

        rendered = []
        for name, data_type in columns:
            self._validate_identifier(name)
            rendered.append(f'"{name}" {self._to_sql_type(data_type)}')

        query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(rendered)})'
        self.conn.execute(query)
        self.conn.commit()

    def add_column(self, table_name: str, column_name: str, data_type: str) -> None:
        self._validate_identifier(table_name)
        self._validate_identifier(column_name)
        query = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {self._to_sql_type(data_type)}'
        self.conn.execute(query)
        self.conn.commit()

    def insert_row(self, table_name: str, row_data: Dict[str, Any]) -> None:
        self._validate_identifier(table_name)
        if not row_data:
            raise ValueError("row_data cannot be empty")

        for col in row_data.keys():
            self._validate_identifier(col)

        cols = [f'"{col}"' for col in row_data.keys()]
        placeholders = ["?" for _ in row_data]
        values = list(row_data.values())
        query = f'INSERT INTO "{table_name}" ({", ".join(cols)}) VALUES ({", ".join(placeholders)})'
        self.conn.execute(query, values)
        self.conn.commit()

    def update_rows(self, table_name: str, set_data: Dict[str, Any], where_data: Dict[str, Any]) -> int:
        self._validate_identifier(table_name)
        if not set_data:
            raise ValueError("set_data cannot be empty")
        if not where_data:
            raise ValueError("where_data cannot be empty")

        for col in list(set_data.keys()) + list(where_data.keys()):
            self._validate_identifier(col)

        set_clause = ", ".join([f'"{k}" = ?' for k in set_data.keys()])
        where_clause = " AND ".join([f'"{k}" = ?' for k in where_data.keys()])
        values = list(set_data.values()) + list(where_data.values())
        query = f'UPDATE "{table_name}" SET {set_clause} WHERE {where_clause}'
        cur = self.conn.execute(query, values)
        self.conn.commit()
        return cur.rowcount

    def delete_rows(self, table_name: str, where_data: Dict[str, Any]) -> int:
        self._validate_identifier(table_name)
        if not where_data:
            raise ValueError("where_data cannot be empty")

        for col in where_data.keys():
            self._validate_identifier(col)

        where_clause = " AND ".join([f'"{k}" = ?' for k in where_data.keys()])
        values = list(where_data.values())
        query = f'DELETE FROM "{table_name}" WHERE {where_clause}'
        cur = self.conn.execute(query, values)
        self.conn.commit()
        return cur.rowcount

    def list_tables(self) -> List[str]:
        cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [row[0] for row in cur.fetchall()]

    def table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        self._validate_identifier(table_name)
        cur = self.conn.execute(f'PRAGMA table_info("{table_name}")')
        return [dict(row) for row in cur.fetchall()]

    def select_rows(self, table_name: str, limit: int = 20) -> List[Dict[str, Any]]:
        self._validate_identifier(table_name)
        cur = self.conn.execute(f'SELECT * FROM "{table_name}" LIMIT ?', (limit,))
        return [dict(row) for row in cur.fetchall()]
