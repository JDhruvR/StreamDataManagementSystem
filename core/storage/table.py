import sqlite3
import pandas as pd

class TableManager:

    # A simple storage manager using SQLite to store table data and query it.
    # Can be used for the static tables in our streaming system.

    def __init__(self, db_path=':memory:'):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        
    def create_table(self, table_name, schema):
        
        # schema: list of dicts [{'name': 'col1', 'type': 'INT'}, ...]
        
        columns = []
        for col in schema:
            col_type = col['type'].upper()
            if col_type == 'STRING':
                sql_type = 'TEXT'
            elif col_type == 'INT':
                sql_type = 'INTEGER'
            elif col_type == 'FLOAT':
                sql_type = 'REAL'
            else:
                sql_type = 'TEXT'
            columns.append(f"{col['name']} {sql_type}")
            
        columns_str = ", ".join(columns)
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
        self.cursor.execute(query)
        self.conn.commit()

    def insert(self, table_name, record):
        # \"\"\"
        # record: dict {'col1': val1, ...}
        # \"\"\"
        columns = ", ".join(record.keys())
        placeholders = ", ".join(["?"] * len(record))
        values = tuple(record.values())
        
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        self.cursor.execute(query, values)
        self.conn.commit()

    def insert_batch(self, table_name, records):
        if not records:
            return
        columns = ", ".join(records[0].keys())
        placeholders = ", ".join(["?"] * len(records[0]))
        values = [tuple(r.values()) for r in records]
        
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        self.cursor.executemany(query, values)
        self.conn.commit()

    def query(self, sql_query):
        # \"\"\"
        # Execute raw SQL on our static tables
        # Returns a pandas dataframe
        # \"\"\"
        return pd.read_sql_query(sql_query, self.conn)

    def close(self):
        self.conn.close()

# Singleton-like instance for basic use
storage = TableManager()
