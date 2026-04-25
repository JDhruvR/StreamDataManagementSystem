# import sqlite3

# # Connect to the database
# conn = sqlite3.connect('data/join_states.db')
# cursor = conn.cursor()

# # Get all tables
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print("Tables:", cursor.fetchall())

# # Fetch everything from our state table
# cursor.execute('SELECT * FROM "join_state_pollution_weather_join"')
# for row in cursor.fetchall():
#     print(row)


import sqlite3
import os

DB_PATH = "./data/static_tables.db"

JUNCTION_DATA = [
    ("J1", "school",      30.0, "MG Road & 1st Cross"),
    ("J2", "commercial",  50.0, "Brigade Road Junction"),
    ("J3", "highway",     80.0, "Outer Ring Road Gate 3"),
    ("J4", "residential", 40.0, "Koramangala 5th Block"),
    ("J5", "hospital",    25.0, "Victoria Hospital Approach"),
]

def create_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing '{DB_PATH}'")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE junction_meta (
            junction_id   TEXT PRIMARY KEY,
            zone          TEXT NOT NULL,
            speed_limit   REAL NOT NULL,
            junction_name TEXT NOT NULL
        )
    """)

    cursor.executemany("""
        INSERT INTO junction_meta (junction_id, zone, speed_limit, junction_name)
        VALUES (?, ?, ?, ?)
    """, JUNCTION_DATA)

    conn.commit()

    # Verify
    print(f"\nCreated '{DB_PATH}' with {cursor.rowcount} rows.")
    print(f"\n{'junction_id':<15} {'zone':<15} {'speed_limit':<15} {'junction_name'}")
    print("-" * 65)
    for row in cursor.execute("SELECT * FROM junction_meta ORDER BY junction_id"):
        print(f"{row[0]:<15} {row[1]:<15} {row[2]:<15} {row[3]}")

    conn.close()

if __name__ == "__main__":
    create_db()