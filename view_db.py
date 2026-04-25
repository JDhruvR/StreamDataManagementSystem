import sqlite3

# Connect to the database
conn = sqlite3.connect('data/join_states.db')
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print("Tables:", cursor.fetchall())

# Fetch everything from our state table
cursor.execute('SELECT * FROM "join_state_pollution_weather_join"')
for row in cursor.fetchall():
    print(row)
