import sqlite3
import os

db_path = "analyzer.db"

if not os.path.exists(db_path):
    print(f"Error: {db_path} not found.")
else:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table_tuple in tables:
        table = table_tuple[0]
        print(f"\n--- TABLE: {table} ---")
        
        # Get headers
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [col[1] for col in cursor.fetchall()]
        print(" | ".join(columns))
        print("-" * (len(" | ".join(columns))))
        
        # Get data
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        for row in rows:
            print(" | ".join(map(str, row)))

    conn.close()
