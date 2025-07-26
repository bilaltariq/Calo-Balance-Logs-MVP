import os
import gzip
import sqlite3
from datetime import datetime

LOGS_DIR = "Logs/balance-sync-logs/balance-sync-logs/a3fb6cdb-607b-469f-8f8a-ec4792e827cb"
DB_PATH = "data/transformed/calo_balances.db"

def create_table_if_not_exists():
    """
    Creates raw_data table if it does not exist.
    Stores folder_name (unique file ref), raw string, and load timestamp.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                raw_string TEXT,
                load_timestamp TEXT
            )
        """)
        conn.commit()

# ---- CHECK IF FILE ALREADY LOADED ----
def file_already_loaded(filename):
    """
    Checks if a log (folder name) has already been ingested.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM raw_data WHERE filename = ?", (filename,))
        return cursor.fetchone() is not None

# ---- READ .GZ FILE ----
def read_gz_file(file_path):
    """
    Safely read .gz file and decode as UTF-8, ignoring bad bytes.
    """
    with gzip.open(file_path, 'rt', encoding='utf-8', errors='replace') as f:
        return f.read()

# ---- INGESTION LOGIC ----
def load_files():
    """
    Iterate through nested folders inside LOGS_DIR and load 000000.gz content into DB.
    """
    create_table_if_not_exists()

    for root, dirs, files in os.walk(LOGS_DIR):
        for file in files:
            if file.endswith(".gz"):
                # Use parent folder name as unique filename reference
                folder_name = os.path.basename(root)
                file_path = os.path.join(root, file)

                # Skip if already loaded
                if file_already_loaded(folder_name):
                    print(f"Skipping {folder_name} (already loaded)")
                    continue

                # Read file content
                raw_content = read_gz_file(file_path)

                # Insert into DB
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO raw_data (filename, raw_string, load_timestamp)
                        VALUES (?, ?, ?)
                    """, (folder_name, raw_content, datetime.utcnow().isoformat()))
                    conn.commit()

                print(f"Loaded: {folder_name}")

if __name__ == "__main__":
    load_files()
    print("Ingestion complete.")
