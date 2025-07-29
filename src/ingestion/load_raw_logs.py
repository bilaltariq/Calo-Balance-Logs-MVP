import os
import sys
import gzip
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database

# Environment-based configuration
LOGS_DIR = os.getenv("LOGS_DIR","Logs/balance-sync-logs/balance-sync-logs/a3fb6cdb-607b-469f-8f8a-ec4792e827cb")
DB_PATH = os.getenv("DB_PATH", "data/transformed/calo_balances.db")

def read_gz_file(file_path):
    """
    Safely read .gz file and decode as UTF-8.
    """
    with gzip.open(file_path, 'rt', encoding='utf-8', errors='replace') as f:
        return f.read()


def load_files():
    """
    Bulk ingestion using Database class.
    - Creates table if not exists
    - Skips already loaded files
    - Inserts in batch
    """
    # Use Database context manager (auto connect/close)
    with Database(DB_PATH) as db:
        # Ensure table exists
        db.ensure_table("raw_data", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "filename": "TEXT UNIQUE",
            "raw_string": "TEXT",
            "load_timestamp": "TEXT"
        })

        # Get already loaded files
        existing_df = db.execute_query("SELECT filename FROM raw_data")
        already_loaded = set(existing_df['filename']) if not existing_df.empty else set()

        inserts = []

        # Walk through logs and prepare batch
        for root, dirs, files in os.walk(LOGS_DIR):
            for file in files:
                if not file.endswith(".gz"):
                    continue

                folder_name = os.path.basename(root)
                if folder_name in already_loaded:
                    continue

                file_path = os.path.join(root, file)
                raw_content = read_gz_file(file_path)
                inserts.append({
                    "filename": folder_name,
                    "raw_string": raw_content,
                    "load_timestamp": datetime.utcnow().isoformat()
                })

        # Insert batch
        if inserts:
            db.insert_rows_dynamic("raw_data", inserts)

        print(f"Ingestion complete. Inserted {len(inserts)} new files.")


if __name__ == "__main__":
    load_files()
