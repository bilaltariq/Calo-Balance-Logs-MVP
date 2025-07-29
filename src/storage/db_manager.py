import sqlite3
import pandas as pd
from pathlib import Path
import os

class Database:

    def __init__(self, db_name=None):
        """
        Initialize Database with optional db_name or environment variable DB_PATH.
        """
        env_db_path = os.getenv("DB_PATH")
        if env_db_path:
            db_name = env_db_path
        else:
            # Fallback to project root calculation
            project_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "../../")
            )
            if db_name is None:
                db_name = os.path.join(project_root, "data/transformed/calo.db")

        self.db_name = db_name
        self.connection = None

    # --- Context manager support ---
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close_connection()

    # --- Connection handling ---
    def connect(self):
        """
        Connect to SQLite DB and apply performance PRAGMAs.
        Creates directory and DB file if missing.
        """
        try:
            # Ensure folder exists
            os.makedirs(os.path.dirname(self.db_name), exist_ok=True)
            self.connection = sqlite3.connect(self.db_name)

            # Apply performance PRAGMAs
            cursor = self.connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")      # Better concurrency
            cursor.execute("PRAGMA synchronous=NORMAL;")    # Balance durability/perf
            cursor.execute("PRAGMA temp_store=MEMORY;")     # Temp data in memory
            cursor.execute("PRAGMA mmap_size=30000000000;") # 30 GB memory mapping (safe fallback)
            cursor.close()

        except sqlite3.Error as e:
            raise Exception(f"Error connecting to database: {e}")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    # --- Schema management ---
    def create_table(self, table_name, columns_dict):
        """
        Create a table if it doesn't exist.
        """
        try:
            cursor = self.connection.cursor()
            columns_str = ', '.join([f"{col} {dtype}" for col, dtype in columns_dict.items()])
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str});"
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
            print(f"Table {table_name} created (if not exists).")
        except sqlite3.Error as e:
            print(f"Error: Unable to create table {table_name}. {e}")

    def ensure_table(self, table_name: str, schema: dict):
        """
        Ensure table exists with given schema. Does not drop or alter existing columns.
        """
        self.create_table(table_name, schema)

    def add_column_if_missing(self, table_name: str, column_name: str, dtype: str = "TEXT"):
        """
        Add a column to an existing table if it doesn't exist.
        """
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_cols = [row[1] for row in cursor.fetchall()]

        if column_name not in existing_cols:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {dtype}")
            self.connection.commit()
            print(f"Added missing column '{column_name}' to {table_name}")

    # --- Insert methods ---
    def insert_dataframe(self, table_name, dataframe, if_exists_m='append'):
        """
        Insert pandas DataFrame into table (basic method).
        """
        try:
            dataframe.to_sql(table_name, self.connection, if_exists=if_exists_m, index=False)
            self.connection.commit()
            print(f"Inserted {len(dataframe)} rows into {table_name}.")
        except sqlite3.Error as e:
            print(f"Error inserting into {table_name}: {e}")

    def insert_dataframe_bulk(self, table_name, dataframe, if_exists='append', chunksize=1000):
        """
        Bulk insert DataFrame using pandas to_sql with chunksize and multi-insert.
        """
        try:
            dataframe.to_sql(
                table_name,
                self.connection,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method='multi'
            )
            self.connection.commit()
            print(f"Inserted {len(dataframe)} rows into {table_name} in bulk.")
        except sqlite3.Error as e:
            print(f"Error bulk inserting into {table_name}: {e}")

    def insert_rows_dynamic(self, table_name: str, rows: list):
        """
        Insert rows into table dynamically adding columns if new keys are found.
        Optimized: batch insert, cached schema, single commit.
        """
        if not rows:
            print("No rows to insert.")
            return

        cursor = self.connection.cursor()

        # Get existing columns
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_cols = {info[1] for info in cursor.fetchall()}

        # Collect all columns from rows
        all_keys = set()
        for row in rows:
            all_keys.update(row.keys())

        # Add missing columns
        new_cols = all_keys - existing_cols
        for col in new_cols:
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{col}' TEXT")
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    continue
                else:
                    raise

        # Prepare insert
        cols_order = list(all_keys)
        placeholders = ", ".join(["?"] * len(cols_order))
        col_names_str = ", ".join([f"'{c}'" for c in cols_order])

        values = []
        for row in rows:
            values.append([
                str(row.get(c)) if isinstance(row.get(c), (list, dict)) else row.get(c)
                for c in cols_order
            ])

        cursor.executemany(
            f"INSERT INTO {table_name} ({col_names_str}) VALUES ({placeholders})",
            values
        )
        self.connection.commit()

    # --- Query methods ---
    def select_table(self, table_name):
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(rows, columns=columns)

    def execute_query(self, query):
        """
        Execute a raw SQL query and return the result as a pandas DataFrame.
        """
        if self.connection is None:
            raise Exception("Database connection is not established. Call connect() first.")

        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

    # --- Deletion / Drop ---
    def delete_rows(self, table_name, where_clause=None, params=None):
        """
        Delete rows from a table based on a WHERE clause.
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        table_exists = cursor.fetchone()

        if not table_exists:
            print(f"Table '{table_name}' does not exist.")
            return

        query = f"DELETE FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        self.connection.execute(query, params or ())
        self.connection.commit()

    def drop_table(self, table_name):
        """
        Drop a table if it exists.
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        table_exists = cursor.fetchone()

        if not table_exists:
            print(f"Table '{table_name}' does not exist.")
            return

        self.connection.execute(f"DROP TABLE {table_name}")
        self.connection.commit()
        print(f"Table '{table_name}' dropped successfully.")
