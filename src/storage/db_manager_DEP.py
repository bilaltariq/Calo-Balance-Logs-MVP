import sqlite3
import pandas as pd
from pathlib import Path
import os

class Database:

    def __init__(self, db_name=None):
        # Check environment variable first (for Docker)
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

    # def __init__(self, db_name=None):
    #     # Determine project root (2 levels up from this file)
    #     project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    #     if db_name is None:
    #         db_name = os.path.join(project_root, "data/transformed/calo.db")

    #     self.db_name = db_name
    #     self.connection = None


    def connect(self):
        """
        Connect to SQLite DB. Creates directory and DB file if missing.
        """
        try:
            # Ensure folder exists
            os.makedirs(os.path.dirname(self.db_name), exist_ok=True)
            self.connection = sqlite3.connect(self.db_name)
        except sqlite3.Error as e:
            raise Exception(f"Error connecting to database: {e}")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def create_table(self, table_name, columns_dict):
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


    def insert_dataframe(self, table_name, dataframe, if_exists_m='append'):
        try:
            dataframe.to_sql(table_name, self.connection, if_exists=if_exists_m, index=False)
            self.connection.commit()
            print(f"Inserted {len(dataframe)} rows into {table_name}.")
        except sqlite3.Error as e:
            print(f"Error inserting into {table_name}: {e}")


    def select_table(self, table_name):
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(rows, columns=columns)


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

    def insert_rows_dynamic(self, table_name: str, rows: list):
        """
        Insert rows into table dynamically adding columns if new keys are found.
        """
        if not rows:
            print("No rows to insert.")
            return

        cursor = self.connection.cursor()

        # 1. Get existing columns
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_cols = {info[1] for info in cursor.fetchall()}

        # 2. Collect all columns from rows
        all_keys = set()
        for row in rows:
            all_keys.update(row.keys())

        # 3. Add missing columns
        new_cols = all_keys - existing_cols
        for col in new_cols:
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{col.lower()}' TEXT")
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    continue  # Safe to ignore duplicates
                else:
                    raise

        # 4. Insert rows
        cols_order = list(all_keys)
        placeholders = ", ".join(["?"] * len(cols_order))
        col_names_str = ", ".join([f"'{c}'" for c in cols_order])

        for row in rows:
            values = [str(row.get(c)) if isinstance(row.get(c), (list, dict)) else row.get(c) for c in cols_order]
            # values = ["" if row.get(c) is None else str(row.get(c)) for c in cols_order]
            cursor.execute(f"INSERT INTO {table_name} ({col_names_str}) VALUES ({placeholders})", values)

        self.connection.commit()
        #print(f"Inserted {len(rows)} rows into {table_name}.")

    def delete_rows(self, table_name, where_clause=None, params=None):
        """
        Delete rows from a table based on a WHERE clause.
        Example:
            db.delete_rows("parsed_logs", "filename = ?", ("somefile",))
        """

        # Check if table exists
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        table_exists = cursor.fetchone()

        if not table_exists:
            print(f"Table '{table_name}' does not exist.")
            return  # Or raise an Exception if you want strict handling

        # Proceed with deletion
        query = f"DELETE FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        self.connection.execute(query, params or ())
        self.connection.commit()

    def drop_table(self, table_name):
        """
        Drop a table if it exists.
        Example:
            db.drop_table("parsed_logs")
        """

        # Check if table exists
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        table_exists = cursor.fetchone()

        if not table_exists:
            print(f"Table '{table_name}' does not exist.")
            return  # Or raise an Exception if you want strict handling

        # Drop table
        self.connection.execute(f"DROP TABLE {table_name}")
        self.connection.commit()
        print(f"Table '{table_name}' dropped successfully.")


    def execute_query(self, query):
        """
        Execute a raw SQL query and return the result as a pandas DataFrame.

        Example:
            df = db.execute_query("SELECT * FROM parsed_logs WHERE type='DEBIT'")
        """
        import pandas as pd

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
