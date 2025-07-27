import sqlite3
import pandas as pd
from pathlib import Path
import os

class Database:
    def __init__(self, db_name=None):
            # Determine project root (2 levels up from this file)
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
            if db_name is None:
                db_name = os.path.join(project_root, "data/transformed/calo_balances.db")

            self.db_name = db_name
            self.connection = None


    def connect(self):
        """
        Connect to SQLite DB. Creates directory and DB file if missing.
        """
        try:
            # Ensure folder exists
            os.makedirs(os.path.dirname(self.db_name), exist_ok=True)

            # Connect (this creates DB file if it doesn't exist)
            self.connection = sqlite3.connect(self.db_name)
            print(f"Connected to SQLite DB: {self.db_name}")
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

    def initialize_tables(self):
        """
        Create initial tables needed for Calo balance pipeline
        """
        self.connect()

        # Transactions log table
        self.create_table("balance_events", {
            "id": "TEXT PRIMARY KEY",
            "user_id": "TEXT",
            "currency": "TEXT",
            "amount": "REAL",
            "vat": "REAL",
            "old_balance": "REAL",
            "new_balance": "REAL",
            "event_type": "TEXT",   # e.g., CREDIT, DEBIT, SYNC_SKIP
            "timestamp": "TEXT"
        })

        # Sync issues (imbalances between subscription vs payment balance)
        self.create_table("sync_issues", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "user_id": "TEXT",
            "subscription_balance": "REAL",
            "payment_balance": "REAL",
            "detected_at": "TEXT"
        })

        # Anomalies (bonus analysis)
        self.create_table("anomalies", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "user_id": "TEXT",
            "description": "TEXT",
            "detected_at": "TEXT"
        })

        self.close_connection()

    def insert_dataframe(self, table_name, dataframe, if_exists_m='append'):
        try:
            dataframe.to_sql(table_name, self.connection, if_exists=if_exists_m, index=False)
            self.connection.commit()
            print(f"Inserted {len(dataframe)} rows into {table_name}.")
        except sqlite3.Error as e:
            print(f"Error inserting into {table_name}: {e}")

    def insert_event_dict(self, event: dict):
        """
        Insert a single log event (parsed from logs)
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO balance_events
            (id, user_id, currency, amount, vat, old_balance, new_balance, event_type, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.get("id"), event.get("user_id"), event.get("currency"),
            event.get("amount"), event.get("vat"), event.get("old_balance"),
            event.get("new_balance"), event.get("event_type"), event.get("timestamp")
        ))
        self.connection.commit()

    def select_table(self, table_name):
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(rows, columns=columns)

    def query_user_history(self, user_id):
        query = f"""
        SELECT * FROM balance_events
        WHERE user_id = ?
        ORDER BY timestamp
        """
        return pd.read_sql_query(query, self.connection, params=[user_id])

    def get_overdraft_users(self):
        query = """
        SELECT DISTINCT user_id FROM balance_events
        WHERE new_balance < 0
        """
        return pd.read_sql_query(query, self.connection)


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
            query = f"DELETE FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"

            self.connection.execute(query, params or ())
            self.connection.commit()
