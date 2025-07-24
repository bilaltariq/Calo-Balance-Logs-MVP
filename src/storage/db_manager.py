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
