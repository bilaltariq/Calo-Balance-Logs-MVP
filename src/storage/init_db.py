from db_manager import Database
import pandas as pd


def initialize_and_seed_db():
    db = Database()
    db.connect()

    # -------------------------
    # Create Tables
    # -------------------------
    # 1. balance_events
    balance_events_schema = {
        "id": "TEXT PRIMARY KEY",
        "user_id": "TEXT",
        "currency": "TEXT",
        "amount": "REAL",
        "vat": "REAL",
        "old_balance": "REAL",
        "new_balance": "REAL",
        "event_type": "TEXT",
        "timestamp": "TEXT"
    }
    db.create_table("balance_events", balance_events_schema)

    # 2. sync_issues
    sync_issues_schema = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "user_id": "TEXT",
        "subscription_balance": "REAL",
        "payment_balance": "REAL",
        "detected_at": "TEXT"
    }
    db.create_table("sync_issues", sync_issues_schema)

    # 3. anomalies
    anomalies_schema = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "user_id": "TEXT",
        "description": "TEXT",
        "detected_at": "TEXT"
    }
    db.create_table("anomalies", anomalies_schema)

    # -------------------------
    # Insert Sample Data
    # -------------------------
    balance_data = [
        {
            "id": "01HHEWH0NAQKZ4ZMFW6PM7K046",
            "user_id": "daa2bf74-cf31-4552-9e85-acb48a7c3f90",
            "currency": "BHD",
            "amount": 22.0,
            "vat": 2.0,
            "old_balance": 433.0,
            "new_balance": 453.0,
            "event_type": "CREDIT",
            "timestamp": "2023-12-12T11:28:13.312Z"
        },
        {
            "id": "01HHEBMC9TMTY7D3G906RMNFME",
            "user_id": "5416c645-2ddd-4c73-a383-a80daafc23ca",
            "currency": "BHD",
            "amount": 34.5,
            "vat": 3.136,
            "old_balance": 225.9075,
            "new_balance": 257.2715,
            "event_type": "CREDIT",
            "timestamp": "2023-12-12T06:32:56.714Z"
        },
        {
            "id": "01HHZF2R7DPJCDQ0RC1BCE3Y6R",
            "user_id": "f8c629ff-e140-4e26-b38c-f99d237959da",
            "currency": "SAR",
            "amount": 2760.0,
            "vat": 360.0,
            "old_balance": 100.0,
            "new_balance": 2500.0,
            "event_type": "CREDIT",
            "timestamp": "2023-12-18T21:59:59.622Z"
        }
    ]
    db.insert_dataframe("balance_events", pd.DataFrame(balance_data), if_exists_m="append")

    sync_issues_data = [
        {
            "user_id": "daa2bf74-cf31-4552-9e85-acb48a7c3f90",
            "subscription_balance": 433.0,
            "payment_balance": 850.0,
            "detected_at": "2023-12-12T11:28:13.367Z"
        }
    ]
    db.insert_dataframe("sync_issues", pd.DataFrame(sync_issues_data), if_exists_m="append")

    anomalies_data = [
        {
            "user_id": "5416c645-2ddd-4c73-a383-a80daafc23ca",
            "description": "Multiple credit events within 5 minutes (possible duplicate processing)",
            "detected_at": "2023-12-12T06:37:12.462Z"
        }
    ]
    db.insert_dataframe("anomalies", pd.DataFrame(anomalies_data), if_exists_m="append")

    db.close_connection()
    print("Database initialized and sample data inserted successfully.")


if __name__ == "__main__":
    initialize_and_seed_db()
