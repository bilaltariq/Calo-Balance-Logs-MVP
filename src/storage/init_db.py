from db_manager import Database
import pandas as pd


def initialize_and_seed_db():
    db = Database()
    db.connect()

    reconcile_events_schema = {
        "transaction_id": "TEXT PRIMARY KEY",       # unique transaction id
        "user_id": "TEXT",              # user who performed the transaction
        "currency": "TEXT",             # currency used
        "amount": "REAL",               # transaction amount
        "vat": "REAL",                  # VAT amount
        "old_balance": "REAL",          # balance before transaction
        "new_balance": "REAL",          # balance after transaction
        "payment_balance": "REAL",      # payment balance if available
        "subscription_balance": "REAL", # subscription balance if available
        "event_type": "TEXT",           # action or event name
        "timestamp": "TEXT"             # event time
    }

    db.create_table("reconcile_events", reconcile_events_schema)

    db.close_connection()
    print("Database initialized and sample data inserted successfully.")


if __name__ == "__main__":
    initialize_and_seed_db()
