import os 
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database

def populate_reconcile_events():
    db = Database()
    db.connect()

    df = db.select_table('parsed_logs')
    reconcile_df = (
            df[df["time"].notnull()]
            .rename(columns={
                "transaction_id": "transaction_id",
                "userid": "user_id",
                "oldbalance": "old_balance",
                "newbalance": "new_balance",
                "paymentbalance": "payment_balance",
                "subscriptionbalance": "subscription_balance",
                "action": "event_type",
                "time": "timestamp"
            })[
                ["transaction_id", "user_id", "currency", "amount", "vat",
                "old_balance", "new_balance", "payment_balance",
                "subscription_balance", "event_type", "timestamp"]
            ]
    )


    print(reconcile_df.columns)
    db.insert_dataframe(table_name='reconcile_events', dataframe=reconcile_df)
    db.close_connection()


if __name__ == "__main__":
    populate_reconcile_events()
