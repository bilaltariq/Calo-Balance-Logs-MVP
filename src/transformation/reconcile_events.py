import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database

def populate_reconcile_events():
    db = Database()
    db.connect()

    # Reconciliation query with COALESCE and country mapping
    query = """
    SELECT 
        time AS timestamp,
        requestid,
        transaction_id,
        type,
        userid AS user_id,
        COALESCE(oldbalance, 0) AS old_balance,
        CASE 
            WHEN type = 'DEBIT' THEN -ABS(COALESCE(amount, 0)) 
            ELSE ABS(COALESCE(amount, 0)) 
        END AS amount,
        COALESCE(newbalance, 0) AS new_balance,
        COALESCE(paymentbalance, 0) AS payment_balance,
        COALESCE(subscriptionbalance, 0) AS subscription_balance,
        COALESCE(vat, 0) AS vat,
        action AS event_type,
        source AS source_type,
        currency,
        filename,
        CASE currency
            WHEN 'SAR' THEN 'Saudi Arabia'
            WHEN 'BHD' THEN 'Bahrain'
            WHEN 'AED' THEN 'United Arab Emirates'
            WHEN 'KWD' THEN 'Kuwait'
            WHEN 'OMR' THEN 'Oman'
            ELSE 'Unknown'
        END AS country,
        CASE 
            WHEN ROUND(CAST(COALESCE(oldbalance, 0) AS FLOAT), 1) 
                 + ROUND(CASE WHEN type = 'DEBIT' THEN -ABS(COALESCE(amount, 0)) ELSE ABS(COALESCE(amount, 0)) END, 1)
                 - ROUND(CAST(COALESCE(vat, 0) AS FLOAT), 1) 
                 != ROUND(CAST(COALESCE(newbalance, 0) AS FLOAT), 1)
                 AND COALESCE(paymentbalance, 0) != COALESCE(subscriptionbalance, 0)
            THEN 'CALCULATION + BALANCE SYNC'
            
            WHEN ROUND(CAST(COALESCE(oldbalance, 0) AS FLOAT), 1) 
                 + ROUND(CASE WHEN type = 'DEBIT' THEN -ABS(COALESCE(amount, 0)) ELSE ABS(COALESCE(amount, 0)) END, 1)
                 - ROUND(CAST(COALESCE(vat, 0) AS FLOAT), 1) 
                 != ROUND(CAST(COALESCE(newbalance, 0) AS FLOAT), 1)
            THEN 'CALCULATION'
            
            WHEN COALESCE(paymentbalance, 0) != COALESCE(subscriptionbalance, 0)
            THEN 'BALANCE SYNC'
        END AS mismatch_type
    FROM parsed_logs
    WHERE time IS NOT NULL
      AND type IS NOT NULL;
    """

    # Execute reconciliation query
    reconcile_df = db.execute_query(query)

    # Drop old reconcile_events table and insert fresh data
    db.drop_table(table_name='reconcile_events')
    db.insert_dataframe(table_name='reconcile_events', dataframe=reconcile_df)
    db.close_connection()


if __name__ == "__main__":
    populate_reconcile_events()
