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
        COALESCE(transformed_type, 'UNKNOWN') AS type,
        filename,
        RequestId,
        transaction_id,
        userId as user_id,
        time as timestamp,
        oldBalance as old_balance,
        amount,
        vat,
        newBalance as new_balance,
        CASE 
            WHEN newBalance < 0 THEN 1
            ELSE 0
        END AS is_overdraft,
        ROUND(
            oldbalance 
            + (CASE WHEN transformed_type = 'DEBIT' THEN -ABS(amount) ELSE ABS(amount) END) 
            - vat,
            2
        ) AS expected_new_balance,
        CASE 
            WHEN ROUND(
                    oldbalance 
                    + (CASE WHEN transformed_type = 'DEBIT' THEN -ABS(amount) ELSE ABS(amount) END) 
                    - vat 
                    - newbalance,
                    0
                ) != 0 
            THEN 'CALCULATION ISSUE'
            WHEN paymentBalance != subscriptionBalance 
            THEN 'BALANCE SYNC ISSUE'
            WHEN (paymentBalance != subscriptionBalance) 
                AND ROUND(
                        oldbalance 
                        + (CASE WHEN transformed_type = 'DEBIT' THEN -ABS(amount) ELSE ABS(amount) END) 
                        - vat 
                        - newbalance,
                        0
                    ) != 0 
            THEN 'CALCULATION + BALANCE SYNC ISSUE'
            ELSE 'NO FOUND ISSUE'
        END AS mismatch_type,
        paymentBalance,
        subscriptionBalance,
        source,
        action,
        country

    FROM (
        SELECT 
            type,
            CASE 
                WHEN type IS NULL 
                    AND (oldBalance + amount - vat = newBalance) 
                THEN 'CREDIT'
                WHEN type IS NULL 
                    AND (oldBalance - amount - vat = newBalance) 
                THEN 'DEBIT'
                ELSE type 
            END AS transformed_type,
            filename,
            RequestId,
            transaction_id,
            userId,
            time,
            oldBalance,
            amount,
            vat,
            newBalance,
            paymentBalance,
            subscriptionBalance,
            source,
            action,
            country

        FROM (
            SELECT 
                type,
                COALESCE(ROUND(oldBalance, 2), 0) AS oldBalance,
                COALESCE(ROUND(amount, 2), 0) AS amount,
                COALESCE(ROUND(vat, 2), 0) AS vat,
                COALESCE(ROUND(newBalance, 2), 0) AS newBalance,
                COALESCE(ROUND(paymentBalance, 2), 0) AS paymentBalance,
                COALESCE(ROUND(subscriptionBalance, 2), 0) AS subscriptionBalance,
                filename,
                RequestId,
                transaction_id,
                userId,
                time,
                source,
                action,
                CASE currency
                    WHEN 'SAR' THEN 'Saudi Arabia'
                    WHEN 'BHD' THEN 'Bahrain'
                    WHEN 'AED' THEN 'United Arab Emirates'
                    WHEN 'KWD' THEN 'Kuwait'
                    WHEN 'OMR' THEN 'Oman'
                    ELSE 'Unknown'
                END AS country

            FROM parsed_logs
            WHERE time IS NOT NULL
        ) base
    ) type_base;
    """

    # Execute reconciliation query
    reconcile_df = db.execute_query(query)

    # Drop old reconcile_events table and insert fresh data
    db.drop_table(table_name='reconcile_events')
    db.insert_dataframe(table_name='reconcile_events', dataframe=reconcile_df)
    db.close_connection()


if __name__ == "__main__":
    populate_reconcile_events()
