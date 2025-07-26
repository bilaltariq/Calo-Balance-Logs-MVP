import os
import re
import json
import sqlite3
import sys
import pandas as pd
from datetime import datetime

# Ensure project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database
from datetime import datetime


import json
import re
import csv
import os

ERROR_LOG_FILE = "failed_parses.csv"
NO_LOGS_FILE = "no_transactions.csv"
ERROR_LOGS_WHILE_DB_INSERT = "db_insert_errors.csv"


def error_while_insert(filename: str, raw_snippet: str):
    """Append failed JSON parse details to a CSV."""
    file_exists = os.path.exists(ERROR_LOGS_WHILE_DB_INSERT)
    with open(ERROR_LOGS_WHILE_DB_INSERT, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["filename", "raw_snippet"])
        writer.writerow([filename, raw_snippet])


def no_logs_records(filename: str):
    """Append failed JSON parse details to a CSV."""
    file_exists = os.path.exists(NO_LOGS_FILE)
    with open(NO_LOGS_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["filename"])
        writer.writerow([filename])  # Limit snippet length



def log_failed_parse(filename: str, raw_snippet: str):
    """Append failed JSON parse details to a CSV."""
    file_exists = os.path.exists(ERROR_LOG_FILE)
    with open(ERROR_LOG_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["filename", "raw_snippet"])
        writer.writerow([filename, raw_snippet[:500]])  # Limit snippet length


def sanitize_row(row):
    clean = {}
    for k, v in row.items():
        # Skip nested dicts/lists
        if isinstance(v, (dict, list)):
            continue
        # Convert booleans to int (True → 1, False → 0)
        if isinstance(v, (float,int)):
            v = str(v)

        if isinstance(v, bool):
            v = "1" if v else "0"

        clean[k] = v
    return clean



def normalize_json_string(text: str) -> str:
    """
    Clean malformed JSON-like strings:
    - Quote unquoted keys
    - Replace single with double quotes
    - Fix malformed datetime patterns
    - Escape notes fields properly
    """
    # Quote unquoted keys
    text = re.sub(r'(\b\w+\b)\s*:', r'"\1":', text)

    # Replace single quotes with double quotes
    text = re.sub(r"(?<!\\)'", '"', text)

    # Fix datetime patterns if broken
    text = re.sub(
        r'"(\d{4}-\d{2})-"(\d{2})T(\d{2})":"(\d{2})":(\d{2}\.\d+Z)"',
        r'"\1-\2T\3:\4:\5"',
        text
    )

    # Escape quotes inside "notes"
    def fix_notes_quotes(match):
        value = match.group(1)
        safe_value = value.replace('"', '\\"')
        return f'"notes":"{safe_value}"'

    text = re.sub(r'"notes":"(.*?)"', fix_notes_quotes, text, flags=re.DOTALL)

    return text

def flatten_dict(prefix: str, value: dict) -> dict:
    """
    Recursively flatten dict fields into key_prefix_subkey format.
    Skip lists entirely.
    """
    flat = {}
    for k, v in value.items():
        new_key = f"{prefix}_{k}" if prefix else k
        if isinstance(v, dict):
            flat.update(flatten_dict(new_key, v))
        elif isinstance(v, list):
            # Skip lists entirely (do not insert into DB)
            continue
        else:
            flat[new_key] = v
    return flat


def extract_json_objects(text: str, filename: str):
    """
    Parse logs into rows per sync cycle:
    - Split at 'Start syncing the balance'
    - Normalize malformed JSONs
    - Flatten all dict fields (transaction + other dicts)
    - Log failures into CSV for later review
    """

    segments = re.split(r"INFO\s+Start syncing the balance", text)
    merged_results = []

    for segment in segments:
        if not segment.strip():
            continue

        # Extract JSON objects using brace matching
        results = []
        brace_stack = 0
        current = []

        for char in segment:
            if char == '{':
                brace_stack += 1
            if brace_stack > 0:
                current.append(char)
            if char == '}':
                brace_stack -= 1
                if brace_stack == 0 and current:
                    results.append(''.join(current))
                    current = []

        merged_row = {}

        # Parse each JSON block
        for raw in results:
            normalized = normalize_json_string(raw)
            try:
                parsed = json.loads(normalized)

                # --- Flatten transaction object first ---
                if "transaction" in parsed and isinstance(parsed["transaction"], dict):
                    tx = parsed.pop("transaction")
                    merged_row.update(flatten_dict("transaction", tx))

                # --- Flatten all remaining dict fields ---
                extra_flat = {}
                for key, value in parsed.items():
                    if isinstance(value, dict):
                        extra_flat.update(flatten_dict(key, value))
                    else:
                        extra_flat[key] = value

                merged_row.update(extra_flat)

            except json.JSONDecodeError:
                log_failed_parse(filename, raw)
                continue

        # Extract error message (if exists)
        error_match = re.search(r"ERROR\s+(.*?)\{", segment)
        if error_match:
            merged_row["error_message"] = error_match.group(1).strip()

        # Skip meaningless rows
        if not merged_row or (
            not any(k.startswith("transaction_") for k in merged_row)
            and "userId" not in merged_row
        ):
            continue

        # Determine sync status
        if "subscriptionBalance" in merged_row and "paymentBalance" in merged_row:
            merged_row["sync_status"] = "FAILED"
        else:
            merged_row["sync_status"] = "SUCCESS"

        merged_results.append(merged_row)

    return merged_results



def parse_raw_table_to_parsed_logs():
    """
    Parse raw logs from 'raw_data' table into structured 'parsed_logs' table.
    Adds new columns dynamically if JSON contains unseen keys.
    One row = one transaction (merged JSON object).
    If file already parsed, old rows are deleted and replaced with new ones.
    """
    db = Database()
    db.connect()

    # Ensure parsed_logs table exists with minimal schema
    db.create_table("parsed_logs", {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "filename": "TEXT",
        "parsed_at": "TEXT"
    })

    # Load raw_data table
    raw_df = db.select_table("raw_data")
    if raw_df.empty:
        print("No raw logs found in raw_data table.")
        db.close_connection()
        return

    parsed_rows = []

    for _, row in raw_df.iterrows():
        raw_text = row["raw_string"]
        filename = row.get("filename", None)

        # Skip unwanted files
        if filename in ['.DS_Store', '000000.gz'] or "Start syncing the balance" not in raw_text:
            continue

        # # Debug: process only specific file for testing
        # filelist = ['2023-12-24-[$LATEST]faad1fc6381f4c409465ec06cdc8c426',
        #             '2023-12-23-[$LATEST]c4330be306f14007baaaea1aa8f3fd5a',
        #             '2024-01-02-[$LATEST]2c6cb52879314d37bd0719a1faa88fea',
        #             '2024-04-02-[$LATEST]307a7bb8a3654729a001a516e53a8ef1'
        #             ]#'2024-04-01-[$LATEST]85c10dcd117f402199111b91b204fa5c']

        # #filelist = ['2024-04-02-[$LATEST]307a7bb8a3654729a001a516e53a8ef1']
        # if filename in filelist:
            # Extract transactions
        transactions = extract_json_objects(raw_text, filename)

        if len(transactions) ==0:
            no_logs_records(filename)


        db.delete_rows("parsed_logs", "filename = ?", (filename,))

        # Prepare rows for insertion
        for tx in transactions:
            tx.pop("_aws", None)  # drop CloudWatch metrics
            if "metadata" in tx and isinstance(tx["metadata"], dict):
                    tx["metadata"] = json.dumps(tx["metadata"])
            


            tx["filename"] = filename
            tx["parsed_at"] = datetime.utcnow().isoformat()
            clean_tx = sanitize_row(tx)

            parsed_rows.append(clean_tx)

        


    for row in parsed_rows:
        try:
            #sanitized = sanitize_row(row)
            db.insert_rows_dynamic("parsed_logs", [row])
            
        except sqlite3.IntegrityError as e:
            print(f"\n IntegrityError in row from file: {row.get('filename')}")
            # print(f"Row content: {json.dumps(row, indent=2)}")
            # print(f"Error: {e}\n")
            error_while_insert(filename, row)

    # Insert new parsed rows
    # parsed_rows = [sanitize_row(r) for r in parsed_rows]

    # if parsed_rows:
    #     db.insert_rows_dynamic("parsed_logs", parsed_rows)
    # else:
    #     print("No JSON objects found in raw_data logs.")

    db.close_connection()


if __name__ == "__main__":
    parse_raw_table_to_parsed_logs()
