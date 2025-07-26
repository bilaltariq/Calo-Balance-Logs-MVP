import os
import re
import json
import sys
import pandas as pd
from datetime import datetime

# Ensure project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database
from datetime import datetime

import re
import json

def normalize_json_string(text: str) -> str:
    try:
        json.loads(text)  # Validate
        return text
    except json.JSONDecodeError:
        pass  # Continue cleaning

    text = re.sub(r'(\b\w+\b)\s*:', r'"\1":', text)

    text = re.sub(r"(?<!\\)'", '"', text)

    text = re.sub(
        r'"(\d{4}-\d{2})-"(\d{2})T(\d{2})":"(\d{2})":(\d{2}\.\d+Z)"',
        r'"\1-\2T\3:\4:\5"',
        text
    )

    def fix_notes_quotes(match):
        value = match.group(1)
        safe_value = value.replace('"', '\\"')
        return f'"notes":"{safe_value}"'

    text = re.sub(r'"notes":"(.*?)"', fix_notes_quotes, text, flags=re.DOTALL)

    try:
        json.loads(text)
    except json.JSONDecodeError as e:
        pass
        #raise ValueError(f"Invalid JSON even after cleanup: {e}\n{text}")

    return text


def extract_json_objects(text, filename):
    segments = re.split(r"INFO\s+Start syncing the balance", text)
    merged_results = []

    for segment in segments:
        if not segment.strip():
            continue

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

        for raw in results:
            normalized = re.sub(r"(?<!\\)'", '"', raw)
            normalized = normalize_json_string(normalized)
            
            try:
                parsed = json.loads(normalized)
                if "transaction" in parsed and isinstance(parsed["transaction"], dict):
                    tx = parsed.pop("transaction")

                    for k, v in tx.items():
                        merged_row[f"transaction_{k}"] = v

                merged_row.update(parsed)
            except Exception as e:
                print(str(filename))

        error_match = re.search(r"ERROR\s+(.*?)\{", segment)
        if error_match:
            merged_row["error_message"] = error_match.group(1).strip()

        if not merged_row or (
            not any(k.startswith("transaction_") for k in merged_row)
            and "userId" not in merged_row
        ):
            continue

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

        filelist = ['2023-12-12-[$LATEST]2d79cb3e8a764a7ebe511af6e51c3f65']
        #if len(raw_text) > 0:
        if filename in filelist:
            # Extract transactions
            try:
                transactions = extract_json_objects(raw_text, filename)
                print(transactions)
                #print(filename)
                exit(0)
            except Exception as e:
                #pass
                print(filename +  ' ' + str(e))
                exit(1)
            #print(transactions)
        
        
    #         db.delete_rows("parsed_logs", "filename = ?", (filename,))

    #         # Prepare rows for insertion
    #         for tx in transactions:
    #             tx["filename"] = filename
    #             tx["parsed_at"] = datetime.utcnow().isoformat()
    #             parsed_rows.append(tx)

        

    # # Insert new parsed rows
    # if parsed_rows:
    #     db.insert_rows_dynamic("parsed_logs", parsed_rows)
    # else:
    #     print("No JSON objects found in raw_data logs.")

    db.close_connection()


if __name__ == "__main__":
    parse_raw_table_to_parsed_logs()
