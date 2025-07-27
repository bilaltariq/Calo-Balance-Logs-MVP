import os
import re
import json
import sys
import pandas as pd
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database
from datetime import datetime
import re
import json


import re
import ast

def parse_log_string(log_string):
    """
    Parse a multiline log string into single-line log entries grouped by timestamp.
    """
    TIMESTAMP_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")

    entries = []
    current_entry = []

    for line in log_string.splitlines():
        # Start of new entry
        if TIMESTAMP_PATTERN.match(line):
            if current_entry:
                entries.append(_clean_entry(current_entry))
                current_entry = []
        current_entry.append(line)

    # Last entry
    if current_entry:
        entries.append(_clean_entry(current_entry))

    return entries


def _clean_entry(entry_lines):
    """Combine lines, remove \n and \t, collapse spaces to single line."""
    combined = " ".join(entry_lines)
    combined = re.sub(r'[\n\t]+', ' ', combined)  # replace \n and \t
    combined = re.sub(r'\s+', ' ', combined)      # collapse multiple spaces
    return combined.strip()


def filter_logs_by_keywords(logs, keywords=['START RequestId', 'Start syncing the balance', 'Subscription balance and payment balance are not in sync']):
    """
    Filter logs that contain any of the specified keywords.

    Args:
        logs (list[str]): List of log entries (single-line format).
        keywords (list[str]): Keywords to filter on.

    Returns:
        list[str]: Logs containing any of the keywords.
    """
    return [log for log in logs if any(keyword in log for keyword in keywords)]

def extract_info(logs):
    data = {
        "RequestId": None,
        "StartSyncBalance": None,
        "BalanceNotInSync": None
    }

    # Regex for RequestId
    requestid_pattern = re.compile(r"RequestId:\s*([a-f0-9-]+)")

    # Loop through logs
    for log in logs:
        # Extract RequestId
        if "RequestId" in log and data["RequestId"] is None:
            match = requestid_pattern.search(log)
            if match:
                data["RequestId"] = match.group(1)

        # Extract Start syncing balance JSON
        if "Start syncing the balance" in log:
            json_part = log.split("Start syncing the balance", 1)[-1].strip()
            try:
                # Convert pseudo-JSON (single quotes) to real Python dict
                data["StartSyncBalance"] = ast.literal_eval(json_part)
            except Exception:
                data["StartSyncBalance"] = json_part  # fallback as raw text

        # Extract Not in sync JSON
        if "Subscription balance and payment balance are not in sync" in log:
            json_part = log.split("not in sync", 1)[-1].strip()
            try:
                data["BalanceNotInSync"] = ast.literal_eval(json_part)
            except Exception:
                data["BalanceNotInSync"] = json_part  # fallback as raw text

    return data


def manual_parse(raw_str):
    # Remove outer braces if present
    s = raw_str.strip()
    s = (s.replace("None", "null")
        .replace("True", "true")
        .replace("False", "false")
        .replace("\\'", "'")        # remove escaped single quotes
        .replace("'", '"')          # use double quotes for JSON
        .replace("\\\\", "")        # remove stray backslashes
        .replace("\\\\", "")        # remove stray backslashes
    )

    colon_positions = [i for i, ch in enumerate(s) if ch == ':']
    result = {}

    for idx in colon_positions:
        # ---- Get key (scan left) ----
        j = idx - 1
        while j >= 0 and s[j] not in "{,":
            j -= 1
        key = s[j+1:idx].strip().strip("'\" ")
    
        # ---- Get value (scan right) ----
        k = idx + 1
        while k < len(s) and s[k] in " '\"":
            k += 1
        # Skip if nested dict starts here
        if k < len(s) and s[k] == '{':
            continue
        # Otherwise, capture till comma or end
        m = k
        while m < len(s) and s[m] not in ",}":
            m += 1
        value = s[k:m].strip().strip("'\" ")
    
        if key and value:
            result[key] = str(value)
    
    return result


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

        all_logs_in_list = (parse_log_string(raw_text))
        filtered_logs_in_list = filter_logs_by_keywords(all_logs_in_list)
        data = extract_info(filtered_logs_in_list)
        print(data)
        valid = manual_parse(str(data))
        print('\n')
        print(dict(valid))
        print('\n')
        print('\n')

    db.close_connection()


if __name__ == "__main__":
    parse_raw_table_to_parsed_logs()
