import os
import re
import sys
from datetime import datetime
import ast
import re
import ast
from datetime import datetime
from collections import defaultdict


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database


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

def extract_info_dep(logs):
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
            timestamp_str = log.split()[0]
            dt = datetime.strptime(timestamp_str[:10], "%Y-%m-%d").date()
            json_part = log.split("Start syncing the balance", 1)[-1].strip()
            try:
                data["StartSyncBalance"] = ast.literal_eval(json_part)
            except Exception:
                data["StartSyncBalance"] = json_part
            data['startsynctime'] = str(dt)

        if "Subscription balance and payment balance are not in sync" in log:
            json_part = log.split("not in sync", 1)[-1].strip()
            #print(json_part)
            try:
                data["BalanceNotInSync"] = ast.literal_eval(json_part)
            except Exception:
                data["BalanceNotInSync"] = json_part  # fallback as raw text
    return data

def extract_info(logs):
    requestid_pattern = re.compile(r"RequestId:\s*([a-f0-9-]+)")
    id_inline_pattern = re.compile(r"\b([a-f0-9-]{36})\b")

    grouped_data = defaultdict(lambda: {
        "RequestId": None,
        "StartSyncBalance": [],
        "BalanceNotInSync": []
    })

    current_request_id = None

    for log in logs:
        match = requestid_pattern.search(log)

        if match:
            current_request_id = match.group(1)
            if grouped_data[current_request_id]["RequestId"] is None:
                grouped_data[current_request_id]["RequestId"] = current_request_id
            continue

        if not current_request_id:
            inline_match = id_inline_pattern.search(log)
            if inline_match:
                current_request_id = inline_match.group(1)
                if grouped_data[current_request_id]["RequestId"] is None:
                    grouped_data[current_request_id]["RequestId"] = current_request_id

        # Skip if still no RequestId
        if not current_request_id:
            continue

        # --- Extract StartSyncBalance ---
        if "Start syncing the balance" in log:
            timestamp_str = log.split()[0]
            dt = datetime.strptime(timestamp_str[:10], "%Y-%m-%d").date()
            json_part = log.split("Start syncing the balance", 1)[-1].strip()

            try:
                parsed_data = ast.literal_eval(json_part)
                parsed_data['is_start_balance_sync'] = 1
            except Exception:
                parsed_data = json_part

            grouped_data[current_request_id]["StartSyncBalance"].append({
                "time": str(dt),
                "data": parsed_data
            })


        elif "Subscription balance and payment balance are not in sync" in log:
            json_part = log.split("not in sync", 1)[-1].strip()

            try:
                parsed_data = ast.literal_eval(json_part)
                #parsed_data['is_balance_not_sync'] = 1
            except Exception:
                parsed_data = json_part

            grouped_data[current_request_id]["BalanceNotInSync"].append(parsed_data)

    return list(grouped_data.values())


def find_metadata_bounds(raw_str: str):
    """
    Returns (start_index, end_index) of metadata JSON inside raw_str.
    If not found, returns (None, None).
    """
    # Clean string similar to manual_parse
    s = (raw_str.strip()
        .replace("None", "null")
        .replace("True", "true")
        .replace("False", "false")
        .replace("\\'", "'")
        .replace("'", '"')
        .replace("\\\\", "")
    )

    # Locate 'metadata'
    meta_pos = s.find('metadata')
    if meta_pos == -1:
        return None, None

    # Find first '{' after 'metadata'
    start = s.find("{", meta_pos)
    if start == -1:
        return None, None

    # Match braces to find end
    brace_count = 0
    for i in range(start, len(s)):
        if s[i] == '{':
            brace_count += 1
        elif s[i] == '}':
            brace_count -= 1
            if brace_count == 0:
                return start, i+1  # return start and end index

    return None, None


def manual_parse(raw_str):
    resultList = []
    for r in raw_str:
        # Remove outer braces if present
        s = str(r).strip()
        s = (s.replace("None", "null")
            .replace("True", "true")
            .replace("False", "false")
            .replace("[", "")
            .replace("\\'", "'")        # remove escaped single quotes
            .replace("'", '"')          # use double quotes for JSON
            .replace("\\\\", "")        # remove stray backslashes
        )

        start_meta, end_meta = find_metadata_bounds(s)

        is_meta = True
        if start_meta is None or end_meta is None:
            is_meta = False

        colon_positions = [i for i, ch in enumerate(s) if ch == ':']
        result = {}

        for idx in colon_positions:
            if is_meta:
                if idx > start_meta and idx < end_meta:
                    continue

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
                result[key] = value

        resultList.append(result)
    return resultList 



def parse_raw_table_to_parsed_logs():
    """
    Parse raw logs from 'raw_data' table into structured 'parsed_logs' table.

    - Creates table if not exists (keeps history)
    - Deletes existing rows per file before re-parsing
    - Dynamically adds new columns as needed
    - Batch insert for performance
    """
    with Database() as db:
        # Ensure parsed_logs table exists
        db.ensure_table("parsed_logs", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "filename": "TEXT",
            "parsed_at": "TEXT"
        })

        # Load raw_data
        raw_df = db.select_table("raw_data")
        if raw_df.empty:
            print("No raw logs found in raw_data table.")
            return

        for _, row in raw_df.iterrows():
            raw_text = row["raw_string"]
            filename = row.get("filename")

            # Skip unwanted files
            if filename in ['.DS_Store', '000000.gz'] or "Start syncing the balance" not in raw_text:
                continue

            # Parse raw string -> list of transaction dicts
            all_logs_in_list = parse_log_string(raw_text)
            filtered_logs_in_list = filter_logs_by_keywords(all_logs_in_list)
            data = extract_info(filtered_logs_in_list)
            all_transactions = manual_parse(data)

            # Remove previous parsed rows for this file
            db.delete_rows("parsed_logs", "filename = ?", (filename,))

            # Prepare batch insert
            batch = []
            for tx in all_transactions:
                tx = dict(tx)
                tx["transaction_id"] = tx.pop("id", None)
                tx["filename"] = filename
                tx["parsed_at"] = datetime.utcnow().isoformat()

                # Convert all keys/values to string for SQLite
                tx = {str(k): str(v) for k, v in tx.items()}
                batch.append(tx)

            if batch:
                db.insert_rows_dynamic("parsed_logs", batch)

            print(f"{filename}: Inserted {len(batch)} transactions")


# def parse_raw_table_to_parsed_logs():
#     """
#     Parse raw logs from 'raw_data' table into structured 'parsed_logs' table.
#     Adds new columns dynamically if JSON contains unseen keys.
#     One row = one transaction (merged JSON object).
#     If file already parsed, old rows are deleted and replaced with new ones.
#     """
#     db = Database()
#     db.connect()

#     tbl_name = "parsed_logs"
#     db.drop_table(table_name=tbl_name)
#     db.create_table(tbl_name, {
#         "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
#         "filename": "TEXT",
#         "parsed_at": "TEXT"
#     })

#     # Load raw_data table
#     raw_df = db.select_table("raw_data")
#     if raw_df.empty:
#         print("No raw logs found in raw_data table.")
#         db.close_connection()
#         return

#     for _, row in raw_df.iterrows():
#         raw_text = row["raw_string"]
#         filename = row.get("filename", None)


#         # Skip unwanted files
#         if filename in ['.DS_Store', '000000.gz'] or "Start syncing the balance" not in raw_text:
#             continue

#         all_logs_in_list = (parse_log_string(raw_text))

#         filtered_logs_in_list = filter_logs_by_keywords(all_logs_in_list)

#         data = extract_info(filtered_logs_in_list)
        
#         all_transactions = manual_parse(data)

#         db.delete_rows("parsed_logs", "filename = ?", (filename,))

#         transaction_count = 0
#         for transactions in all_transactions:
#             transactions = dict(transactions)
#             transactions["transaction_id"] = transactions.pop("id", None)

#             transactions["filename"] = filename
#             transactions["parsed_at"] = datetime.now()

#             try:
#                 transactions = {str(k): str(v) for k, v in transactions.items()}
#                 db.insert_rows_dynamic(tbl_name, [transactions])
#                 transaction_count+=1

#             except Exception as db_err:
#                 continue
#         print(filename + ": " + str(transaction_count))
#     db.close_connection()


if __name__ == "__main__":
    parse_raw_table_to_parsed_logs()
