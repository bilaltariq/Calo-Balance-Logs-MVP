#!/bin/bash
set -e  # Exit on error

echo "Running database initialization scripts..."

python src/storage/init_db.py #initialize db
python src/ingestion/load_raw_logs.py #load raw logs to sqlite3 db
python src/ingestion/parse_raw_to_parsed.py  # clean and transformation
python src/ingestion/reconcile_events.py  # transformed table for dashboard

echo "Starting Dash app..."
exec gunicorn -b 0.0.0.0:8050 app:server
