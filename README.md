### Balance Logs Data Pipeline – Project Plan

### 1. Objective

Develop a pipeline to process subscription balance logs, enabling:

- **ETL** from raw logs to extract meaningful insights  
- **Data Transformation** using SQL or Python  
- **Dashboarding** with Dash to visualize key metrics:  
  - Transaction **trend analysis**  
  - **Overdraft detection** to mitigate financial risks  
  - **Reconciliation** of subscription and payment balances  
  - **Anomaly detection** for proactive issue resolution  

---

### 2. High-Level Architecture - Layers

#### 1. Storage Layer
- Store structured data in **SQLite**
- Lightweight and suitable for Dash-based applications

#### 2. Ingestion Layer
- Parse AWS Lambda log files:
  - Load all files as raw strings into the database
  - Implemented in 'src/ingestion/load_raw_logs.py'
- Once raw data is available:
  - Parse file line by line, extracting only three keywords:  
    'RequestId', '"Start syncing the balance"', and  
    '"Subscription balance and payment balance are not in sync"'.
  - JSON objects are reconstructed character-by-character due to noisy raw strings
  - Parsed data is stored in database
  - Logic implemented in 'src/ingestion/parse_raw_to_parsed.py'

#### 3. Transformation Layer
- Identify discrepancies using columns:
  - Old Balance, Amount, VAT, New Balance, Payment Balance, Subscription Balance
- Two mismatch checks:
  1. (oldbalance + (amount as negative if type = DEBIT else positive) - vat) != newbalance → **CALCULATION ISSUE**
  2. paymentbalance != subscriptionbalance → **BALANCE SYNC ISSUE**
  3. Both →  **CALCULATION ISSUE + BALANCE SYNC ISSUE**

#### 4. Visualization Layer (Dash)
- Tabs:
  1. **Project Details**
  2. **Reconciliation Transactions**
  3. **Trends**
  4. **Anomaly Detection**

---

## 3. Detailed Steps (Checklist)

### Phase 1: Setup & Planning
- [X] Create project repository and folder structure on Git  
- [X] Define '.env' for DB connections (SQLite)  
- [X] Analyze provided log files  
- [X] Define 'requirements.txt' for dependencies

---

### Phase 2: Data Ingestion
- [X] Write parser for raw logs (regex/JSON parsing)  
- [X] Extract fields:
  - 'transaction_id', 'user_id', 'currency', 'old_balance', 'new_balance', 'amount', 'vat'
  - 'event_type', 'timestamp', 'RequestId'
- [X] Handle edge cases:
  - Skipped events (no sync keyword in logs?)
  - Discrepancies between subscription and payment
- [X] Save parsed data into 'parsed_logs' table

---

### Phase 3: Data Transformation
- [X] Finalize logic for mismatch transactions  
- [X] Parsing in Database; Other in Python.  

---

### Phase 4: Database Schema
- [X] Decide database technology (SQLite)  
- [X] Design ingestion and transformation tables

---

### Phase 5: Visualization (Dash App)
- [X] Create Dash skeleton with multi-tab layout  
- [X] **Transaction Trends**
- [X] **Overdraft Detection**
- [X] **Reconciliation**
- [X] **Anomaly Detection**

---

### Phase 6: Testing & Validation
- [X] Validate parser on multiple log samples  
- [X] Cross-check derived balances vs raw logs  
- [X] Ensure dashboard filters and charts are responsive  

---

### Phase 7: Deployment
- [X] Containerize app with Docker  

---

### Future Work
- Schedule pipeline (Airflow)
- Generate daily overdraft report (CSV)
- Generate weekly reconciliation summary

---

## 4. Deliverables

- Python ingestion and transformation scripts
- Dash web app
