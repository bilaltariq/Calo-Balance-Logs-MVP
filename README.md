# Balance Logs Data Pipeline – Project Plan

## 1. Objective
Build a pipeline to process subscription balance logs for:
- **Trend analysis** of transactions.
- **Overdraft detection** to prevent financial losses.
- **Reconciliation** between subscription and payment balances.
- **Anomaly detection**.
- Visualization via **Dash** for accounting & engineering teams.

---

## 2. High-Level Architecture

### **Layers**

1. **Storage Layer**
   - Store structured data in **SQLite**.
   - It is lite and works fine with Dash applications.


2. **Ingestion Layer**
   - Parse AWS Lambda log files: Done via first loading all files in database as raw string. 
      - This process is implemented in src/ingestion/load_raw_logs.py

   - Once the raw data is available; I have parse the file one by one with the following logic.
      - We only need three keywords from each logfile: RequestId, "Start syncing the balance" and "Subscription balance and payment balance are not in sync".
      - JSON objects are attached to both "Start syncing the balance" and "Subscription balance and payment balance are not in sync"
      - But since these are strings and have alot of noise, I have create a custom method to re-create JSON by reading json char by char. 
      - Once a proper JSON is create, we insert the data in table.
      - All this logic is implemented in src/ingestion/parse_raw_to_parsed.py

3. **Transformation Layer**
   - In order to find Disprecencies in records I have created two mis match types. We have following columns in our ingested tables. Logic for mismatch column is
   created based on following columns and my assumed definations.
    1. Old Balance: User's old balance in app.
    2. Amount: The Transaction amout.
    3. VAT: Tax
    4. New Balance: The balance after transaction finished.
    5. Payment Balance: User new balance after transaction.
    6. Subscription Balance: Balance as per subscription of user (monthly or yearly).

    Two mismatch types.
    1. Old Balance + Amount - VAT = New Balance
    If this is not the case then we label this trasnaction as CALCULATION error.
    2. Payment Balance == Subscription Balance
    If this is not equal then we label this as BALANCE SYNC error. 


4. **Visualization Layer (Dash)**
   - Tabs:
     1. **Project Details**
     2. **Reconciliation Transactions**
     3. **Trends**
     4. **Anomaly Detection**
---

5. Detailed Steps (Checklist which I aim at the start of project)

### **Phase 1: Setup & Planning**
- [X] Create project repository and folder structure on GIT.
- [X] Define `.env` for DB connections (SQLite).
- [X] Understand log files provided.
- [X] Define requirements.txt that will be used for app.

---

### **Phase 2: Data Ingestion**
- [X] Write parser for raw logs (regex or JSON parsing).
- [X] Extract fields:
  - `transaction_id`, `user_id`, `currency`, `old_balance`, `new_balance`, `amount`, `vat`
  - `event_type` `timestamp`, `message_id`, `RequestId`
- [X] Handle edge cases:
  - Skipped events (no sync keyword in Logs?).
  - Discrepancies (subscription vs payment).
- [X] Save parsed data into `parsed_logs` table.

---

### **Phase 3: Data Transformation**
- [ ] Understand data and try to finalize logic for mismatch transaction.
- [ ] Build `reconcile_events` table (latest balance per user).
- [ ] Build `discrepancy_report` table (subscription vs payment mismatch).
- [ ] Build `anomalies` table:
  - Large transaction spikes.
  - Frequent discrepancies for same user.
  - VAT mismatches.

--- 

### **Phase 4: Database Schema**
- [X] Which datbase to use
- [X] Tables to create for ingestion and transformation.

---

### **Phase 5: Visualization (Dash App)**
- [X] Create Dash skeleton with multi-tab layout.
- [ ] **Tab 1: Transaction Trends**
  - Line chart: User balance history.
  - Aggregate credit vs debit chart.
- [ ] **Tab 2: Overdraft Detection**
  - Table of users with negative balances.
  - Filter by date/currency.
- [X] **Tab 3: Reconciliation**
  - Table of mismatched balances.
  - Export to CSV button.
- [X] **Tab 4: Anomaly Detection**
  - Highlight large changes or suspicious patterns.
- [ ] **Tab 5: Skipped Events/System Health**
  - Count of skipped events.
  - Cold start durations over time.

---

### **Phase 6: Testing & Validation**
- [X] Validate parser on multiple log samples.
- [X] Cross-check derived balances vs raw logs.
- [X] Ensure dashboard filters & charts are responsive.

### **Phase 8: Deployment**
- [X] Containerize app with Docker.


### **Future Work**
- Schedule pipeline (Airflow).
- Generate daily overdraft report (CSV).
- Generate weekly reconciliation summary.

---

## 5. Deliverables
- Python ingestion + transformation scripts.
- Dash web app.