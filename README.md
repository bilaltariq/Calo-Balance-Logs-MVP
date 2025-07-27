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

1. **Ingestion Layer**
   - Parse AWS Lambda log files (CloudWatch exports).
   - Extract structured fields (transaction, skipped, discrepancy events).
   - Handle incremental ingestion for new logs.

2. **Transformation Layer**
   - Normalize to relational schema (`balance_events`).
   - Derived tables:
     - `user_balances` (latest balance per user).
     - `discrepancy_report` (payment vs subscription mismatches).
     - `anomalies` (unusual transaction patterns).

3. **Storage Layer**
   - Store structured data in **SQLite**.

4. **Visualization Layer (Dash)**
   - Tabs:
     1. **Transaction Trends**
     2. **Overdraft Detection**
     3. **Reconciliation**
     4. **Anomaly Detection**
     5. **Skipped Events / System Health**

5. **Automation Layer (Optional)**
   - Scheduled ingestion (e.g., cron job or Airflow).
   - Email/Slack alerts for overdrafts or discrepancies.

---

## 3. Detailed Steps (Checklist)

### **Phase 1: Setup & Planning**
- [X] Create project repository and folder structure.
- [X] Define `.env` for DB connections (Postgres/SQLite).
- [X] Collect sample log files from CloudWatch exports.
- [X] Define requirements.txt (Dash, Pandas, SQLAlchemy, etc.).

---

### **Phase 2: Data Ingestion**
- [X] Write parser for raw logs (regex or JSON parsing).
- [X] Extract fields:
  - `transaction_id`, `user_id`, `currency`, `old_balance`, `new_balance`, `amount`, `vat`
  - `event_type` (processed, skipped, discrepancy)
  - `timestamp`, `message_id`, `RequestId`
- [X] Handle edge cases:
  - Skipped events (no userId).
  - Discrepancies (subscription vs payment).
  - Duplicate events.
- [X] Save parsed data into `parsed_logs` table.

---

### **Phase 3: Data Transformation**
- [ ] Create derived field `net_change = new_balance - old_balance`.
- [ ] Flag overdrafts (`new_balance < 0`).
- [ ] Build `reconcile_events` table (latest balance per user).
- [ ] Build `discrepancy_report` table (subscription vs payment mismatch).
- [ ] Build `anomalies` table:
  - Large transaction spikes.
  - Frequent discrepancies for same user.
  - VAT mismatches.

---

### **Phase 4: Database Schema**
- [ ] Design relational schema for:
  - `balance_events` (raw + parsed events).
  - `user_balances` (aggregated).
  - `discrepancy_report` (for reconciliation tab).
  - `anomalies` (for anomaly tab).
- [ ] Implement schema using SQLAlchemy / raw SQL DDL.

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
- [ ] **Tab 4: Anomaly Detection**
  - Highlight large changes or suspicious patterns.
- [ ] **Tab 5: Skipped Events/System Health**
  - Count of skipped events.
  - Cold start durations over time.

---

### **Phase 6: Automation & Reporting**
- [ ] Schedule pipeline (Airflow).
- [ ] Generate daily overdraft report (CSV).
- [ ] Generate weekly reconciliation summary.

---

### **Phase 7: Testing & Validation**
- [ ] Validate parser on multiple log samples.
- [ ] Cross-check derived balances vs raw logs.
- [ ] Test overdraft detection with edge cases.
- [ ] Ensure dashboard filters & charts are responsive.

---

### **Phase 8: Deployment**
- [ ] Containerize with Docker.

---

## 5. Deliverables
- Python ingestion + transformation scripts.
- Normalized database (SQLite/Postgres).
- Dash multi-tab web app.