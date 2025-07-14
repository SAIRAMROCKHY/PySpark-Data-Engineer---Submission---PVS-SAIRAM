# PySpark Data Engineer Assignment 

## 📌 Submission by: *PVS SAIRAM*

---

## 🚀 Project Overview

This project simulates real-time transactional data processing using **PySpark on Databricks**, with streaming ingestion from **Google Drive to ADLS Gen 2**, detection of actionable **customer insights**, and secure output delivery via **ADLS Gen 2**.

The system is divided into:

- **Mechanism X**: A batch streaming simulator that reads 10,000 rows every second from `/transactions.csv` in Google Drive and uploads the chunk to an ADLS container.
- **Mechanism Y**: A streaming detection engine that listens to this ADLS stream, applies 3 business patterns, and outputs detection results (50 at a time) as zipped files to S3.

---

## 🧩 Patterns Detected

### ✅ `PatId1: UPGRADE`
- Top 10% customers (txn count) for a merchant
- Avg. weight in bottom 10%
- Merchant has more than 50,000 transactions
- ➡️ ActionType: `UPGRADE`

### ✅ `PatId2: CHILD`
- Customer's avg. txn value < ₹23
- At least 80 transactions
- ➡️ ActionType: `CHILD`

### ✅ `PatId3: DEI-NEEDED`
- Female customers < Male customers for merchant
- At least 100 Female customers
- ➡️ ActionType: `DEI-NEEDED`

---

## 🧱 Architecture Diagram

![Mechanism X Architecture](https://github.com/SAIRAMROCKHY/PySpark-Data-Engineer---Submission---PVS-SAIRAM/blob/main/Mechanism_X.png)

![Mechanism Y Architecture](https://github.com/SAIRAMROCKHY/PySpark-Data-Engineer---Submission---PVS-SAIRAM/blob/main/Mechanism_Y.png)

