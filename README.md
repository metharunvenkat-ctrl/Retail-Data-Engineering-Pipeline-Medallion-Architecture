# 🛒 Retail Data Engineering Pipeline: Medallion Architecture

An end-to-end Big Data Engineering pipeline built with **Apache Spark (PySpark)** and **Databricks Delta Lake** implementing the **Medallion Architecture** (Bronze ➔ Silver ➔ Gold/Optimized Tables) for retail sales data analysis.

---

## 📌 Project Overview

Retail organizations generate vast amounts of transactional data daily across multiple store locations. To translate raw, messy sales data into business intelligence, this pipeline ingests raw sales records, cleans corrupt or invalid entries, deduplicates records, validates core business rules, creates aggregated summary data marts, optimizes Delta storage layouts via **Partitioning** and **Z-Ordering**, and performs SQL-based analytics and data visualizations.

---

## 🏗️ Architectural Blueprint: Medallion Architecture

```
                               ┌────────────────────────────────┐
                               │  Raw Retail Sales Data (CSV)   │
                               └───────────────┬────────────────┘
                                               │
                                               ▼
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │  🥉 BRONZE LAYER: Data Ingestion                                                       │
  │  • Explicit string schema to safely capture all incoming raw data (including corrupt)  │
  └────────────────────────────────────────────┬───────────────────────────────────────────┘
                                               │
                                               ▼
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │  🥈 SILVER LAYER: Data Transformation & Data Quality Enforcement                       │
  │  • Whitespace trimming & schema casting                                                │
  │  • Null / Corrupted row removal (`TransactionID`, `ProductID`, `StoreID`, etc.)       │
  │  • Deduplication by `TransactionID`                                                   │
  │  • Business logic validation (`QuantitySold > 0`, `SalesAmount > 0`, valid timestamps)   │
  │  • Date standardization (`yyyy-MM-dd`)                                                 │
  │  • Transformed outputs stored as Delta Tables (`Daily_Sales_Summary`, `Top_Products`) │
  └────────────────────────────────────────────┬───────────────────────────────────────────┘
                                               │
                                               ▼
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │  🥇 GOLD / STORAGE OPTIMIZATION LAYER                                                  │
  │  • Partitioning: Partitioned by `StoreID` for efficient store-level querying           │
  │  • Multi-Dimensional Clustering: Delta Z-ORDER by `TransactionDate` & `ProductID`      │
  │  • Optimized file layout for fast temporal and product analytical slicing              │
  └────────────────────────────────────────────┬───────────────────────────────────────────┘
                                               │
                                               ▼
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │  📊 ANALYTICS & VISUALIZATION                                                          │
  │  • SQL analysis for top-performing stores & products                                  │
  │  • Peak & lowest sales date analysis per store                                         │
  │  • Interactive Databricks visualizations (Bar Charts & 7-Day Trend Lines)             │
  └────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Key Pipeline Stages

### 1. Data Ingestion (Bronze Layer)
- Ingests raw sales transaction data containing `TransactionID`, `ProductID`, `StoreID`, `QuantitySold`, `SalesAmount`, and `TransactionDate`.
- Defines an explicit, permissive string-based schema (`StructType`) during Bronze ingestion to ensure raw records are captured cleanly without pipeline failures caused by malformed input.

### 2. Data Cleaning & Transformation (Silver Layer)
- **Null & Corrupt Record Removal**: Filters out invalid or incomplete records across all mandatory columns.
- **Deduplication**: Eliminates duplicate `TransactionID` instances via `dropDuplicates(["TransactionID"])`.
- **Business Rule Enforcement**:
  - `QuantitySold > 0`
  - `SalesAmount > 0`
  - `TransactionDate` is valid and parseable as a timestamp.
- **Analytical Transformations**:
  - Standardizes `TransactionDate` into `yyyy-MM-dd` format.
  - Aggregates total daily sales per store (`df_daily_sales`).
  - Identifies top-sold products per store using Spark Window functions (`Window.partitionBy("StoreID").orderBy(F.desc("TotalQuantity"))`).
  - Filters stores exceeding $300 in average daily sales.

### 3. Data Storage & Query Optimization
- **Partitioning**: Writes transformed Delta tables partitioned by `StoreID` to isolate data by store location and minimize scan range.
- **Z-Ordering**: Executes Delta Lake optimization commands:
  ```sql
  OPTIMIZE default.Daily_Sales_Summary ZORDER BY (TransactionDate);
  OPTIMIZE default.Top_Products_By_Store ZORDER BY (ProductID);
  ```
  Z-Ordering co-locates related data within Delta files, drastically improving performance for temporal queries and product-specific filtering.

### 4. Data Analysis & Insights (SQL)
- **Top-Performing Store**: Identifies total revenue by store to highlight peak retail locations.
- **Top 5 Products**: Aggregates total sales across all locations to pinpoint overall top-selling SKUs.
- **Sales Extremes**: Computes highest and lowest revenue dates for every store location.

### 5. Data Visualization
- **Bar Chart**: Total daily sales comparison across store locations.
- **Line Chart**: 7-day sales trend tracking for store performance analysis.

---

## 📂 Project Repository Structure

```
.
├── Retail_Data_Engineering_Pipeline.ipynb             # Primary Databricks PySpark & SQL Pipeline Notebook
├── Retail_Data_Engineering_Project_Documentation.pdf # Detailed Technical Requirements & Project Specifications
├── .gitignore                                         # Excluded temporary, system, and checkpoint files
└── README.md                                          # Complete Project Architecture & Documentation
```

---

## 🚀 Technologies Used

- **Databricks Environment**: Distributed Apache Spark cluster (Serverless / Shared Runtime)
- **Languages**: Python (PySpark), SQL
- **Data Lake Storage**: Delta Lake (ACID Transactions, Time Travel, Partitioning & Z-Ordering)
- **Data Visualizations**: Databricks Native Plotting & Analytics Engine

---

## 👤 Author & Acknowledgments

Developed as part of the **Retail Data Engineering Pipeline** project demonstrating modern Medallion Architecture and Delta Lake performance optimization techniques.
