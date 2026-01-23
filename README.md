# spark-medallion-pipeline
Medallion Architecture data pipeline (Bronze → Silver → Gold) demonstrating lakehouse patterns for data engineering
# 🏅 Medallion Architecture Pipeline

A data engineering pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) pattern used in modern data lakehouses like Databricks and Delta Lake.

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data-150458?style=flat&logo=pandas&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-Storage-50ABF1?style=flat)
![License](https://img.shields.io/badge/License-MIT-green.svg)

---

## 📋 Overview

The Medallion Architecture is a data design pattern used to organize data in a lakehouse. This project demonstrates a complete implementation with:

- **🥉 Bronze Layer:** Raw data ingestion with metadata tracking
- **🥈 Silver Layer:** Cleaned, validated, and standardized data
- **🥇 Gold Layer:** Business-level aggregations ready for analytics

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA LAKEHOUSE                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │   🥉 BRONZE  │    │   🥈 SILVER  │    │   🥇 GOLD    │     │
│  │             │    │             │    │             │     │
│  │ Raw Data    │───▶│ Cleaned     │───▶│ Aggregated  │     │
│  │ + Metadata  │    │ Validated   │    │ Analytics   │     │
│  │             │    │ Standardized│    │ Ready       │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│                                                             │
│  • Parquet format   • Data quality    • Daily summary      │
│  • Source tracking  • Type conversion • Customer metrics   │
│  • Ingestion time   • Deduplication   • Product metrics    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 Features

| Layer | Transformations |
|-------|-----------------|
| **🥉 Bronze** | Raw ingestion, metadata tagging, source tracking |
| **🥈 Silver** | Column standardization, date parsing, null handling, calculated fields |
| **🥇 Gold** | Daily aggregations, customer analytics, product metrics, rankings |

---

## 📊 Gold Layer Outputs

### 1. Daily Sales Summary
- Total transactions per day/region
- Gross and net revenue
- Average order value

### 2. Customer Metrics
- Total orders and spend per customer
- First/last purchase dates
- Customer spend rankings

### 3. Product Metrics
- Units sold and revenue per product
- Average pricing and discounts
- Product revenue rankings

---

## 🚀 Quick Start

### Prerequisites

```bash
pip install pandas numpy pyarrow
```

### Run the Pipeline

1. Clone this repository
2. Open `spark_medallion_pipeline.ipynb` in Jupyter Notebook
3. Run all cells to execute the full pipeline

---

## 📁 Project Structure

```
spark-medallion-pipeline/
├── spark_medallion_pipeline.ipynb    # Main pipeline notebook
├── medallion_lakehouse/              # Generated data directory
│   ├── bronze/                       # Raw data layer
│   │   └── transactions/
│   ├── silver/                       # Cleaned data layer
│   │   └── transactions/
│   └── gold/                         # Aggregated data layer
│       ├── daily_summary/
│       ├── customer_metrics/
│       └── product_metrics/
├── gold_daily_summary.csv            # Exported CSV
├── gold_customer_metrics.csv         # Exported CSV
├── gold_product_metrics.csv          # Exported CSV
└── README.md
```

---

## 🔄 Data Transformations

### Bronze → Silver

| Transformation | Description |
|----------------|-------------|
| Column Standardization | Uppercase customer IDs, title case products |
| Date Parsing | Handle multiple date formats (YYYY-MM-DD, MM/DD/YYYY, etc.) |
| Null Handling | Default discount_pct to 0 |
| Calculated Fields | gross_amount, discount_amount, net_amount |
| Date Parts | Extract year, month, day for partitioning |

### Silver → Gold

| Aggregation | Metrics |
|-------------|---------|
| Daily Summary | Transaction count, revenue, avg order value |
| Customer Metrics | Total spend, order count, rankings |
| Product Metrics | Units sold, revenue, rankings |

---

## 📈 Sample Output

### Customer Metrics (Top 5)
| customer_id | total_orders | total_spend | spend_rank |
|-------------|--------------|-------------|------------|
| CUST001 | 45 | $12,543.00 | 1 |
| CUST007 | 42 | $11,892.50 | 2 |
| CUST003 | 38 | $10,234.75 | 3 |

---

## 🛠️ Tech Stack

- **Python 3.8+**
- **Pandas** - Data manipulation
- **NumPy** - Numerical operations
- **Parquet** - Columnar storage format
- **Jupyter Notebook** - Interactive development

---

## 💡 Production Considerations

In a production environment, this pipeline would be implemented with:

- **Apache Spark / PySpark** - Distributed processing at scale
- **Delta Lake** - ACID transactions, time travel, schema enforcement
- **Databricks** - Managed Spark platform with Unity Catalog
- **Airflow / Prefect** - Workflow orchestration
- **Great Expectations** - Data quality validation

---

## 🔮 Future Improvements

- [ ] Add incremental/CDC processing
- [ ] Implement data quality framework
- [ ] Add unit tests for transformations
- [ ] Create visualization dashboard
- [ ] Add schema evolution handling

---

## 👤 Author

**Brian Stratton**  
Data Engineer | AI/ML Engineer | Doctoral Researcher  
[LinkedIn](https://www.linkedin.com/in/briankstratton/) | [GitHub](https://github.com/BrianKeith2027)

---

## 📄 License

This project is licensed under the MIT License.
