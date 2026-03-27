# 🛒 E-Commerce Analytics Engineering Project

A production-grade analytics engineering project built on the **Brazilian E-Commerce (Olist)** public dataset. Demonstrates end-to-end ELT pipeline design using modern data stack tools — Snowflake, dbt Cloud, and Great Expectations.

**Author:** Nitesh Ranga  
**Stack:** Snowflake · dbt Cloud · Great Expectations · Kimball Star Schema  
**Status:** 🚧 In Progress

---

## 🎯 Project Goals

This project is designed to demonstrate core Analytics Engineering competencies:

- Design and implement a **Kimball Star Schema** dimensional model
- Build and test **dbt transformation pipelines** with full documentation
- Implement **Data Quality checks** using dbt tests and Great Expectations
- Optimize Snowflake performance using **clustering keys and Iceberg tables**
- Maintain **data lineage** via dbt docs

---

## 📊 Dataset

**Source:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle)

| Table | Rows | Description |
|-------|------|-------------|
| orders | 99,441 | Order header with status and timestamps |
| customers | 99,441 | Customer location data |
| order_items | 112,650 | Line items with product and seller |
| products | 32,951 | Product attributes and category |
| sellers | 3,095 | Seller location data |
| order_payments | 103,886 | Payment type and value |
| order_reviews | 99,224 | Customer review scores and comments |
| product_category_translation | 72 | Portuguese to English category names |

---

## 🏗️ Architecture

```
Kaggle CSVs (8 files)
        ↓
Snowflake RAW Schema
(All VARCHAR — source of truth, no transformations)
        ↓
dbt Staging Models (stg_*)
(Cast types, rename columns, basic cleaning)
        ↓
dbt Dimensions (dim_*)              dbt Facts (fct_*)
dim_customers                       fct_orders
dim_products                        fct_order_items
dim_sellers
dim_date
        ↓
dbt Marts (mart_*)
mart_sales_summary
mart_seller_performance
mart_customer_segments
        ↓
Snowflake Iceberg Tables (Marts Layer)
```

---

## 📁 Project Structure

```
ecommerce-analytics/
│
├── dbt_project.yml                  ← dbt project config
├── README.md                        ← This file
│
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_products.sql
│   │   ├── stg_sellers.sql
│   │   ├── stg_order_payments.sql
│   │   ├── stg_order_reviews.sql
│   │   └── schema.yml               ← dbt tests for staging
│   │
│   ├── dimensions/
│   │   ├── dim_customers.sql        ← SCD Type 2
│   │   ├── dim_products.sql
│   │   ├── dim_sellers.sql
│   │   ├── dim_date.sql
│   │   └── schema.yml
│   │
│   ├── facts/
│   │   ├── fct_orders.sql
│   │   ├── fct_order_items.sql
│   │   └── schema.yml
│   │
│   └── marts/
│       ├── mart_sales_summary.sql
│       ├── mart_seller_performance.sql
│       ├── mart_customer_segments.sql
│       └── schema.yml
│
├── tests/
│   └── assert_order_items_positive_price.sql  ← singular test
│
├── snapshots/
│   └── dim_customers_snapshot.sql   ← SCD Type 2 snapshot
│
├── macros/
│   └── generate_schema_name.sql
│
└── great-expectations/
    ├── great_expectations.yml
    └── expectations/
        └── raw_orders_suite.json
```

---

## 🔷 Dimensional Model (Kimball Star Schema)

### Fact Tables

**fct_orders** (grain: one row per order)
```
order_key (PK)
customer_key (FK → dim_customers)
date_key (FK → dim_date)
order_status
total_order_value
freight_value
payment_installments
delivery_delay_days
review_score
```

**fct_order_items** (grain: one row per order line item)
```
order_item_key (PK)
order_key (FK → fct_orders)
product_key (FK → dim_products)
seller_key (FK → dim_sellers)
price
freight_value
```

### Dimension Tables

| Dimension | Key Attributes | SCD Type |
|-----------|---------------|----------|
| dim_customers | city, state, region | Type 2 |
| dim_products | category (EN), weight, dimensions | Type 1 |
| dim_sellers | city, state | Type 1 |
| dim_date | day, week, month, quarter, year, is_weekend | Static |

---

## ✅ Data Quality Strategy

### Layer 1 — dbt Tests (every model)
```yaml
# Example from schema.yml
- name: fct_orders
  columns:
    - name: order_key
      tests:
        - unique
        - not_null
    - name: customer_key
      tests:
        - not_null
        - relationships:
            to: ref('dim_customers')
            field: customer_key
```

### Layer 2 — Singular Tests
```sql
-- tests/assert_order_items_positive_price.sql
SELECT * FROM {{ ref('fct_order_items') }}
WHERE price <= 0
```

### Layer 3 — Great Expectations (RAW layer)
Expectations defined on `RAW.ORDERS`:
- `order_id` is unique and not null
- `order_status` is in expected value set
- `order_purchase_timestamp` matches datetime format
- Row count between 90,000 and 110,000

---

## ❄️ Snowflake Configuration

```sql
-- Database & Schema structure
ECOMMERCE/
├── RAW       (source data, all VARCHAR)
├── STG       (dbt staging, views)
├── DIMS      (dbt dimensions, tables)
├── FACTS     (dbt facts, tables)
└── MARTS     (dbt marts, Iceberg tables)

-- Warehouse
DBT_WH  (X-Small, auto-suspend 60s)
```

### Iceberg Tables (Marts Layer)
Marts are materialized as Snowflake-managed Iceberg tables demonstrating modern open table format support.

### Clustering Keys
```sql
-- fct_orders clustered by date for performance
CLUSTER BY (order_purchase_date)
```

---

## 🚀 How to Run

### Prerequisites
- Snowflake trial account
- dbt Cloud account (free developer tier)
- Python 3.9+ (for Great Expectations)

### Setup

```bash
# 1. Clone repository
git clone https://github.com/niteshranga/ecommerce-analytics.git
cd ecommerce-analytics

# 2. Run dbt
dbt debug       # test connection
dbt run         # build all models
dbt test        # run all tests
dbt docs generate && dbt docs serve  # view lineage
```

### Great Expectations
```bash
pip install great_expectations
great_expectations checkpoint run raw_orders_checkpoint
```

---

## 🔧 Technology Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Storage | Snowflake | Cloud data warehouse |
| Transformation | dbt Cloud | ELT, testing, docs |
| Table Format | Apache Iceberg (via Snowflake) | Open table format |
| Data Quality | Great Expectations + dbt tests | Validation |
| Version Control | GitHub | CI/CD |
| Orchestration | dbt Cloud Scheduler | Pipeline runs |

---

## 📈 Key Design Decisions

**Why all VARCHAR in RAW?**  
The RAW layer is a faithful copy of source files. Type casting happens in dbt staging models — this prevents load failures if source formats change and follows industry standard patterns used by tools like Fivetran and Airbyte.

**Why Kimball over Data Vault?**  
Given the relatively small number of source entities and analytical (not operational) use case, Kimball Star Schema provides simpler, faster query patterns optimal for BI consumption.

**Why dbt Cloud over dbt Core?**  
dbt Cloud provides built-in lineage visualization, IDE, job scheduling, and GitHub integration — reducing infrastructure overhead for a solo project.

---

## 📚 References

- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Kimball Group — Dimensional Modeling](https://www.kimballgroup.com/)
- [Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

**Last Updated:** March 2026  
**Version:** 1.0.0
