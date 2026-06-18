# Demo Menu Structure

This document defines the terminal demo menu structure for the Sales Data Analytics project.

The menu is organized around the two required project parts:

- Part 1: Apache Spark analytics
- Part 2: Hadoop HDFS integration

This avoids presenting infrastructure steps as if they were analysis result pages.

## Execution Model

The terminal menu should use a two-phase demo model:

```text
1. Run the full Spark analysis once.
2. Browse the exported results through the menu.
```

The analysis menu items should not recompute Spark jobs one by one. They should read the already exported CSV/JSON files from `output/` and display the relevant result tables quickly.

Only explicit runtime actions should execute Docker, HDFS, or Spark commands:

- Hadoop HDFS Integration actions
- Run Full Analysis

This keeps the live demo stable and avoids repeatedly processing the full dataset for each menu item.

## Main Menu

```text
Sales Data Analytics Demo

1. Run Full Analysis
2. Apache Spark Analytics
3. Hadoop HDFS Integration
4. View Exported Files
0. Exit
```

Main menu behavior:

| Menu item | Behavior |
| --- | --- |
| Run Full Analysis | Execute the full PySpark pipeline once, preferably from the HDFS path, and regenerate all `output/` result files. |
| Apache Spark Analytics | Browse existing exported Spark analysis results from `output/`. If required files are missing, ask the user to run `Run Full Analysis` first. |
| Hadoop HDFS Integration | Run infrastructure-oriented HDFS checks or upload commands. |
| View Exported Files | List generated CSV/JSON files under `output/`. |

## 1. Apache Spark Analytics

This section maps to Part 1 of the project requirement:

- load the dataset
- clean and transform the data
- perform analyses related to sales, products, customers, and countries
- display analysis results
- compute business indicators and KPIs

Submenu:

```text
Apache Spark Analytics

1. Load Dataset
2. Clean and Transform
3. KPI Summary
4. Sales Analysis
5. Product Analysis
6. Customer Analysis
7. Country Analysis
8. Export Results
0. Back to Main Menu
```

Important behavior:

```text
These options are result viewers.
They should read exported files from output/.
They should not launch a new Spark job.
```

Menu item mapping:

| Menu item | Project step | Data source | Purpose |
| --- | --- | --- | --- |
| Load Dataset | Step 2 | Run summary / exported metadata if available | Show input path, raw row count, columns, missing values, and sample data summary. |
| Clean and Transform | Step 3 | Run summary / exported metadata if available | Show cleaning rules, cleaned row count, excluded invalid rows, and derived fields. |
| KPI Summary | Step 4 | `output/overall_kpis/overall_kpis.csv` | Show total revenue, orders, quantity, customers, products, countries, AOV, revenue per customer, and cancellation rate. |
| Sales Analysis | Step 5 | `output/monthly_sales_trend`, `output/weekday_sales`, `output/hourly_sales`, `output/monthly_revenue_extremes` | Show monthly trend, weekday sales, hourly sales, and highest/lowest revenue months. |
| Product Analysis | Step 6 | `output/top_products_by_quantity`, `output/top_products_by_revenue`, `output/bottom_products_by_revenue` | Show top products by quantity, top products by revenue, and low-revenue products. |
| Customer Analysis | Step 7 | `output/top_customers`, `output/customer_rfm_top`, `output/customer_segment_summary` | Show top customers, RFM results, and customer segment summary. |
| Country Analysis | Step 8 | `output/top_countries_by_revenue`, `output/market_share_uk_vs_non_uk`, `output/potential_growth_markets` | Show top countries, UK vs Non-UK market share, and potential growth markets. |
| Export Results | Step 10 | `output/` | Show exported CSV/JSON result tables in the `output/` directory. |

## 2. Hadoop HDFS Integration

This section maps to Part 2 of the project requirement:

- store the dataset file in Hadoop Distributed File System (HDFS)
- run Spark analysis using the HDFS dataset path

Submenu:

```text
Hadoop HDFS Integration

1. Check HDFS Services
2. Upload Dataset to HDFS
3. Show HDFS Dataset Path
4. Run Analysis from HDFS
0. Back to Main Menu
```

Menu item mapping:

| Menu item | Related step | Purpose |
| --- | --- | --- |
| Check HDFS Services | Step 1 / Step 9 | Check Docker services, NameNode, DataNode, and HDFS directory access. |
| Upload Dataset to HDFS | Step 9 | Create `/data/online-retail` and upload `online_retail.csv` to HDFS. |
| Show HDFS Dataset Path | Step 9 | Display the dataset path: `hdfs://namenode:9000/data/online-retail/online_retail.csv`. |
| Run Analysis from HDFS | Step 9 + Spark steps | Run the full PySpark analysis using the HDFS path as input. |

## 3. Run Full Analysis

This main menu item runs the complete PySpark analysis pipeline.

Recommended default:

```text
Run full analysis from HDFS and export results to output/
```

Expected behavior:

1. Ensure Spark and HDFS services are running.
2. Ensure the dataset exists in HDFS.
3. Run the full PySpark pipeline from the HDFS input path.
4. Export all result tables to `output/`.
5. Return to the main menu after completion.

Equivalent command:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py \
  hdfs://namenode:9000/data/online-retail/online_retail.csv \
  /app/output
```

## 4. View Exported Files

This section lists generated result tables from `output/`.

This is also the fallback check for the result viewer menu. If these files do not exist, the menu should prompt the user to run:

```text
Main Menu -> Run Full Analysis
```

Expected tables:

- `overall_kpis`
- `monthly_sales_trend`
- `weekday_sales`
- `hourly_sales`
- `monthly_revenue_extremes`
- `top_products_by_quantity`
- `top_products_by_revenue`
- `bottom_products_by_revenue`
- `top_customers`
- `customer_rfm_top`
- `customer_segment_summary`
- `country_performance`
- `top_countries_by_revenue`
- `market_share_uk_vs_non_uk`
- `potential_growth_markets`

Each table is exported as CSV and JSON:

```text
output/<table_name>/<table_name>.csv
output/<table_name>/<table_name>.json
```

## Demo Explanation

Use this explanation when presenting the menu:

```text
The project requirements have two parts.
Part 1 is Apache Spark: loading, cleaning, analyzing, displaying results, and computing KPIs.
Part 2 is Hadoop HDFS: storing the dataset in HDFS and using it as the data source for Spark.
Therefore, the demo menu is organized into Spark Analytics and HDFS Integration.
```
