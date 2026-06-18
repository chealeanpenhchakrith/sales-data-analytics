# Sales-Data-Analytics

## Step 1: Docker Runtime

This project includes a Docker Compose runtime for Spark, PySpark, and HDFS.

### Optional local run

The script resolves default paths from the project directory, so it does not depend on the folder where the repository was cloned.

Docker is the primary runtime for this project. If running outside Docker, install dependencies from `requirements.txt`, then run:

```bash
python src/script.py
```

Default local paths:

- input data: `src/data/Online Retail.csv`
- output directory: `output/`

You can still override the input and output paths:

```bash
python src/script.py "src/data/Online Retail.csv" output
```

### Services

- Spark master UI: <http://localhost:8080>
- Spark worker UI: <http://localhost:8081>
- Spark master service: `spark://spark-master:7077`
- HDFS NameNode UI: <http://localhost:9870>
- HDFS DataNode UI: <http://localhost:9864>
- HDFS RPC address: `hdfs://namenode:9000`

The project directory is mounted into the Spark containers at `/app`.
The dataset `src/data/Online Retail.csv` is mounted into the NameNode container as `/upload/online_retail.csv`.
HDFS NameNode and DataNode storage use Docker volumes so the uploaded dataset survives container restarts.

### Start the environment

Check Docker and Docker Compose:

```bash
docker --version
docker compose version
```

Start all services:

```bash
docker compose up -d
```

Check container status:

```bash
docker compose ps
```

All four services should be running:

- `spark-master`
- `spark-worker`
- `namenode`
- `datanode`

### Verify Spark

Open <http://localhost:8080> and confirm that the Spark master page shows one connected worker.

Run the current PySpark script from the Spark master container:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/script.py
```

### Verify HDFS

Open <http://localhost:9870> and confirm that the HDFS NameNode page loads.

Check HDFS from the NameNode container:

```bash
docker compose exec namenode hdfs dfs -ls /
```

Create the HDFS data directory and upload the retail CSV:

```bash
bash scripts/upload_data_to_hdfs.sh
```

The HDFS path for later PySpark integration is:

```text
hdfs://namenode:9000/data/online-retail/online_retail.csv
```

### Step 1 completion check

Run:

```bash
bash scripts/verify_step1.sh
```

Step 1 is complete when:

- `docker compose up -d` starts all four services.
- Spark UI opens at <http://localhost:8080> and shows a worker.
- HDFS NameNode UI opens at <http://localhost:9870>.
- `hdfs dfs -ls /` works inside the NameNode container.
- `spark-submit` can run `/app/src/script.py`.

## Step 2: Load and Inspect the Dataset

The PySpark script loads the retail CSV with the required options:

- `header=True`
- `sep=";"`
- `encoding="UTF-8"`
- permissive CSV parsing
- BOM-safe column name cleanup

The script prints:

- source path
- schema
- total row count
- column list
- null or empty value count by column
- first 30 sample rows

Run Step 2 with the default local container path:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/script.py
```

Run Step 2 with the HDFS dataset path:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/script.py hdfs://namenode:9000/data/online-retail/online_retail.csv
```

Verify Step 2:

```bash
bash scripts/verify_step2.sh
```

Expected checks:

- row count: `541909`
- columns: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`
- missing or empty values detected in `Description` and `CustomerID`
- both local and HDFS input paths can be read

## Step 3: Clean and Transform the Dataset

The PySpark script now prepares a cleaned sales DataFrame for regular sales analysis.

Cleaning and transformation rules:

- trims string fields
- keeps `CustomerID` as a string
- converts `Quantity` to integer
- converts `UnitPrice` from comma decimal format to double
- converts `InvoiceDate` with `dd/MM/yyyy HH:mm`
- marks cancelled invoices with `IsCancelled`
- excludes cancelled invoices from regular sales analysis
- removes rows with empty `Description`
- removes rows with empty `CustomerID`
- removes rows with `Quantity <= 0`
- removes rows with `UnitPrice <= 0`

Derived fields:

- `Revenue = Quantity * UnitPrice`
- `InvoiceDateOnly`
- `Year`
- `Month`
- `YearMonth`
- `DayOfWeek`
- `Hour`

Verify Step 3:

```bash
bash scripts/verify_step3.sh
```

Expected Step 3 checks:

- raw row count: `541909`
- cleaned regular sales row count: `397884`
- cancelled invoice rows excluded from regular sales: `9288`
- empty `Description` rows: `1454`
- empty `CustomerID` rows: `135080`
- invalid `Quantity` rows: `10624`
- invalid `UnitPrice` rows: `2517`

## Step 4: Calculate Core KPIs

The PySpark script calculates core business KPIs from the cleaned regular sales data.

KPI definitions:

- total sales revenue: `sum(Revenue)`
- total orders: `countDistinct(InvoiceNo)`
- total quantity sold: `sum(Quantity)`
- total customers: `countDistinct(CustomerID)`
- total products: `countDistinct(StockCode)`
- total countries: `countDistinct(Country)`
- average order value: `TotalRevenue / TotalOrders`
- average revenue per customer: `TotalRevenue / TotalCustomers`
- cancelled orders: distinct cancelled invoice count from the typed raw data
- cancellation rate: `CancelledOrders / TotalRawInvoices * 100`

Verify Step 4:

```bash
bash scripts/verify_step4.sh
```

Expected Step 4 KPI values:

- total revenue: `8911407.9`
- total orders: `18532`
- total quantity sold: `5167812`
- total customers: `4338`
- total products: `3665`
- total countries: `37`
- average order value: `480.87`
- average revenue per customer: `2054.27`
- cancelled orders: `3836`
- total raw invoices: `25900`
- cancellation rate: `14.81%`

## Step 5: Sales Analysis

The PySpark script now prints sales analysis tables from the cleaned regular sales data.

Sales analysis outputs:

- monthly sales trend with revenue, order count, and quantity sold
- sales by day of week
- sales by hour of day
- highest and lowest revenue months

Verify Step 5:

```bash
bash scripts/verify_step5.sh
```

Expected Step 5 highlights:

- monthly trend covers `2010-12` through `2011-12`
- highest revenue month: `2011-11`, revenue `1161817.38`
- lowest revenue month: `2011-02`, revenue `447137.35`
- highest weekday revenue in this cleaned dataset: `Thursday`, revenue `1976859.07`
- highest hourly revenue in this cleaned dataset: hour `12`, revenue `1378571.48`

## Step 6: Product Analysis

The PySpark script now aggregates product performance by `StockCode` and `Description`.

Product analysis outputs:

- total quantity sold per product
- total revenue per product
- distinct order count per product
- Top 10 best-selling products by quantity
- Top 10 products by revenue
- Bottom 10 products by revenue for low-performance review

Verify Step 6:

```bash
bash scripts/verify_step6.sh
```

Expected Step 6 highlights:

- top product by quantity: `PAPER CRAFT , LITTLE BIRDIE`, quantity `80995`
- top product by revenue: `PAPER CRAFT , LITTLE BIRDIE`, revenue `168469.6`
- second revenue product: `REGENCY CAKESTAND 3 TIER`, revenue `142592.95`
- low-revenue product example: `PADS TO MATCH ALL CUSHIONS`, revenue `0.0`

## Step 7: Customer Analysis

The PySpark script now aggregates customer performance and calculates a simple RFM segmentation.

Customer analysis outputs:

- customer total revenue
- distinct order count
- total quantity purchased
- latest purchase date
- Top 10 high-value customers
- RFM fields: `RecencyDays`, `Frequency`, `Monetary`
- customer segment summary

Segmentation rules:

- `High Value`: monetary >= `5000`, frequency >= `10`, recency <= `60`
- `Active`: frequency >= `5`, recency <= `90`
- `At Risk`: recency > `180`
- `Regular`: all other customers

Verify Step 7:

```bash
bash scripts/verify_step7.sh
```

Expected Step 7 highlights:

- top customer: `14646`, revenue `280206.02`
- second customer: `18102`, revenue `259657.3`
- customer segments:
  - `High Value`: `201`
  - `Active`: `842`
  - `Regular`: `2434`
  - `At Risk`: `861`

## Step 8: Country Analysis

The PySpark script now aggregates country-level performance from the cleaned regular sales data.

Country analysis outputs:

- revenue by country
- distinct order count by country
- distinct customer count by country
- quantity sold by country
- Top 10 countries by revenue
- United Kingdom vs Non-UK market share
- potential growth markets outside the United Kingdom

Potential growth markets are defined as non-UK countries with at least `5` customers and `10` orders, sorted by lower revenue per customer and then market activity.

Verify Step 8:

```bash
bash scripts/verify_step8.sh
```

Expected Step 8 highlights:

- top country: `United Kingdom`, revenue `7308391.55`
- second country: `Netherlands`, revenue `285446.34`
- third country: `EIRE`, revenue `265545.9`
- UK revenue share: `82.01%`
- Non-UK revenue share: `17.99%`
- potential growth market examples: `Austria`, `Poland`, `Italy`, `Belgium`

## Step 9: HDFS Integration

The project is integrated with Hadoop HDFS through the Docker Compose runtime.

HDFS services:

- NameNode: `namenode`
- DataNode: `datanode`
- NameNode Web UI: <http://localhost:9870>
- HDFS RPC address: `hdfs://namenode:9000`

Dataset location:

```text
hdfs://namenode:9000/data/online-retail/online_retail.csv
```

Upload the local CSV to HDFS:

```bash
bash scripts/upload_data_to_hdfs.sh
```

Run the PySpark analysis from HDFS:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/script.py hdfs://namenode:9000/data/online-retail/online_retail.csv
```

Verify Step 9:

```bash
bash scripts/verify_step9.sh
```

Expected Step 9 checks:

- HDFS NameNode and DataNode containers are running
- HDFS directory `/data/online-retail` exists
- dataset file exists at `/data/online-retail/online_retail.csv`
- PySpark can read the HDFS path
- analysis output still reports the raw row count `541909`

## Step 10: Save Analysis Outputs

The PySpark script now exports the analysis results for reporting and later review.

Default output directory inside the Spark container:

```text
/app/output
```

Because the project directory is mounted into the Spark container at `/app`, the exported files appear locally in:

```text
output/
```

Run the analysis and write outputs from the HDFS dataset:

```bash
mkdir -p output
chmod 777 output
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/script.py hdfs://namenode:9000/data/online-retail/online_retail.csv /app/output
```

Exported result tables:

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

Each table is saved as both CSV and JSON:

```text
output/<table_name>/<table_name>.csv
output/<table_name>/<table_name>.json
```

Verify Step 10:

```bash
bash scripts/verify_step10.sh
```

Expected Step 10 checks:

- Step 10 export section is printed by the Spark job
- KPI output includes total revenue `8911407.9`
- monthly trend output includes `2011-11`
- top product output includes `PAPER CRAFT`
- top customer output includes `14646`
- country output includes `United Kingdom`
