import csv
import json
import os
import sys
from functools import partial
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


print = partial(print, flush=True)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_PATH = str(PROJECT_ROOT / "src" / "data" / "Online Retail.csv")
DEFAULT_OUTPUT_PATH = str(PROJECT_ROOT / "output")


def build_spark_session():
    return (
        SparkSession.builder
        .appName("SalesDataAnalytics")
        .getOrCreate()
    )


def clean_column_names(df):
    cleaned_names = [
        column_name.replace("\ufeff", "").strip()
        for column_name in df.columns
    ]
    return df.toDF(*cleaned_names)


def load_retail_data(spark, input_path):
    raw_df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .csv(input_path)
    )
    return clean_column_names(raw_df)


def summarize_nulls(df):
    null_expressions = [
        F.count(
            F.when(
                F.col(column_name).isNull()
                | (F.trim(F.col(column_name).cast("string")) == ""),
                column_name,
            )
        )
        .alias(column_name)
        for column_name in df.columns
    ]
    return df.select(null_expressions)


def prepare_typed_data(df):
    return (
        df.select(
            F.trim(F.col("InvoiceNo")).alias("InvoiceNo"),
            F.trim(F.col("StockCode")).alias("StockCode"),
            F.trim(F.col("Description")).alias("Description"),
            F.trim(F.col("Quantity")).cast("int").alias("Quantity"),
            F.to_timestamp(F.trim(F.col("InvoiceDate")), "dd/MM/yyyy HH:mm").alias("InvoiceDate"),
            F.regexp_replace(F.trim(F.col("UnitPrice")), ",", ".").cast("double").alias("UnitPrice"),
            F.trim(F.col("CustomerID")).alias("CustomerID"),
            F.trim(F.col("Country")).alias("Country"),
        )
        .withColumn("IsCancelled", F.col("InvoiceNo").startswith("C"))
    )


def clean_sales_data(df):
    return (
        df.filter(F.col("Description").isNotNull() & (F.trim(F.col("Description")) != ""))
        .filter(F.col("CustomerID").isNotNull() & (F.trim(F.col("CustomerID")) != ""))
        .filter(F.col("Quantity").isNotNull() & (F.col("Quantity") > 0))
        .filter(F.col("UnitPrice").isNotNull() & (F.col("UnitPrice") > 0))
        .filter(~F.col("IsCancelled"))
        .withColumn("Revenue", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
        .withColumn("InvoiceDateOnly", F.to_date(F.col("InvoiceDate")))
        .withColumn("Year", F.year(F.col("InvoiceDate")))
        .withColumn("Month", F.month(F.col("InvoiceDate")))
        .withColumn("YearMonth", F.date_format(F.col("InvoiceDate"), "yyyy-MM"))
        .withColumn("DayOfWeek", F.date_format(F.col("InvoiceDate"), "EEEE"))
        .withColumn("Hour", F.hour(F.col("InvoiceDate")))
    )


def print_step2_profile(df, input_path):
    print("\n=== Step 2: Data Source ===")
    print(input_path)

    print("\n=== Step 2: Raw Schema ===")
    df.printSchema()

    print("\n=== Step 2: Raw Row Count ===")
    print(df.count())

    print("\n=== Step 2: Raw Columns ===")
    print(df.columns)

    print("\n=== Step 2: Raw Null or Empty Value Count by Column ===")
    summarize_nulls(df).show(truncate=False)

    print("\n=== Step 2: Raw Sample Data ===")
    df.show(30, truncate=False)


def print_step3_profile(typed_df, cleaned_df):
    print("\n=== Step 3: Data Cleaning Summary ===")
    raw_count = typed_df.count()
    cleaned_count = cleaned_df.count()
    cancelled_count = typed_df.filter(F.col("IsCancelled")).count()
    empty_description_count = typed_df.filter(
        F.col("Description").isNull() | (F.trim(F.col("Description")) == "")
    ).count()
    empty_customer_count = typed_df.filter(
        F.col("CustomerID").isNull() | (F.trim(F.col("CustomerID")) == "")
    ).count()
    invalid_quantity_count = typed_df.filter(
        F.col("Quantity").isNull() | (F.col("Quantity") <= 0)
    ).count()
    invalid_unit_price_count = typed_df.filter(
        F.col("UnitPrice").isNull() | (F.col("UnitPrice") <= 0)
    ).count()

    print(f"Raw rows: {raw_count}")
    print(f"Rows after cleaning for regular sales analysis: {cleaned_count}")
    print(f"Rows removed or excluded: {raw_count - cleaned_count}")
    print(f"Cancelled invoice rows excluded from regular sales: {cancelled_count}")
    print(f"Rows with empty Description: {empty_description_count}")
    print(f"Rows with empty CustomerID: {empty_customer_count}")
    print(f"Rows with invalid Quantity: {invalid_quantity_count}")
    print(f"Rows with invalid UnitPrice: {invalid_unit_price_count}")

    print("\n=== Step 3: Cleaned Schema ===")
    cleaned_df.printSchema()

    print("\n=== Step 3: Cleaned Null or Empty Value Count by Column ===")
    summarize_nulls(cleaned_df).show(truncate=False)

    print("\n=== Step 3: Cleaned Sample Data ===")
    cleaned_df.show(30, truncate=False)


def calculate_core_kpis(typed_df, cleaned_df):
    sales_kpis = cleaned_df.agg(
        F.round(F.sum("Revenue"), 2).alias("TotalRevenue"),
        F.countDistinct("InvoiceNo").alias("TotalOrders"),
        F.sum("Quantity").alias("TotalQuantitySold"),
        F.countDistinct("CustomerID").alias("TotalCustomers"),
        F.countDistinct("StockCode").alias("TotalProducts"),
        F.countDistinct("Country").alias("TotalCountries"),
    )

    cancellation_kpis = typed_df.agg(
        F.countDistinct("InvoiceNo").alias("TotalRawInvoices"),
        F.countDistinct(F.when(F.col("IsCancelled"), F.col("InvoiceNo"))).alias("CancelledOrders"),
    )

    return (
        sales_kpis.crossJoin(cancellation_kpis)
        .withColumn(
            "AverageOrderValue",
            F.round(F.col("TotalRevenue") / F.col("TotalOrders"), 2),
        )
        .withColumn(
            "AverageRevenuePerCustomer",
            F.round(F.col("TotalRevenue") / F.col("TotalCustomers"), 2),
        )
        .withColumn(
            "CancellationRatePercent",
            F.round((F.col("CancelledOrders") / F.col("TotalRawInvoices")) * 100, 2),
        )
        .select(
            "TotalRevenue",
            "TotalOrders",
            "TotalQuantitySold",
            "TotalCustomers",
            "TotalProducts",
            "TotalCountries",
            "AverageOrderValue",
            "AverageRevenuePerCustomer",
            "CancelledOrders",
            "TotalRawInvoices",
            "CancellationRatePercent",
        )
    )


def print_step4_kpis(typed_df, cleaned_df):
    print("\n=== Step 4: Core KPI Summary ===")
    calculate_core_kpis(typed_df, cleaned_df).show(truncate=False)


def calculate_monthly_sales_trend(cleaned_df):
    return (
        cleaned_df.groupBy("YearMonth")
        .agg(
            F.round(F.sum("Revenue"), 2).alias("MonthlyRevenue"),
            F.countDistinct("InvoiceNo").alias("MonthlyOrders"),
            F.sum("Quantity").alias("MonthlyQuantitySold"),
        )
        .orderBy("YearMonth")
    )


def calculate_weekday_sales(cleaned_df):
    return (
        cleaned_df.withColumn("DayOfWeekNumber", F.dayofweek(F.col("InvoiceDate")))
        .groupBy("DayOfWeekNumber", "DayOfWeek")
        .agg(
            F.round(F.sum("Revenue"), 2).alias("WeekdayRevenue"),
            F.countDistinct("InvoiceNo").alias("WeekdayOrders"),
            F.sum("Quantity").alias("WeekdayQuantitySold"),
        )
        .orderBy("DayOfWeekNumber")
    )


def calculate_hourly_sales(cleaned_df):
    return (
        cleaned_df.groupBy("Hour")
        .agg(
            F.round(F.sum("Revenue"), 2).alias("HourlyRevenue"),
            F.countDistinct("InvoiceNo").alias("HourlyOrders"),
            F.sum("Quantity").alias("HourlyQuantitySold"),
        )
        .orderBy("Hour")
    )


def calculate_monthly_revenue_extremes(monthly_sales_df):
    highest_month = (
        monthly_sales_df.orderBy(F.desc("MonthlyRevenue"))
        .limit(1)
        .withColumn("Metric", F.lit("HighestRevenueMonth"))
    )
    lowest_month = (
        monthly_sales_df.orderBy(F.asc("MonthlyRevenue"))
        .limit(1)
        .withColumn("Metric", F.lit("LowestRevenueMonth"))
    )
    return highest_month.unionByName(lowest_month).select(
        "Metric",
        "YearMonth",
        "MonthlyRevenue",
        "MonthlyOrders",
        "MonthlyQuantitySold",
    )


def print_step5_sales_analysis(cleaned_df):
    monthly_sales_df = calculate_monthly_sales_trend(cleaned_df)

    print("\n=== Step 5: Monthly Sales Trend ===")
    monthly_sales_df.show(24, truncate=False)

    print("\n=== Step 5: Sales by Day of Week ===")
    calculate_weekday_sales(cleaned_df).show(7, truncate=False)

    print("\n=== Step 5: Sales by Hour ===")
    calculate_hourly_sales(cleaned_df).show(24, truncate=False)

    print("\n=== Step 5: Highest and Lowest Revenue Months ===")
    calculate_monthly_revenue_extremes(monthly_sales_df).show(truncate=False)


def calculate_product_performance(cleaned_df):
    return (
        cleaned_df.groupBy("StockCode", "Description")
        .agg(
            F.sum("Quantity").alias("ProductQuantitySold"),
            F.round(F.sum("Revenue"), 2).alias("ProductRevenue"),
            F.countDistinct("InvoiceNo").alias("ProductOrderCount"),
        )
    )


def print_step6_product_analysis(cleaned_df):
    product_performance_df = calculate_product_performance(cleaned_df)

    print("\n=== Step 6: Top 10 Best-Selling Products by Quantity ===")
    product_performance_df.orderBy(
        F.desc("ProductQuantitySold"),
        F.desc("ProductRevenue"),
    ).show(10, truncate=False)

    print("\n=== Step 6: Top 10 Products by Revenue ===")
    product_performance_df.orderBy(
        F.desc("ProductRevenue"),
        F.desc("ProductQuantitySold"),
    ).show(10, truncate=False)

    print("\n=== Step 6: Bottom 10 Products by Revenue ===")
    product_performance_df.orderBy(
        F.asc("ProductRevenue"),
        F.asc("ProductQuantitySold"),
    ).show(10, truncate=False)


def calculate_customer_performance(cleaned_df):
    return (
        cleaned_df.groupBy("CustomerID")
        .agg(
            F.round(F.sum("Revenue"), 2).alias("CustomerRevenue"),
            F.countDistinct("InvoiceNo").alias("CustomerOrderCount"),
            F.sum("Quantity").alias("CustomerQuantityPurchased"),
            F.max("InvoiceDateOnly").alias("LastPurchaseDate"),
        )
    )


def calculate_customer_rfm(cleaned_df):
    analysis_date_df = cleaned_df.agg(
        F.date_add(F.max("InvoiceDateOnly"), 1).alias("AnalysisDate")
    )

    return (
        calculate_customer_performance(cleaned_df)
        .crossJoin(analysis_date_df)
        .withColumn(
            "RecencyDays",
            F.datediff(F.col("AnalysisDate"), F.col("LastPurchaseDate")),
        )
        .withColumnRenamed("CustomerOrderCount", "Frequency")
        .withColumnRenamed("CustomerRevenue", "Monetary")
        .withColumn(
            "CustomerSegment",
            F.when(
                (F.col("Monetary") >= 5000)
                & (F.col("Frequency") >= 10)
                & (F.col("RecencyDays") <= 60),
                "High Value",
            )
            .when((F.col("Frequency") >= 5) & (F.col("RecencyDays") <= 90), "Active")
            .when(F.col("RecencyDays") > 180, "At Risk")
            .otherwise("Regular"),
        )
        .select(
            "CustomerID",
            "Monetary",
            "Frequency",
            "CustomerQuantityPurchased",
            "LastPurchaseDate",
            "AnalysisDate",
            "RecencyDays",
            "CustomerSegment",
        )
    )


def print_step7_customer_analysis(cleaned_df):
    customer_performance_df = calculate_customer_performance(cleaned_df)
    customer_rfm_df = calculate_customer_rfm(cleaned_df)

    print("\n=== Step 7: Top 10 High-Value Customers ===")
    customer_performance_df.orderBy(
        F.desc("CustomerRevenue"),
        F.desc("CustomerOrderCount"),
    ).show(10, truncate=False)

    print("\n=== Step 7: Top 10 RFM Customers ===")
    customer_rfm_df.orderBy(
        F.desc("Monetary"),
        F.desc("Frequency"),
        F.asc("RecencyDays"),
    ).show(10, truncate=False)

    print("\n=== Step 7: Customer Segment Summary ===")
    calculate_customer_segment_summary(customer_rfm_df).show(truncate=False)


def calculate_customer_segment_summary(customer_rfm_df):
    return (
        customer_rfm_df.groupBy("CustomerSegment")
        .agg(
            F.countDistinct("CustomerID").alias("CustomerCount"),
            F.round(F.sum("Monetary"), 2).alias("SegmentRevenue"),
            F.round(F.avg("Frequency"), 2).alias("AverageFrequency"),
            F.round(F.avg("RecencyDays"), 2).alias("AverageRecencyDays"),
        )
        .orderBy(F.desc("SegmentRevenue"))
    )


def calculate_country_performance(cleaned_df):
    return (
        cleaned_df.groupBy("Country")
        .agg(
            F.round(F.sum("Revenue"), 2).alias("CountryRevenue"),
            F.countDistinct("InvoiceNo").alias("CountryOrderCount"),
            F.countDistinct("CustomerID").alias("CountryCustomerCount"),
            F.sum("Quantity").alias("CountryQuantitySold"),
        )
    )


def calculate_market_share(cleaned_df):
    return (
        cleaned_df.withColumn(
            "Market",
            F.when(F.col("Country") == "United Kingdom", "United Kingdom").otherwise("Non-UK"),
        )
        .groupBy("Market")
        .agg(
            F.round(F.sum("Revenue"), 2).alias("MarketRevenue"),
            F.countDistinct("InvoiceNo").alias("MarketOrderCount"),
            F.countDistinct("CustomerID").alias("MarketCustomerCount"),
            F.sum("Quantity").alias("MarketQuantitySold"),
        )
        .withColumn(
            "RevenueSharePercent",
            F.round(
                F.col("MarketRevenue") / F.sum("MarketRevenue").over(Window.partitionBy()) * 100,
                2,
            ),
        )
        .orderBy(F.desc("MarketRevenue"))
    )


def calculate_potential_growth_markets(country_performance_df):
    return (
        country_performance_df.filter(F.col("Country") != "United Kingdom")
        .filter((F.col("CountryCustomerCount") >= 5) & (F.col("CountryOrderCount") >= 10))
        .withColumn(
            "RevenuePerCustomer",
            F.round(F.col("CountryRevenue") / F.col("CountryCustomerCount"), 2),
        )
        .withColumn(
            "RevenuePerOrder",
            F.round(F.col("CountryRevenue") / F.col("CountryOrderCount"), 2),
        )
        .orderBy(
            F.asc("RevenuePerCustomer"),
            F.desc("CountryCustomerCount"),
            F.desc("CountryOrderCount"),
        )
    )


def print_step8_country_analysis(cleaned_df):
    country_performance_df = calculate_country_performance(cleaned_df)

    print("\n=== Step 8: Top 10 Countries by Revenue ===")
    country_performance_df.orderBy(
        F.desc("CountryRevenue"),
        F.desc("CountryOrderCount"),
    ).show(10, truncate=False)

    print("\n=== Step 8: UK vs Non-UK Market Share ===")
    calculate_market_share(cleaned_df).show(truncate=False)

    print("\n=== Step 8: Potential Growth Markets ===")
    calculate_potential_growth_markets(country_performance_df).show(10, truncate=False)


def serialize_output_value(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def write_output_table(df, output_path, table_name):
    rows = df.collect()
    columns = df.columns
    table_path = os.path.join(output_path, table_name)
    os.makedirs(table_path, exist_ok=True)

    csv_path = os.path.join(table_path, f"{table_name}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                column: serialize_output_value(row[column])
                for column in columns
            })

    json_path = os.path.join(table_path, f"{table_name}.json")
    with open(json_path, "w", encoding="utf-8") as json_file:
        json.dump(
            [
                {
                    column: serialize_output_value(row[column])
                    for column in columns
                }
                for row in rows
            ],
            json_file,
            ensure_ascii=False,
            indent=2,
        )


def export_analysis_results(typed_df, cleaned_df, output_path):
    monthly_sales_df = calculate_monthly_sales_trend(cleaned_df)
    product_performance_df = calculate_product_performance(cleaned_df)
    customer_performance_df = calculate_customer_performance(cleaned_df)
    customer_rfm_df = calculate_customer_rfm(cleaned_df)
    country_performance_df = calculate_country_performance(cleaned_df)

    output_tables = {
        "overall_kpis": calculate_core_kpis(typed_df, cleaned_df),
        "monthly_sales_trend": monthly_sales_df,
        "weekday_sales": calculate_weekday_sales(cleaned_df),
        "hourly_sales": calculate_hourly_sales(cleaned_df),
        "monthly_revenue_extremes": calculate_monthly_revenue_extremes(monthly_sales_df),
        "top_products_by_quantity": product_performance_df.orderBy(
            F.desc("ProductQuantitySold"),
            F.desc("ProductRevenue"),
        ).limit(10),
        "top_products_by_revenue": product_performance_df.orderBy(
            F.desc("ProductRevenue"),
            F.desc("ProductQuantitySold"),
        ).limit(10),
        "bottom_products_by_revenue": product_performance_df.orderBy(
            F.asc("ProductRevenue"),
            F.asc("ProductQuantitySold"),
        ).limit(10),
        "top_customers": customer_performance_df.orderBy(
            F.desc("CustomerRevenue"),
            F.desc("CustomerOrderCount"),
        ).limit(10),
        "customer_rfm_top": customer_rfm_df.orderBy(
            F.desc("Monetary"),
            F.desc("Frequency"),
            F.asc("RecencyDays"),
        ).limit(10),
        "customer_segment_summary": calculate_customer_segment_summary(customer_rfm_df),
        "country_performance": country_performance_df.orderBy(
            F.desc("CountryRevenue"),
            F.desc("CountryOrderCount"),
        ),
        "top_countries_by_revenue": country_performance_df.orderBy(
            F.desc("CountryRevenue"),
            F.desc("CountryOrderCount"),
        ).limit(10),
        "market_share_uk_vs_non_uk": calculate_market_share(cleaned_df),
        "potential_growth_markets": calculate_potential_growth_markets(country_performance_df),
    }

    print("\n=== Step 10: Export Analysis Results ===")
    print(f"Output path: {output_path}")
    for table_name, table_df in output_tables.items():
        write_output_table(table_df, output_path, table_name)
        print(f"Exported {table_name}")


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT_PATH
    output_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_PATH
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = load_retail_data(spark, input_path)
        print_step2_profile(df, input_path)

        typed_df = prepare_typed_data(df)
        cleaned_df = clean_sales_data(typed_df)
        print_step3_profile(typed_df, cleaned_df)

        print_step4_kpis(typed_df, cleaned_df)

        print_step5_sales_analysis(cleaned_df)

        print_step6_product_analysis(cleaned_df)

        print_step7_customer_analysis(cleaned_df)

        print_step8_country_analysis(cleaned_df)

        export_analysis_results(typed_df, cleaned_df, output_path)
    finally:
        spark.stop()

# Count nulls for each column
print("Nulls in InvoiceNo", df.filter(df.InvoiceNo.isNull()).count())
print("Nulls in StockCode", df.filter(df.StockCode.isNull()).count())
print("Nulls in Description", df.filter(df.Description.isNull()).count())
print("Nulls in Quantity", df.filter(df.Quantity.isNull()).count())
print("Nulls in InvoiceDate", df.filter(df.InvoiceDate.isNull()).count())
print("Nulls in UnitPrice", df.filter(df.UnitPrice.isNull()).count())
print("Nulls in CustomerID", df.filter(df.CustomerID.isNull()).count())
print("Nulls in Country", df.filter(df.Country.isNull()).count())

if __name__ == "__main__":
    main()
