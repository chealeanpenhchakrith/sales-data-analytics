#!/usr/bin/env python3
import csv
import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "output"
FULL_ANALYSIS_LOG = PROJECT_ROOT / "output" / "demo_full_analysis.log"
HDFS_ANALYSIS_LOG = PROJECT_ROOT / "output" / "hdfs_analysis.log"
HDFS_PATH = "hdfs://namenode:9000/data/online-retail/online_retail.csv"
CONTAINER_OUTPUT_DIR = "/app/output"
EXPECTED_TABLES = [
    "overall_kpis",
    "monthly_sales_trend",
    "weekday_sales",
    "hourly_sales",
    "monthly_revenue_extremes",
    "top_products_by_quantity",
    "top_products_by_revenue",
    "bottom_products_by_revenue",
    "top_customers",
    "customer_rfm_top",
    "customer_segment_summary",
    "country_performance",
    "top_countries_by_revenue",
    "market_share_uk_vs_non_uk",
    "potential_growth_markets",
]


def clear_screen():
    print("\033[2J\033[H", end="")


def pause():
    input("\nPress Enter to continue...")


def money(value):
    return f"{float(value):,.2f}"


def integer(value):
    return f"{int(float(value)):,}"


def print_header(title):
    clear_screen()
    print("=" * 72)
    print(title)
    print("=" * 72)


def run_command(command, capture_to=None):
    print(f"\n$ {' '.join(command)}\n")
    if capture_to is None:
        completed = subprocess.run(command, cwd=PROJECT_ROOT)
        return_code = completed.returncode
    else:
        capture_to.parent.mkdir(parents=True, exist_ok=True)
        with capture_to.open("w", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                command,
                cwd=PROJECT_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert process.stdout is not None
            for line in process.stdout:
                print(line, end="", flush=True)
                log_file.write(line)
                log_file.flush()
            return_code = process.wait()
        print(f"Command output saved to: {capture_to}")

    if return_code != 0:
        print(f"\nCommand failed with exit code {return_code}.")
    return return_code


def read_table(table_name):
    csv_path = OUTPUT_DIR / table_name / f"{table_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Missing {csv_path.relative_to(PROJECT_ROOT)}. "
            "Run Main Menu -> Run Full Analysis first."
        )

    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


def require_exported_results(table_names):
    missing = [
        table_name
        for table_name in table_names
        if not (OUTPUT_DIR / table_name / f"{table_name}.csv").exists()
    ]
    if missing:
        print("Missing exported result tables:")
        for table_name in missing:
            print(f"- output/{table_name}/{table_name}.csv")
        print("\nRun Main Menu -> Run Full Analysis first.")
        return False
    return True


def print_rows(rows, columns, limit=10):
    if not rows:
        print("No rows found.")
        return

    rows = rows[:limit]
    widths = {}
    for column in columns:
        widths[column] = max(
            len(column),
            max(len(str(row.get(column, ""))) for row in rows),
        )
        widths[column] = min(widths[column], 36)

    print(" | ".join(column[:widths[column]].ljust(widths[column]) for column in columns))
    print("-+-".join("-" * widths[column] for column in columns))
    for row in rows:
        values = []
        for column in columns:
            value = str(row.get(column, ""))
            if len(value) > widths[column]:
                value = value[: widths[column] - 3] + "..."
            values.append(value.ljust(widths[column]))
        print(" | ".join(values))


def check_hdfs_services():
    print_header("Hadoop HDFS Integration - Check HDFS Services")
    run_command(["docker", "compose", "up", "-d", "namenode", "datanode", "spark-master", "spark-worker"])
    run_command(["docker", "compose", "ps"])
    run_command(["docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-ls", "/"])


def upload_dataset_to_hdfs():
    print_header("Hadoop HDFS Integration - Upload Dataset to HDFS")
    run_command(["bash", "scripts/upload_data_to_hdfs.sh"])


def show_hdfs_dataset_path():
    print_header("Hadoop HDFS Integration - HDFS Dataset Path")
    print(f"HDFS dataset path:\n{HDFS_PATH}\n")
    print("Verification command:")
    print("docker compose exec namenode hdfs dfs -ls -h /data/online-retail")
    run_command([
        "docker",
        "compose",
        "exec",
        "-T",
        "namenode",
        "hdfs",
        "dfs",
        "-ls",
        "-h",
        "/data/online-retail",
    ])


def run_full_analysis(title="Run Full Analysis from HDFS", log_path=FULL_ANALYSIS_LOG):
    print_header(title)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(OUTPUT_DIR, 0o777)

    print("Step 1: Ensure Spark and HDFS services are running.")
    if run_command(["docker", "compose", "up", "-d", "namenode", "datanode", "spark-master", "spark-worker"]) != 0:
        return

    print("\nStep 2: Upload or refresh the dataset in HDFS.")
    if run_command(["bash", "scripts/upload_data_to_hdfs.sh"]) != 0:
        return

    print("\nStep 3: Run the complete PySpark analysis from HDFS.")
    run_command([
        "docker",
        "compose",
        "exec",
        "-T",
        "-e",
        "PYTHONUNBUFFERED=1",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "/app/src/script.py",
        HDFS_PATH,
        CONTAINER_OUTPUT_DIR,
    ], capture_to=log_path)

    print_exported_files_summary()
    print("\nAfter completion, use Apache Spark Analytics to browse the exported results.")


def run_hdfs_analysis():
    run_full_analysis(
        title="Hadoop HDFS Integration - Run Analysis from HDFS",
        log_path=HDFS_ANALYSIS_LOG,
    )


def show_load_dataset():
    print_header("Apache Spark Analytics - Load Dataset")
    print("This menu item is a result viewer. It does not launch a Spark job.\n")
    print(f"Input path used by full analysis:\n{HDFS_PATH}\n")
    print("Validated dataset summary:")
    print(f"{'Raw row count:':32} 541,909")
    print(f"{'Column count:':32} 8")
    print(f"{'Delimiter:':32} semicolon (;)")
    print(f"{'Encoding:':32} UTF-8")
    print(f"{'CSV header:':32} enabled")
    print("\nColumns:")
    print("InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country")
    print("\nKnown missing-value fields:")
    print("- Description")
    print("- CustomerID")
    existing_logs = [path for path in [FULL_ANALYSIS_LOG, HDFS_ANALYSIS_LOG] if path.exists()]
    if existing_logs:
        print("\nAvailable run logs:")
        for path in existing_logs:
            print(f"- {path}")


def show_clean_transform():
    print_header("Apache Spark Analytics - Clean and Transform")
    print("This menu item is a result viewer. It does not launch a Spark job.\n")
    print("Cleaning summary validated by the project verification scripts:")
    print(f"{'Raw rows:':48} 541,909")
    print(f"{'Rows after cleaning for regular sales analysis:':48} 397,884")
    print(f"{'Cancelled invoice rows excluded:':48} 9,288")
    print(f"{'Rows with empty Description:':48} 1,454")
    print(f"{'Rows with empty CustomerID:':48} 135,080")
    print(f"{'Rows with invalid Quantity:':48} 10,624")
    print(f"{'Rows with invalid UnitPrice:':48} 2,517")
    print("\nDerived fields:")
    print("Revenue, InvoiceDateOnly, Year, Month, YearMonth, DayOfWeek, Hour")


def show_kpi_summary():
    print_header("Apache Spark Analytics - KPI Summary")
    if not require_exported_results(["overall_kpis"]):
        return
    row = read_table("overall_kpis")[0]
    print(f"{'Total Revenue:':34} {money(row['TotalRevenue'])}")
    print(f"{'Total Orders:':34} {integer(row['TotalOrders'])}")
    print(f"{'Total Quantity Sold:':34} {integer(row['TotalQuantitySold'])}")
    print(f"{'Total Customers:':34} {integer(row['TotalCustomers'])}")
    print(f"{'Total Products:':34} {integer(row['TotalProducts'])}")
    print(f"{'Total Countries:':34} {integer(row['TotalCountries'])}")
    print(f"{'Average Order Value:':34} {money(row['AverageOrderValue'])}")
    print(f"{'Average Revenue Per Customer:':34} {money(row['AverageRevenuePerCustomer'])}")
    print(f"{'Cancelled Orders:':34} {integer(row['CancelledOrders'])}")
    print(f"{'Total Raw Invoices:':34} {integer(row['TotalRawInvoices'])}")
    print(f"{'Cancellation Rate:':34} {row['CancellationRatePercent']}%")


def show_sales_analysis():
    print_header("Apache Spark Analytics - Sales Analysis")
    required = ["monthly_sales_trend", "weekday_sales", "hourly_sales", "monthly_revenue_extremes"]
    if not require_exported_results(required):
        return

    print("Highest and lowest revenue months:")
    print_rows(read_table("monthly_revenue_extremes"), [
        "Metric",
        "YearMonth",
        "MonthlyRevenue",
        "MonthlyOrders",
        "MonthlyQuantitySold",
    ], limit=10)

    print("\nMonthly sales trend:")
    print_rows(read_table("monthly_sales_trend"), [
        "YearMonth",
        "MonthlyRevenue",
        "MonthlyOrders",
        "MonthlyQuantitySold",
    ], limit=24)

    print("\nSales by day of week:")
    print_rows(read_table("weekday_sales"), [
        "DayOfWeek",
        "WeekdayRevenue",
        "WeekdayOrders",
        "WeekdayQuantitySold",
    ], limit=7)

    print("\nTop hours by revenue:")
    hourly = sorted(read_table("hourly_sales"), key=lambda row: float(row["HourlyRevenue"]), reverse=True)
    print_rows(hourly, ["Hour", "HourlyRevenue", "HourlyOrders", "HourlyQuantitySold"], limit=10)


def show_product_analysis():
    print_header("Apache Spark Analytics - Product Analysis")
    required = ["top_products_by_quantity", "top_products_by_revenue", "bottom_products_by_revenue"]
    if not require_exported_results(required):
        return

    print("Top products by revenue:")
    print_rows(read_table("top_products_by_revenue"), [
        "StockCode",
        "Description",
        "ProductQuantitySold",
        "ProductRevenue",
        "ProductOrderCount",
    ], limit=10)

    print("\nTop products by quantity:")
    print_rows(read_table("top_products_by_quantity"), [
        "StockCode",
        "Description",
        "ProductQuantitySold",
        "ProductRevenue",
        "ProductOrderCount",
    ], limit=10)

    print("\nLow-revenue products:")
    print_rows(read_table("bottom_products_by_revenue"), [
        "StockCode",
        "Description",
        "ProductQuantitySold",
        "ProductRevenue",
        "ProductOrderCount",
    ], limit=10)


def show_customer_analysis():
    print_header("Apache Spark Analytics - Customer Analysis")
    required = ["top_customers", "customer_rfm_top", "customer_segment_summary"]
    if not require_exported_results(required):
        return

    print("Top customers:")
    print_rows(read_table("top_customers"), [
        "CustomerID",
        "CustomerRevenue",
        "CustomerOrderCount",
        "CustomerQuantityPurchased",
        "LastPurchaseDate",
    ], limit=10)

    print("\nTop RFM customers:")
    print_rows(read_table("customer_rfm_top"), [
        "CustomerID",
        "Monetary",
        "Frequency",
        "RecencyDays",
        "CustomerSegment",
    ], limit=10)

    print("\nCustomer segment summary:")
    print_rows(read_table("customer_segment_summary"), [
        "CustomerSegment",
        "CustomerCount",
        "SegmentRevenue",
        "AverageFrequency",
        "AverageRecencyDays",
    ], limit=10)


def show_country_analysis():
    print_header("Apache Spark Analytics - Country Analysis")
    required = ["top_countries_by_revenue", "market_share_uk_vs_non_uk", "potential_growth_markets"]
    if not require_exported_results(required):
        return

    print("Top countries by revenue:")
    print_rows(read_table("top_countries_by_revenue"), [
        "Country",
        "CountryRevenue",
        "CountryOrderCount",
        "CountryCustomerCount",
        "CountryQuantitySold",
    ], limit=10)

    print("\nUK vs Non-UK market share:")
    print_rows(read_table("market_share_uk_vs_non_uk"), [
        "Market",
        "MarketRevenue",
        "MarketOrderCount",
        "MarketCustomerCount",
        "RevenueSharePercent",
    ], limit=10)

    print("\nPotential growth markets:")
    print_rows(read_table("potential_growth_markets"), [
        "Country",
        "CountryRevenue",
        "CountryOrderCount",
        "CountryCustomerCount",
        "RevenuePerCustomer",
        "RevenuePerOrder",
    ], limit=10)


def view_exported_files():
    print_header("View Exported Files")
    if not OUTPUT_DIR.exists():
        print("No output directory found. Run Main Menu -> Run Full Analysis first.")
        return

    found = sorted(path for path in OUTPUT_DIR.glob("*/*") if path.is_file())
    if not found:
        print("No exported files found. Run Main Menu -> Run Full Analysis first.")
        return

    print("Exported files:")
    for path in found:
        print(f"- {path.relative_to(PROJECT_ROOT)}")

    missing = [
        table_name
        for table_name in EXPECTED_TABLES
        if not (OUTPUT_DIR / table_name / f"{table_name}.csv").exists()
        or not (OUTPUT_DIR / table_name / f"{table_name}.json").exists()
    ]
    if missing:
        print("\nMissing expected tables:")
        for table_name in missing:
            print(f"- {table_name}")


def print_exported_files_summary():
    print("\nExported result files:")
    if not OUTPUT_DIR.exists():
        print("- No output directory found.")
        return

    for table_name in EXPECTED_TABLES:
        csv_path = OUTPUT_DIR / table_name / f"{table_name}.csv"
        json_path = OUTPUT_DIR / table_name / f"{table_name}.json"
        if csv_path.exists() and json_path.exists():
            print(f"- {csv_path.relative_to(PROJECT_ROOT)}")
            print(f"- {json_path.relative_to(PROJECT_ROOT)}")
        else:
            print(f"- Missing output/{table_name}/")


def apache_spark_analytics_menu():
    options = {
        "1": ("Load Dataset", show_load_dataset),
        "2": ("Clean and Transform", show_clean_transform),
        "3": ("KPI Summary", show_kpi_summary),
        "4": ("Sales Analysis", show_sales_analysis),
        "5": ("Product Analysis", show_product_analysis),
        "6": ("Customer Analysis", show_customer_analysis),
        "7": ("Country Analysis", show_country_analysis),
        "8": ("Export Results", view_exported_files),
    }
    run_menu("Apache Spark Analytics", options)


def hadoop_hdfs_integration_menu():
    options = {
        "1": ("Check HDFS Services", check_hdfs_services),
        "2": ("Upload Dataset to HDFS", upload_dataset_to_hdfs),
        "3": ("Show HDFS Dataset Path", show_hdfs_dataset_path),
        "4": ("Run Analysis from HDFS", run_hdfs_analysis),
    }
    run_menu("Hadoop HDFS Integration", options)


def run_menu(title, options):
    while True:
        print_header(title)
        for key, (label, _) in options.items():
            print(f"{key}. {label}")
        print("0. Back to Main Menu" if title != "Sales Data Analytics Demo" else "0. Exit")

        try:
            choice = input("\nSelect an option: ").strip()
        except EOFError:
            return
        if choice == "0":
            return

        selected = options.get(choice)
        if selected is None:
            print("\nInvalid option.")
            pause()
            continue

        _, action = selected
        try:
            action()
        except KeyboardInterrupt:
            print("\nInterrupted.")
        except Exception as exc:
            print(f"\nError: {exc}")
        pause()


def main():
    options = {
        "1": ("Run Full Analysis", run_full_analysis),
        "2": ("Apache Spark Analytics", apache_spark_analytics_menu),
        "3": ("Hadoop HDFS Integration", hadoop_hdfs_integration_menu),
        "4": ("View Exported Files", view_exported_files),
    }
    run_menu("Sales Data Analytics Demo", options)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\nGoodbye.")
