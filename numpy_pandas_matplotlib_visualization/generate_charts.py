#!/usr/bin/env python3
import os
from pathlib import Path


VISUALIZATION_DIR = Path(__file__).resolve().parent
GENERATED_DIR = VISUALIZATION_DIR / "generated"
os.environ.setdefault("MPLCONFIGDIR", str(GENERATED_DIR / ".matplotlib_cache"))

try:
    import numpy as np
    import pandas as pd
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:
    raise SystemExit(
        f"Missing dependency: {exc.name}\n"
        "Install dependencies with:\n"
        "python3 -m pip install -r numpy_pandas_matplotlib_visualization/requirements.txt"
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "output"


def table_path(table_name):
    return OUTPUT_DIR / table_name / f"{table_name}.csv"


def read_table(table_name):
    path = table_path(table_name)
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path.relative_to(PROJECT_ROOT)}. Run the full analysis first."
        )
    return pd.read_csv(path)


def format_currency_axis(axis):
    axis.get_yaxis().set_major_formatter(
        plt.FuncFormatter(lambda value, _: f"${value / 1_000_000:.1f}M")
    )


def save_figure(filename):
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    output_path = GENERATED_DIR / filename
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    return output_path


def monthly_revenue_trend():
    df = read_table("monthly_sales_trend")
    x = np.arange(len(df))
    revenue = df["MonthlyRevenue"].to_numpy(dtype=float)

    plt.figure(figsize=(11, 5))
    plt.plot(x, revenue, marker="o", linewidth=2.5, color="#2563eb")
    plt.fill_between(x, revenue, alpha=0.12, color="#2563eb")
    plt.xticks(x, df["YearMonth"], rotation=35, ha="right")
    format_currency_axis(plt.gca())
    plt.title("Monthly Revenue Trend")
    plt.xlabel("Month")
    plt.ylabel("Revenue")
    plt.grid(axis="y", alpha=0.25)
    return save_figure("01_monthly_revenue_trend.png")


def top_products_revenue():
    df = read_table("top_products_by_revenue").head(10).copy()
    df = df.sort_values("ProductRevenue", ascending=True)
    labels = df["Description"].str.slice(0, 34)
    values = df["ProductRevenue"].to_numpy(dtype=float)

    plt.figure(figsize=(11, 6))
    plt.barh(labels, values, color="#0f766e")
    plt.title("Top Products by Revenue")
    plt.xlabel("Revenue")
    plt.grid(axis="x", alpha=0.25)
    plt.gca().get_xaxis().set_major_formatter(
        plt.FuncFormatter(lambda value, _: f"${value / 1000:.0f}K")
    )
    return save_figure("02_top_products_revenue.png")


def top_countries_revenue():
    df = read_table("top_countries_by_revenue").head(10).copy()
    df = df.sort_values("CountryRevenue", ascending=True)
    values = df["CountryRevenue"].to_numpy(dtype=float)

    plt.figure(figsize=(11, 6))
    plt.barh(df["Country"], values, color="#7c3aed")
    plt.title("Top Countries by Revenue")
    plt.xlabel("Revenue")
    plt.grid(axis="x", alpha=0.25)
    plt.gca().get_xaxis().set_major_formatter(
        plt.FuncFormatter(lambda value, _: f"${value / 1_000_000:.1f}M")
    )
    return save_figure("03_top_countries_revenue.png")


def market_share_pie():
    df = read_table("market_share_uk_vs_non_uk")
    values = df["MarketRevenue"].to_numpy(dtype=float)
    percentages = values / np.sum(values) * 100
    labels = [
        f"{market}\n{percent:.1f}%"
        for market, percent in zip(df["Market"], percentages)
    ]

    plt.figure(figsize=(7, 7))
    plt.pie(
        values,
        labels=labels,
        startangle=90,
        colors=["#2563eb", "#0f766e"],
        wedgeprops={"edgecolor": "white", "linewidth": 1.5},
    )
    plt.title("UK vs Non-UK Revenue Share")
    return save_figure("04_market_share_pie.png")


def customer_segments_pie():
    df = read_table("customer_segment_summary")
    values = df["CustomerCount"].to_numpy(dtype=float)
    percentages = values / np.sum(values) * 100
    labels = [
        f"{segment}\n{percent:.1f}%"
        for segment, percent in zip(df["CustomerSegment"], percentages)
    ]

    plt.figure(figsize=(7, 7))
    plt.pie(
        values,
        labels=labels,
        startangle=90,
        colors=["#15803d", "#2563eb", "#7c3aed", "#be123c"],
        wedgeprops={"edgecolor": "white", "linewidth": 1.5},
    )
    plt.title("Customer Segment Distribution")
    return save_figure("05_customer_segments_pie.png")


def weekday_revenue():
    df = read_table("weekday_sales")
    values = df["WeekdayRevenue"].to_numpy(dtype=float)

    plt.figure(figsize=(9, 5))
    plt.bar(df["DayOfWeek"], values, color="#b45309")
    plt.title("Revenue by Day of Week")
    plt.xlabel("Day of Week")
    plt.ylabel("Revenue")
    plt.grid(axis="y", alpha=0.25)
    format_currency_axis(plt.gca())
    return save_figure("06_weekday_revenue.png")


def hourly_orders():
    df = read_table("hourly_sales")
    x = df["Hour"].to_numpy(dtype=int)
    orders = df["HourlyOrders"].to_numpy(dtype=float)

    plt.figure(figsize=(10, 5))
    plt.plot(x, orders, marker="o", linewidth=2.5, color="#be123c")
    plt.title("Orders by Hour of Day")
    plt.xlabel("Hour")
    plt.ylabel("Orders")
    plt.xticks(x)
    plt.grid(axis="y", alpha=0.25)
    return save_figure("07_hourly_orders.png")


def write_index(chart_paths):
    cards = "\n".join(
        f"""
        <section>
          <h2>{path.stem.replace('_', ' ').title()}</h2>
          <img src="{path.name}" alt="{path.stem}" />
        </section>
        """
        for path in chart_paths
    )
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Sales Data Visualizations</title>
  <style>
    body {{
      margin: 0;
      background: #f4f6f8;
      color: #1f2937;
      font-family: Arial, Helvetica, sans-serif;
    }}
    header {{
      background: #ffffff;
      border-bottom: 1px solid #d0d5dd;
      padding: 28px 36px;
    }}
    h1 {{
      margin: 0;
      font-size: 30px;
    }}
    main {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 24px;
      display: grid;
      gap: 18px;
    }}
    section {{
      background: #ffffff;
      border: 1px solid #d0d5dd;
      border-radius: 8px;
      padding: 18px;
    }}
    h2 {{
      margin: 0 0 12px;
      font-size: 18px;
    }}
    img {{
      display: block;
      width: 100%;
      height: auto;
    }}
  </style>
</head>
<body>
  <header>
    <h1>Sales Data Visualizations</h1>
  </header>
  <main>
    {cards}
  </main>
</body>
</html>
"""
    index_path = GENERATED_DIR / "index.html"
    index_path.write_text(html, encoding="utf-8")
    return index_path


def main():
    chart_paths = [
        monthly_revenue_trend(),
        top_products_revenue(),
        top_countries_revenue(),
        market_share_pie(),
        customer_segments_pie(),
        weekday_revenue(),
        hourly_orders(),
    ]
    index_path = write_index(chart_paths)

    print("Generated visualization files:")
    for path in chart_paths:
        print(f"- {path}")
    print(f"- {index_path}")


if __name__ == "__main__":
    main()
