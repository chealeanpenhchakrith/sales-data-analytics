# NumPy / Pandas / Matplotlib Visualization

This folder is independent from the main Spark/HDFS program.

It reads the exported CSV files from `output/` and creates simple chart images using:

- NumPy for numeric arrays and percentage calculations
- Pandas for CSV loading and data preparation
- Matplotlib for chart rendering

It does not modify `src/script.py`, the terminal menu, Docker, HDFS, or Spark code.

## Install dependencies

If the libraries are not installed:

```bash
python3 -m pip install -r numpy_pandas_matplotlib_visualization/requirements.txt
```

Or with the project virtual environment:

```bash
.venv/bin/python -m pip install -r numpy_pandas_matplotlib_visualization/requirements.txt
```

## Generate charts

Run from the project root:

```bash
python3 numpy_pandas_matplotlib_visualization/generate_charts.py
```

Generated files:

```text
numpy_pandas_matplotlib_visualization/generated/
```

Charts generated:

- `01_monthly_revenue_trend.png`
- `02_top_products_revenue.png`
- `03_top_countries_revenue.png`
- `04_market_share_pie.png`
- `05_customer_segments_pie.png`
- `06_weekday_revenue.png`
- `07_hourly_orders.png`
- `index.html`

Open `generated/index.html` in a browser to view all charts together.

If `output/` is missing, run the full analysis first:

```bash
python3 scripts/demo_menu.py
```

Then choose:

```text
1. Run Full Analysis
```
