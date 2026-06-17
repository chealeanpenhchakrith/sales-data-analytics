from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# Create a spark session
spark = SparkSession.builder.getOrCreate()

# Load the dataset with spark
df = spark.read.csv("src/data/Online Retail.csv", header=True, inferSchema=True, sep=";")

# Show the first 30 rows
df.show(30)

# Show column data types
df.printSchema()

# Count nulls for each column
print("Nulls in InvoiceNo", df.filter(df.InvoiceNo.isNull()).count())
print("Nulls in StockCode", df.filter(df.StockCode.isNull()).count())
print("Nulls in Description", df.filter(df.Description.isNull()).count())
print("Nulls in Quantity", df.filter(df.Quantity.isNull()).count())
print("Nulls in InvoiceDate", df.filter(df.InvoiceDate.isNull()).count())
print("Nulls in UnitPrice", df.filter(df.UnitPrice.isNull()).count())
print("Nulls in CustomerID", df.filter(df.CustomerID.isNull()).count())
print("Nulls in Country", df.filter(df.Country.isNull()).count())

# Count negative values in Quantity's column
print("Negative values in Quantity", df.filter(df.Quantity < 0).count())

# Create a new dataframe with non-null values
df_clean = df.filter(df.CustomerID.isNotNull())

df_clean.printSchema()

# Check that the CustomerID's column doesn't contain null values
print("Nulls in CustomerID", df_clean.filter(df_clean.CustomerID.isNull()).count())

# Count the number of total gross revenue from sales
total_revenue = df_clean.select(F.sum(df_clean.Quantity * df_clean.UnitPrice.cast("float"))).collect()[0][0]
print("Total sale revenues : ", total_revenue, "$")

total_number_product_sold = df_clean.filter(df_clean.Quantity > 0).select(F.sum(df_clean.Quantity)).collect()[0][0]
print("The total number of product sold is", total_number_product_sold)