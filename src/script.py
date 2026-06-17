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
print("Nulls in InvoiceNo", df.filter(df.InvoiceDate.isNull()).count())
print("Nulls in UnitPrice", df.filter(df.UnitPrice.isNull()).count())
print("Nulls in CustomerID", df.filter(df.CustomerID.isNull()).count())
print("Nulls in Country", df.filter(df.Country.isNull()).count())

