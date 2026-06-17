from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("src/data/Online Retail.csv", header=True, inferSchema=True, sep=";")

df.show(30)



