from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

conf = SparkConf().setMaster("local[*]").setAppName("Walmart-Analysis")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "C:/Users/Mohan/Downloads/WMT.csv")
df.show()

df1 = df.count()
print(df1)

# To find the column names
print(df.columns)

# To print the schema
df.printSchema()

# To print the first 5 lines
df.show(5)

# Use describe() to learn about the DataFrame.
descdf = df.describe()
descdf.show()

# There are too many decimal places for mean and stddev in the describe() dataframe. Format the numbers to just show up to two decimal places
decdf = descdf.filter(col("summary").isin(["mean", "stddev", "min", "max"]))
decdf.show()

findecdf = decdf.withColumn("Open", col("Open").cast("decimal(10,2)")) \
    .withColumn("High", col("High").cast("decimal(10,2)")) \
    .withColumn("Low", col("Low").cast("decimal(10,2)")) \
    .withColumn("Close", col("Close").cast("decimal(10,2)")) \
    .withColumn("Adj Close", col("Adj Close").cast("decimal(10,2)")) \
    .withColumn("Close", col("Close").cast("decimal(10,2)")) \
    .withColumn("Volume", col("Volume").cast("decimal(10,2)"))
findecdf.show()

# Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.
hvdf = df.withColumn("HV Ratio", expr("High/Volume"))
hvdf.show(5, False)

# What is the mean of the Close column?
meandf = df.select(mean("Close").alias("mean").cast("decimal(10,2)"))
meandf.show()

# What is the max and min of the Volume column?
maxdf = df.select(max("Volume").alias("MaxVolume"), min("Volume").alias("MinVolume"))
maxdf.show()

# How many days was the Close lower than 60 dollars?
closecountdf = df.filter(col("Close") < 60).count()
print(closecountdf)

# What percentage of the time was the High greater than 80 dollars ?
highpercdf = df.filter(col("High") > 80).count() * 100 / df1
print(highpercdf)

# What is the max High per year?
maxhighdf = df.withColumn("year", year("Date")).groupBy("year").agg(
    max("High").cast("decimal(10,2)").alias("High")).orderBy("year")
maxhighdf.show()

# What is the average Close for each Calendar Month?
avgclosedf = df.withColumn("month", month("Date")).groupBy("month").agg(
    avg("Close").cast("decimal(10,2)").alias("average")).orderBy("month")
avgclosedf.show()
