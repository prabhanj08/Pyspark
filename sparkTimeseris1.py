"""
PySpark Time Series Interview Coding Question:

You are given a PySpark DataFrame df with time series data containing the following columns: timestamp (timestamp), value (numeric).
Write a PySpark code to calculate the moving average of the value column over a window of the last 3 timestamps for each row.

+-------------------+-----+--------------+
|          timestamp|value|moving_average|
+-------------------+-----+--------------+
|2022-01-01 00:00:00|   10|          null|
|2022-01-01 00:05:00|   20|          null|
|2022-01-01 00:10:00|   30|          20.0|
|2022-01-01 00:15:00|   40|          30.0|
|2022-01-01 00:20:00|   50|          40.0|
|2022-01-01 00:25:00|   60|          50.0|
+-------------------+-----+--------------+

"""



from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Create or get SparkSession
spark = SparkSession.builder.appName("TimeSeriesInterview").getOrCreate()

# Sample DataFrame
data = [
    ("2022-01-01 00:00:00", 10),
    ("2022-01-01 00:05:00", 20),
    ("2022-01-01 00:10:00", 30),
    ("2022-01-01 00:15:00", 40),
    ("2022-01-01 00:20:00", 50),
    ("2022-01-01 00:25:00", 60)
]

columns = ["timestamp", "value"]
df = spark.createDataFrame(data, columns)

# Convert 'timestamp' column to timestamp type
df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

# Create a window specification to calculate moving average over the last 3 timestamps
windowSpec = Window.orderBy("timestamp").rowsBetween(-2, 0)

# Calculate the moving average and add it as a new column
df = df.withColumn("moving_average", F.avg("value").over(windowSpec))

df.show()
