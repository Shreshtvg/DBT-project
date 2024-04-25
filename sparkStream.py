from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
from pandas import *
import random
import time
topic = ""
print("WELCOME TO SPARK STREAMING CONSUMER")
print("Choose one of the options: \n 1. Net views per channel \n 2. Channels with over 50000 subscribers \n 3. Top 10 videos based on like count \n 4. Channels with high engagement \n")
choice = random.randint(1, 4)
print("The chosen topic is : ", choice)
if choice == 1:
    topic = "total_views_per_channel"
elif choice == 2:
    topic = "channels_high_subscribers"
elif choice == 3:
    topic = "videos_top_likes"
elif choice == 4:
    topic = "top_channels_engagement"

# create a SparkSession object
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .master("local[*]") \
    .getOrCreate()

# Define the schema for the incoming data
schema = StructType([
    StructField("channelId", StringType()),
    StructField("likesPerSubscriber", DoubleType()),
    StructField("videoId", StringType()),
    StructField("dislikesPerSubscriber", DoubleType()),
    StructField("viewsPerElapsedTime", DoubleType()),
    StructField("videoViewCount", DoubleType()),
    StructField("videoPublished", DoubleType()),
    StructField("channelViewCount", DoubleType()),
    StructField("videoCount", DoubleType()),
    StructField("subscriberCount", DoubleType()),
    StructField("channelelapsedtime", DoubleType()),
    StructField("channelCommentCount", DoubleType())
])

# Create the streaming dataframe
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Define the window and group by clauses
windowedCounts = df \
    .withWatermark("videoPublished", "15 minutes") \
    .groupBy(
        window(col("videoPublished"), "15 minutes"),
        col("videoId")
    ) \
    .agg(count("channelId").alias("videoCount"))

# Sort the data in ascending order by the window start time and video IDs 
sortedCounts = windowedCounts \
    .sort(
        col("window.start").asc(),
        col("videoId").asc()
    )

# Write the output to the console
console_query = sortedCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Write the output to a CSV file
csv_query = sortedCounts \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epochId: df.toPandas().to_csv("output.csv", index=False, header=True)) \
    .start()

# Wait for the queries to terminate
console_query.awaitTermination()
csv_query.awaitTermination()

time.sleep(100)

# Stop the queries
console_query.stop()
csv_query.stop()
