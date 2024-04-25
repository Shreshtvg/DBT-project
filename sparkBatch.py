from pyspark.sql.functions import col, split, count
from pyspark.sql.functions import window, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType
from pyspark.sql import SparkSession
import random
topic = ""
print("WELCOME TO SPARK BATCH CONSUMER")
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

spark = SparkSession.builder.appName("YoutubeBatch").getOrCreate()

# Define the schema for the Kafka topic
schema = StructType([
    StructField("channelId", StringType(), True),
    StructField("likesPerSubscriber", DoubleType(), True),
    StructField("videoId", StringType(), True),
    StructField("dislikesPerSubscriber", DoubleType(), True),
    StructField("viewsPerElapsedTime", DoubleType(), True),
    StructField("videoViewCount", DoubleType(), True),
    StructField("videoPublished", DoubleType(), True),
    StructField("channelViewCount", DoubleType(), True),
    StructField("videoCount", DoubleType(), True),
    StructField("subscriberCount", DoubleType(), True),
    StructField("channelelapsedtime", DoubleType(), True),
    StructField("channelCommentCount", DoubleType(), True)
])

# Read the Kafka topic as a streaming DataFrame
df_Youtube = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "videoId") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data in the value column
df_Youtube_parsed = df_Youtube.select(from_json(col("videoId").cast("string"), schema).alias("video_data"), col("videoPublished"))

# Extract the fields from the parsed JSON data
df_Youtube_processed = df_Youtube_parsed.select("video_data.*", "videoPublished") \
    .withColumn("videoPublished", to_timestamp("videoPublished", "yyyy-MM-dd HH:mm:ss"))

# Define the window duration for the batch processing
window_duration = "5 minutes" 

# Group the videos by channel and timestamp window and count the videos
df_Youtube_counts = df_Youtube_processed \
    .withWatermark("videoPublished", window_duration) \
    .groupBy(window("videoPublished", window_duration), "channelId") \
    .agg(count("*").alias("video_count"))

# Print the results to the console
query_Youtube = df_Youtube_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for the query to terminate
query_tweets.awaitTermination()