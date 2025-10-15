# Import the necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Task3_WindowedAnalytics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("TASK 3: Windowed Time-Based Analytics")
print("=" * 60)

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
print("\n✓ Connecting to socket localhost:9999...")
streaming_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

print("✓ Connection established!")

# Parse JSON data into columns using the defined schema
print("✓ Parsing incoming JSON data...")
parsed_df = streaming_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
print("✓ Converting timestamps to proper type...")
df_with_timestamp = parsed_df.withColumn(
    "event_time", 
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
).withWatermark("event_time", "1 minute")

print("✓ Setting up 5-minute sliding windows (sliding by 1 minute)...\n")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_aggregation = df_with_timestamp \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute")
    ) \
    .agg(
        sum("fare_amount").alias("total_fare")
    )

# Extract window start and end times as separate columns
windowed_results = windowed_aggregation.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# Define a function to write each batch to a CSV file with column names
def write_batch_to_csv(batch_df, batch_id):
    """
    Write each micro-batch to a separate CSV file with headers
    """
    if batch_df.count() > 0:
        output_path = f"outputs/task_3/batch_{batch_id}.csv"
        # Save the batch DataFrame as a CSV file with headers included
        batch_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(output_path)
        print(f"✓ Batch {batch_id} written to {output_path}")
        
        # Display results
        print(f"\n--- Batch {batch_id} Windowed Results ---")
        batch_df.orderBy("window_start").show(truncate=False)

# Use foreachBatch to apply the function to each micro-batch
print("Starting streaming query...")
print("Results will be saved to outputs/task_3/")
print("Analysis: 5-minute windows sliding by 1 minute\n")

query = windowed_results.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch_to_csv) \
    .trigger(processingTime='15 seconds') \
    .start()

print("✓ Streaming started! Press Ctrl+C to stop.\n")

query.awaitTermination()
