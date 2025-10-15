from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Task2_DriverAggregations") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("TASK 2: Real-Time Driver Aggregations")
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

# Parse JSON data into columns using the defined schema
print("✓ Parsing incoming JSON data...")
parsed_df = streaming_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
print("✓ Converting timestamps and adding watermark...")
df_with_timestamp = parsed_df.withColumn(
    "timestamp", 
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
).withWatermark("timestamp", "10 minutes")

# Compute aggregations: total fare and average distance grouped by driver_id
print("✓ Computing aggregations by driver...\n")
driver_aggregations = df_with_timestamp.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Define a function to write each batch to a CSV file
def write_batch_to_csv(batch_df, batch_id):
    """
    Write each micro-batch to a separate CSV file
    """
    if batch_df.count() > 0:
        output_path = f"outputs/task_2/batch_{batch_id}.csv"
        batch_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(output_path)
        print(f"✓ Batch {batch_id} written to {output_path}")
        
        # Also print to console for monitoring
        print(f"\n--- Batch {batch_id} Results ---")
        batch_df.show(truncate=False)

# Use foreachBatch to apply the function to each micro-batch
print("Starting streaming query...")
print("Results will be saved to outputs/task_2/\n")

query = driver_aggregations.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch_to_csv) \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()