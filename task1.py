from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("Task1_Ingestion").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("TASK 1: Streaming Ingestion and Parsing")
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
print("✓ Parsing incoming JSON data...\n")
parsed_df = streaming_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Define function to write each batch to CSV
def write_batch_to_csv(batch_df, batch_id):
    if batch_df.count() > 0:
        output_path = f"outputs/task_1/batch_{batch_id}.csv"
        batch_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(output_path)
        print(f"✓ Batch {batch_id} written to {output_path}")
        batch_df.show(truncate=False)

# Write parsed data to console AND CSV files
print("Starting streaming query...")
print("Data will be saved to outputs/task_1/\n")

query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch_to_csv) \
    .trigger(processingTime='10 seconds') \
    .start()

print("✓ Streaming started! Press Ctrl+C to stop.\n")

query.awaitTermination()