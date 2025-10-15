# Real-Time Ride-Sharing Analytics with Apache Spark Streaming

## 📋 Project Overview

This project implements a **real-time analytics pipeline** for a ride-sharing platform using **Apache Spark Structured Streaming**. The system processes streaming ride data, performs real-time aggregations, and analyzes trends over time windows.

The pipeline simulates real-world scenarios where ride data continuously flows in, and analytics are computed instantly rather than in batch mode.

---

## 🎯 Objectives

1. **Task 1:** Ingest and parse real-time ride-sharing data from a socket stream
2. **Task 2:** Perform real-time aggregations on driver earnings and trip distances
3. **Task 3:** Analyze fare trends using sliding time windows

---

## 🛠️ Technologies Used

- **Apache Spark 3.x** - Distributed data processing engine
- **PySpark** - Python API for Spark
- **Spark Structured Streaming** - Stream processing framework
- **Python 3.12+** - Programming language
- **Faker** - Generate realistic test data
- **Socket Programming** - Data streaming mechanism

---

## 📁 Project Structure
```
ride-sharing-analytics/
├── outputs/
│   ├── task_1/          # Parsed streaming data (CSV)
│   ├── task_2/          # Driver aggregations (CSV)
│   └── task_3/          # Windowed analytics (CSV)
├── task1.py             # Stream ingestion and parsing
├── task2.py             # Real-time driver aggregations
├── task3.py             # Time-window based analytics
├── data_generator.py    # Simulates real-time ride data
└── README.md            # Project documentation
```

---

## ⚙️ Setup Instructions

### Prerequisites
- Python 3.12+
- pip package manager

### Installation

1. **Clone the repository** (or open in GitHub Codespaces)

2. **Install dependencies:**
```bash
pip install pyspark faker
```

3. **Create output directories:**
```bash
mkdir -p outputs/task_1 outputs/task_2 outputs/task_3
```

---

## 🚀 How to Run

You need **two terminal windows** to run this project:

### Terminal 1: Start Data Generator
```bash
python data_generator.py
```
This starts a socket server on `localhost:9999` that continuously generates ride data.

### Terminal 2: Run Tasks Sequentially

**Run Task 1:**
```bash
python task1.py
# Let it run for 30-60 seconds, then press Ctrl+C
```

**Run Task 2:**
```bash
python task2.py
# Let it run for 30-60 seconds, then press Ctrl+C
```

**Run Task 3:**
```bash
python task3.py
# Let it run for 1-2 minutes, then press Ctrl+C
```

> **Note:** Only one task can run at a time since they all connect to the same socket (localhost:9999).

---

## 📊 Task Details & Approach

### **Task 1: Streaming Ingestion and Parsing**

#### Objective
Ingest streaming data from a socket and parse JSON messages into a structured Spark DataFrame.

#### Approach
1. **Create Spark Session** - Initialize Spark with local mode
2. **Define Schema** - Specify data structure (trip_id, driver_id, distance_km, fare_amount, timestamp)
3. **Read Stream** - Connect to socket using `spark.readStream.format("socket")`
4. **Parse JSON** - Use `from_json()` to convert raw JSON strings to structured columns
5. **Output Results** - Write parsed data to console and CSV files using `foreachBatch`

#### Key Code Snippet
```python
# Read from socket
streaming_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON
parsed_df = streaming_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")
```

#### Sample Output
```
+------------------------------------+---------+-----------+-----------+-------------------+
|trip_id                             |driver_id|distance_km|fare_amount|timestamp          |
+------------------------------------+---------+-----------+-----------+-------------------+
|8f4e2c1a-5b6d-4e9f-a3c7-1d8e9f0a2b3c|45       |23.45      |67.80      |2025-10-14 17:30:15|
|3a7b9c2d-1e5f-4g8h-6i9j-0k1l2m3n4o5p|23       |15.67      |42.30      |2025-10-14 17:30:16|
|7c2d4e6f-8g9h-1i2j-3k4l-5m6n7o8p9q0r|78       |31.20      |89.50      |2025-10-14 17:30:17|
+------------------------------------+---------+-----------+-----------+-------------------+
```

**Output Location:** `outputs/task_1/batch_X.csv`

---

### **Task 2: Real-Time Driver Aggregations**

#### Objective
Aggregate streaming data to compute total earnings and average distance per driver in real-time.

#### Approach
1. **Reuse Parsed Data** - Start from Task 1's parsed DataFrame
2. **Convert Timestamps** - Transform string timestamps to `TimestampType`
3. **Add Watermark** - Handle late-arriving data with 10-minute watermark
4. **Group & Aggregate** - Group by `driver_id` and compute:
   - `SUM(fare_amount)` as total_fare
   - `AVG(distance_km)` as avg_distance
5. **Output Mode: Complete** - Show cumulative results (totals keep growing)
6. **Write to CSV** - Save aggregations every 10 seconds

#### Key Code Snippet
```python
# Timestamp conversion
df_with_timestamp = parsed_df.withColumn(
    "timestamp", 
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
).withWatermark("timestamp", "10 minutes")

# Aggregations
driver_aggregations = df_with_timestamp.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)
```

#### Sample Output
```
--- Batch 2 Results ---
+---------+------------------+------------------+
|driver_id|total_fare        |avg_distance      |
+---------+------------------+------------------+
|45       |1234.50           |19.23             |
|23       |987.60            |22.45             |
|78       |1567.80           |17.89             |
|12       |845.20            |21.34             |
|56       |1023.40           |18.67             |
+---------+------------------+------------------+
```

**Insights:**
- Driver 78 has highest earnings ($1,567.80)
- Driver 23 has longest average trips (22.45 km)
- Real-time updates show cumulative totals

**Output Location:** `outputs/task_2/batch_X.csv`

---

### **Task 3: Windowed Time-Based Analytics**

#### Objective
Analyze fare trends using sliding time windows to identify patterns and peak earning periods.

#### Approach
1. **Convert Timestamps** - Ensure proper `TimestampType` for event time
2. **Add Watermark** - 1-minute watermark for handling late data
3. **Create Windows** - Use `window()` function with:
   - **Window size:** 5 minutes
   - **Slide interval:** 1 minute
   - Creates overlapping windows for trend analysis
4. **Aggregate** - Calculate `SUM(fare_amount)` per window
5. **Extract Window Bounds** - Get window start and end times
6. **Output Mode: Complete** - Show all active windows

#### Key Code Snippet
```python
# Windowed aggregation
windowed_aggregation = df_with_timestamp \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute")
    ) \
    .agg(
        sum("fare_amount").alias("total_fare")
    )

# Extract window details
windowed_results = windowed_aggregation.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)
```

#### Sample Output
```
--- Batch 3 Windowed Results ---
+-------------------+-------------------+------------------+
|window_start       |window_end         |total_fare        |
+-------------------+-------------------+------------------+
|2025-10-14 17:30:00|2025-10-14 17:35:00|1234.50           |
|2025-10-14 17:31:00|2025-10-14 17:36:00|1456.80           |
|2025-10-14 17:32:00|2025-10-14 17:37:00|1589.30           |
|2025-10-14 17:33:00|2025-10-14 17:38:00|1678.90           |
|2025-10-14 17:34:00|2025-10-14 17:39:00|1823.40           |
+-------------------+-------------------+------------------+
```

**Insights:**
- Sliding windows show earnings trend **increasing** over time
- 5-minute windows overlap by 4 minutes, providing smooth trend analysis
- Window from 17:34-17:39 has highest earnings ($1,823.40)
- Useful for identifying peak hours and demand patterns

**Output Location:** `outputs/task_3/batch_X.csv`

---






**Happy Streaming! 🚀**
