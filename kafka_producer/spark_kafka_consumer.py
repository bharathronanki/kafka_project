from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, StringType

# Define schema for JSON messages
schema = StructType() \
    .add("id", IntegerType()) \
    .add("name", StringType()) \
    .add("text_message", StringType())

# Create Spark session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaToCSV") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka topic
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "messages_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert binary "value" column to string and parse JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Write to CSV (append mode, batch every 10 seconds)
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/messages") \
    .option("checkpointLocation", "output/checkpoints") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
