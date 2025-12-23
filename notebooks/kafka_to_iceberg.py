from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, decode, expr, when
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DecimalType
)
from pyspark.sql.functions import from_json, col, udf, unbase64
import decimal
import struct
import base64
# ─────────────────────────────────────────────────────────────
# 1. Create Spark session with Iceberg + Streaming support
# ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("KafkaToIcebergOrders")
    .config("spark.metrics.conf.*.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
    .config("spark.metrics.conf.*.sink.prometheusServlet.class", "org.apache.spark.metrics.sink.PrometheusServlet")
    .config("spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics/prometheus")
    .config("spark.sql.streaming.metricsEnabled", "true")
    .config("spark.sql.catalog.rest.uri", "http://rest:8181")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────
# 2. Define Debezium payload schema for topic ice.public.orders
# ─────────────────────────────────────────────────────────────
orders_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", LongType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__table", StringType(), True),
    StructField("__lsn", LongType(), True),
    StructField("__source_ts_ms", LongType(), True)
])

# ─────────────────────────────────────────────────────────────
# 3. Read from Kafka
# ─────────────────────────────────────────────────────────────
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "ice.public.orders")
    .option("startingOffsets", "earliest")
    .load()
)

# ─────────────────────────────────────────────────────────────
# 4. Parse Kafka value as JSON (Debezium payload)
# ─────────────────────────────────────────────────────────────
from pyspark.sql.functions import get_json_object

parsed_df = kafka_df.select(
    from_json(get_json_object(col("value").cast("string"), "$.payload"), orders_schema).alias("data")
).select("data.*")

parsed_df.writeStream.format("console").option("truncate", False).start()

# ─────────────────────────────────────────────────────────────
# 5. Decode and clean columns
# ─────────────────────────────────────────────────────────────
def decode_debezium_decimal(b64_str):
    if b64_str is None:
        return None
    try:
        raw = base64.b64decode(b64_str)
        # Debezium uses big-endian signed integer representation
        unscaled = int.from_bytes(raw, byteorder="big", signed=True)
        return decimal.Decimal(unscaled) / decimal.Decimal(100)  # scale=2
    except Exception:
        return None

decode_decimal_udf = udf(decode_debezium_decimal, DecimalType(10, 2))
clean_df = (
    parsed_df.withColumn("total_amount", decode_decimal_udf(col("total_amount")))
    .withColumn(
        "order_timestamp",
        (col("order_date") / 1_000_000).cast("timestamp")
    )
    .withColumn(
        "is_deleted",
        when(col("__deleted") == "true", True).otherwise(False)
    )
    .drop("__deleted", "__table", "__op", "__lsn", "__source_ts_ms")
)

# ─────────────────────────────────────────────────────────────
# 6. Write to Iceberg table
# ─────────────────────────────────────────────────────────────
(
    clean_df.writeStream
    .format("iceberg")
    .option("checkpointLocation", "s3a://warehouse/checkpoints/orders")
    .outputMode("append")
    .toTable("rest.db.orders")
)

# Keep the query running
spark.streams.awaitAnyTermination()
