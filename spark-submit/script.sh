spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 /home/iceberg/notebooks/notebooks/kafka_to_iceberg.py

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --conf spark.metrics.conf.path=/opt/spark/conf/metrics.properties \
  --conf spark.sql.streaming.metricsEnabled=true \
  /home/iceberg/notebooks/notebooks/kafka_to_iceberg.py > ~/spark.log 2>&1 &
