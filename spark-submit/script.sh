spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 /home/iceberg/notebooks/notebooks/kafka_to_iceberg.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /home/iceberg/notebooks/notebooks/kafka_to_cassandra.py
