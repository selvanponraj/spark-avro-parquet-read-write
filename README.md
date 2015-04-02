# Spark: how to read and write from/to avro/parquet files
Sometimes can be tricky to read and write into HDFS using those very popular formats.

I put together a couple of sbt based projects meant to be a quick starting point/guideline for managing avro/parquet fro Spark.

* spark-avro-parquet-read-write-native shows how to read and write avro/parquet files using the input/output formats pairs from Spark
* spark-avro-parquet-read-write-sql shows how the spark sql apis provides a handier way for loading and writing avro/parquet files.
