from pyspark.conf import SparkConf

spark_config = (
    SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.hive.convertMetastoreParquet", "false")
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
)
