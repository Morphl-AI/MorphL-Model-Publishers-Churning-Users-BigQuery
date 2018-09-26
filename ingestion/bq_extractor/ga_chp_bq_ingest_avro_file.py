from os import getenv
from pyspark.sql import functions as f, SparkSession

MASTER_URL = 'local[*]'
APPLICATION_NAME = 'ingest_avro'

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

LOCAL_AVRO_FILE = getenv('LOCAL_AVRO_FILE')

def main():
    spark_session = (
        SparkSession.builder
                    .appName(APPLICATION_NAME)
                    .master(MASTER_URL)
                    .config('spark.cassandra.connection.host', MORPHL_SERVER_IP_ADDRESS)
                    .config('spark.cassandra.auth.username', MORPHL_CASSANDRA_USERNAME)
                    .config('spark.cassandra.auth.password', MORPHL_CASSANDRA_PASSWORD)
                    .config('spark.sql.shuffle.partitions', 16)
                    .getOrCreate())

    log4j = spark_session.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    avro_df = (
        spark_session
        .read
        .format('com.databricks.spark.avro')
        .load(LOCAL_AVRO_FILE))

