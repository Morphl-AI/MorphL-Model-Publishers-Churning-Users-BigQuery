from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import FloatType
from pyspark.ml.feature import Imputer, VectorAssembler, Normalizer, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors


from scipy.stats import boxcox
from pandas import Series
from numpy import log1p

MODEL_DAY_AS_STR = getenv('MODEL_DAY_AS_STR')
UNIQUE_HASH = getenv('UNIQUE_HASH')

MASTER_URL = 'local[*]'
APPLICATION_NAME = 'preprocessor'

TRAINING_OR_PREDICTION = getenv('TRAINING_OR_PREDICTION')

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')

HDFS_PORT = 9000
HDFS_DIR_INPUT_TRAINING = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_preproc_training'
HDFS_DIR_OUTPUT_TRAINING = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_scaled_features_training'
HDFS_DIR_INPUT_PREDICTION = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_preproc_prediction'
HDFS_DIR_OUTPUT_PREDICTION = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_scaled_features_prediction'


def add_one_func(a):
    return a + 1


def box_cox_func(a):
    box_cox_data, _ = boxcox(a)

    return Series(box_cox_data)


def log1p_func(a):
    return Series(log1p(a))


def extract(row):
    return (row.client_id, ) + tuple(row.scaledFeatures.toArray().tolist())


def process_dataframe(spark_session, hdfs_dir_input, hdfs_dir_output):

    numeric_features = ['bounces', 'events', 'page_views', 'sessions']

    add_one = pandas_udf(add_one_func, returnType=FloatType())
    box_cox = pandas_udf(box_cox_func, returnType=FloatType())
    log = pandas_udf(log1p_func, returnType=FloatType())

    df = spark_session.read.parquet(hdfs_dir_input)

    df_first_part_plus_one = (df.select('client_id',
                                        add_one(col('bounces')).alias(
                                            'bounces'),
                                        add_one(col('events')).alias(
                                            'events'),
                                        add_one(col('page_views')).alias(
                                            'page_views'),
                                        add_one(col('sessions')).alias(
                                            'sessions'),
                                        )
                              )

    df_first_part_box_cox = (df_first_part_plus_one.select('client_id',
                                                           box_cox(col('bounces')).alias(
                                                               'bounces'),
                                                           box_cox(col('events')).alias(
                                                               'events'),
                                                           box_cox(col('page_views')).alias(
                                                               'page_views'),
                                                           box_cox(col('sessions')).alias(
                                                               'sessions'),
                                                           )
                             )

    second_part_columns = ['client_id', 'churned', 'is_desktop', 'is_mobile',
                           'is_tablet'] if TRAINING_OR_PREDICTION == 'training' else ['client_id', 'is_desktop', 'is_mobile', 'is_tablet']

    df_second_part = (df.select(*second_part_columns,
                                log(col('session_duration')).alias(
                                    'session_duration')
                                )
                      )

    imputer = Imputer(inputCols=numeric_features, outputCols=numeric_features).setStrategy(
        "mean").setMissingValue(0.0)

    assembler = VectorAssembler(
        inputCols=numeric_features, outputCol="features")

    normalizer = Normalizer(inputCol="features",
                            outputCol="normFeatures", p=2.0)

    scaler = StandardScaler(inputCol="normFeatures", outputCol="scaledFeatures",
                            withStd=True, withMean=True)

    pipeline = Pipeline(stages=[imputer, assembler, normalizer, scaler])

    model = pipeline.fit(df_first_part_box_cox)

    result = model.transform(df_first_part_box_cox).drop(
        'bounces', 'events', 'page_views', 'sessions', 'features', 'normFeatures', 'scaled')

    final_df_first_part = (result.rdd.map(extract).repartition(32).toDF(['client_id'])
                           .withColumnRenamed('_2', 'bounces')
                           .withColumnRenamed('_3', 'events')
                           .withColumnRenamed('_4', 'page_views')
                           .withColumnRenamed('_5', 'sessions')
                           .drop('scaledFeatures'))

    final_df = final_df_first_part.join(df_second_part, 'client_id')

    if TRAINING_OR_PREDICTION == 'training':
        final_df.drop('client_id').repartition(
            numPartitions=32).write.parquet(hdfs_dir_output)
    else:
        final_df.repartition(numPartitions=32).write.parquet(hdfs_dir_output)


def main():
    spark_session = (
        SparkSession.builder
        .appName('preprocessor')
        .master('local[*]')
        .config('spark.sql.shuffle.partitions', 16)
        .config('parquet.enable.summary-metadata', 'true')
        .getOrCreate())

    log4j = spark_session.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    if TRAINING_OR_PREDICTION == 'training':
        process_dataframe(spark_session, HDFS_DIR_INPUT_TRAINING,
                          HDFS_DIR_OUTPUT_TRAINING)
    else:
        process_dataframe(spark_session, HDFS_DIR_INPUT_PREDICTION,
                          HDFS_DIR_OUTPUT_PREDICTION)


if __name__ == '__main__':
    main()
