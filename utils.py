import configparser
from pyspark import SparkConf
import os

def get_spark_config():
    '''
    Load from hte spark.conf file the Spark Session params.
    '''
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read('spark.conf')
    for (key, value) in config.items('SPARK_APP_CONFIGS'):
        spark_conf.set(key, value)
    return spark_conf


def load_df(spark, logger, path):
    '''
    Load the path as a dataframe.
    '''
    logger.info(f'Reading {os.getcwd()}{path}...')
    df = spark.read\
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .csv(f"file:{os.getcwd()}{path}")
    return df

def count_by_country(df):
    '''
    Group by and count by Country.
    '''
    df_grouped_by_country = df.groupBy('Country')
    return df_grouped_by_country.count()


def repart_df(df, num):
    '''
    Repart the data into the speicified number (num) of parts.
    '''
    return df.repartition(num)
