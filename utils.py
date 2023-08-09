import configparser
from pyspark import SparkConf
from pyspark.sql.functions import regexp_extract
import os
import re
from constants import APACHE_LOG_REGEX, MALE, FEMALE

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

def load_apache_logs(logger, spark):
    '''
    Load the Apache logs file.
    '''
    apache_logs = spark.read.text('data/apache_logs.txt')

    apache_logs_df = apache_logs.select(regexp_extract('value', APACHE_LOG_REGEX, 1).alias('ip'),
                                        regexp_extract('value', APACHE_LOG_REGEX, 4).alias('date'),
                                        regexp_extract('value', APACHE_LOG_REGEX, 6).alias('request'),
                                        regexp_extract('value', APACHE_LOG_REGEX, 10).alias('referrer'))
    logger.info(f'Apache logs schema: {apache_logs_df.printSchema()}')
    return apache_logs_df

def parse_gender(gender):
    '''
    Parse gender column to the following types:
    - MALE
    - FEMALE
    - UNKNOWN
    '''
    if re.search(FEMALE, gender.lower()):
        return "Female"
    elif re.search(MALE, gender.lower()):
        return "Male"
    else:
        return "Unknown"
