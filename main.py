import pyspark
from pyspark.sql import *
from pyspark.sql.functions import substring_index, udf, expr
from lib.logger import Log4j
from utils import get_spark_config, load_df, count_by_country, repart_df,\
load_apache_logs, parse_gender
from data_reader import DataReader
from data_writer import DataWriter
from constants import *


if __name__ == '__main__':

    spark_conf = get_spark_config()
    spark = SparkSession \
        .builder \
        .config(conf=spark_conf)\
        .enableHiveSupport()\
        .getOrCreate()

    logger = Log4j(spark)
    logger.info(f'Application has started.')
    logger.info(f'Using the following params:')
    logger.info(spark.sparkContext.getConf().toDebugString())

    df = load_df(spark, logger, '/data/sample.csv')

    # to simulate partitions, let split the data into 2 partitions
    repartitioned_df = df.repartition(2)
    repartitioned_df.show()

    logger.info('People Age, Gender, Country and State filtered by Age > 40:')
    filtered_df = repartitioned_df.where('Age > 40')\
        .select('Age', 'Gender', 'Country', 'state')
    filtered_df.show()

    counts_by_country = count_by_country(repartitioned_df)
    logger.info(f'Total people grouped by Country: {counts_by_country.collect()}')

    logger.info('Loading different types of data files...')
    flight_csv_df = DataReader(logger=logger,
                               spark=spark,
                               file='data/flight*csv',
                               schema=FLIGHT_SCHEMA_STRUCT,
                               format=CSV,
                               mode=FAILFAST,
                               date_Format='M/d/y',
                               show_header= True).read()
    flight_csv_df.show(5)
    logger.info(f'CSV File schema: {flight_csv_df.schema.simpleString()}')

    flight_json_df = DataReader(logger=logger,
                                spark=spark,
                                file='data/flight*json',
                                format=JSON,
                                mode=PERMISSIVE,
                                date_Format='M/d/y').read()
    flight_json_df.show(5)
    logger.info(f'JSON File schema: {flight_json_df.schema.simpleString()}')

    logger.info(f'Saving to file...')
    DataWriter(logger=logger,
               spark=spark,
               df=flight_json_df,
               filepath='data/json/',
               format=JSON,
               mode=OVERWRITE,
               max_records_file=10000,
               repartition_cols=['OP_CARRIER', 'ORIGIN']).write_to_file()

    flight_parquet_df = DataReader(logger=logger,
                                   spark=spark,
                                   file='data/flight*parquet',
                                   format=PARQUET,
                                   mode=DROPMALFORMED,
                                   date_Format='M/d/y').read()
    flight_parquet_df.show(5)
    logger.info(f'PARQUET File schema: {flight_parquet_df.schema.simpleString()}')

    logger.info(f'Saving to file...')
    flight_parquet_repartitioned_df = repart_df(flight_parquet_df,2)
    DataWriter(logger=logger,
               spark=spark,
               df=flight_parquet_repartitioned_df,
               filepath='data/avro/',
               format=AVRO,
               mode=OVERWRITE).write_to_file()

    logger.info(f'Saving to table...')
    DataWriter(logger=logger,
               spark=spark,
               df=flight_parquet_df,
               db='AIRLINE_DB',
               mode=OVERWRITE,
               repartition_cols=['OP_CARRIER', 'ORIGIN'],
               sort_by=['OP_CARRIER', 'ORIGIN'],
               bucket_num=5).write_to_table()

    logger.info('Applying transformations to apache logs file...')

    apache_logs_df = load_apache_logs(logger, spark)
    (apache_logs_df.where("trim(referrer) != '-' ")\
                   .withColumn('referrer', substring_index('referrer', '/', 3))\
                   .groupBy('referrer')\
                   .count()\
                   .show(100, truncate=False))

    logger.info('Custom User Data Function (UDF)...')

    survey_df = DataReader(logger=logger,
                           spark=spark,
                           file='data/survey.csv',
                           format=CSV,
                           mode=FAILFAST,
                           show_header= True).read()

    # Registering the UDF in the Driver. It will not be created in the catalog.
    parse_gender_udf = udf(parse_gender, returnType=StringType())
    survey_df_transformed_no_catalog = survey_df.withColumn('Gender', parse_gender_udf('Gender'))
    survey_df_transformed_no_catalog.show(5)

    # If you want to register in the catalog as a sql function, you need to register it in a different way:
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    logger.info("Catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]
    survey_df_transformed_catalog = survey_df.withColumn('Gender', expr('parse_gender_udf(Gender)'))
    survey_df_transformed_catalog.show(5)

    logger.info('Application has finished.')

    spark.stop()
