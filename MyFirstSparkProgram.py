import pyspark
from pyspark.sql import *
from lib.logger import Log4j
from utils import get_spark_config, load_df, count_by_country


if __name__ == '__main__':

    spark_conf = get_spark_config()
    spark = SparkSession \
        .builder \
        .config(conf=spark_conf)\
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

    logger.info('Application has finished.')

    spark.stop()
