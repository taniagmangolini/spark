from pyspark.sql.functions import spark_partition_id

class DataWriter(object):

    def __init__(self, logger, spark, df, filepath=None, format=None, mode=None, db=None,
                 max_records_file=None, repartition_cols=None, bucket_num=None, sort_by=None):
        self.logger = logger
        self.spark = spark
        self.df = df
        self.filepath = filepath
        self.format = format
        self.mode = mode
        self.db = db
        self.max_records_file = max_records_file
        self.repartition_cols = repartition_cols
        self.bucket_num = bucket_num
        self.sort_by = sort_by

    def write_to_file(self):
        '''
        Write df to file(s).
        The number of files depends on the number of partitions.
        '''
        self.logger.info(f'Writing to file {self.filepath}')
        self.logger.info(f'Number of partitions {self.df.rdd.getNumPartitions()}')
        self.logger.info(f'Number of rows per partition {self.df.groupBy(spark_partition_id()).count().show()}')
        output_file = self.df.write\
                          .format(self.format) \
                          .option('path', self.filepath) \
                          .mode(self.mode)\

        if self.max_records_file:
            output_file.option('maxRecordsPerFile', self.max_records_file)

        if self.repartition_cols:
            output_file.partitionBy(*self.repartition_cols)

        output_file.save()

    def write_to_table(self):
        '''
        Write df to table.
        '''
        if self.db:
            self.spark.sql(f'CREATE DATABASE IF NOT EXISTS {self.db}')
            self.spark.catalog.setCurrentDatabase(self.db)
        self.logger.info(f'Writing to table into db {self.spark.catalog.currentDatabase}...')

        table = self.df.write \
                    .mode(self.mode)

        if self.repartition_cols and not self.bucket_num:
            table.partitionBy(*self.repartition_cols)

        if self.repartition_cols and self.bucket_num:
            table.bucketBy(self.bucket_num, *self.repartition_cols)

        if self.sort_by :
            table.sortBy(*self.sort_by)

        table.saveAsTable('flight_data_tbl')
        self.logger.info(f'Available tables {self.spark.catalog.listTables(self.db)}')
