from pyspark.sql.functions import spark_partition_id

class DataWriter(object):

    def __init__(self, logger, df, filepath, format, mode, max_records_file=None, repartition_cols=None):
        self.logger = logger
        self.df = df
        self.filepath = filepath
        self.format = format
        self.mode = mode
        self.max_records_file = max_records_file
        self.repartition_cols = repartition_cols

    def write(self):
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
