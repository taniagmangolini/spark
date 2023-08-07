class DataReader(object):

    def __init__(self, logger, spark, file, format, mode, schema= None, date_Format=None, show_header=False):
        self.logger = logger
        self.spark = spark
        self.file = file
        self.schema = schema
        self.format = format
        self.mode = mode
        self.show_header = show_header
        self.date_Format = date_Format

    def read(self):
        self.logger.info(f'Reading file {self.file}')

        reader = self.spark.read\
            .format(self.format) \
            .option('mode', self.mode)

        if self.schema:
            reader.schema(self.schema)
        else:
            reader.option('inferSchema', 'true')

        if self.show_header:
            reader.option('header', 'true')

        if self.date_Format:
            reader.option('dateFormat', 'M/d/y') \

        return reader.load(self.file)

