from unittest import TestCase
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import load_df, count_by_country
from lib.logger import Log4j
from datetime import date

def to_date_df(df, format, field):
    '''
    Convert string data to date type.
    '''
    return df.withColumn(field, to_date(col(field), format))

class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (SparkSession
                     .builder\
                     .appName('Test')\
                     .master('local[3]')\
                     .getOrCreate())
        cls.logger = Log4j(cls.spark)


    def test_datafile_loading(self):
        df = load_df(self.spark, self.logger, '/data/sample.csv')
        total_df = count_by_country(df).collect()
        total_dict = dict([(row['Country'], row['count']) for row in total_df])
        self.assertEquals(total_dict['United States'], 6)
        self.assertEquals(total_dict['Canada'], 2)
        self.assertEquals(total_dict['United Kingdom'], 1)


class RowTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (SparkSession
                     .builder\
                     .appName('Test')\
                     .master('local[3]')\
                     .getOrCreate())
        cls.logger = Log4j(cls.spark)

        my_schema = StructType([
            StructField('ID', StringType()),
            StructField('EventDate', StringType())
        ])
        my_rows = [
            Row('1', '4/5/2020'),
            Row('2', '08/15/2022'),
            Row('3', '01/01/2023'),
            Row('4', '12/31/2022')
        ]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    def test_date_field_type(self):
        formatted_rows = to_date_df( self.my_df, 'M/d/y', 'EventDate').collect()
        for row in formatted_rows:
            self.assertIsInstance(row['EventDate'], date)
