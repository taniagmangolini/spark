from unittest import TestCase
from pyspark.sql import SparkSession
from utils import load_df, count_by_country
from lib.logger import Log4j


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
