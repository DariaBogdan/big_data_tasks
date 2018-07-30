from pyspark import SparkConf
from pyspark.context import SparkContext
import unittest

from hw import MotelsHomeRecommendation

class TestMR(unittest.TestCase):
    def setUp(self):
        MotelsHomeRecommendation(
            'input/bids.txt',
            'input/exchange_rate.txt',
            'input/motels.txt',
            'result'
        ).processData()

        self.sc = SparkContext.getOrCreate(SparkConf())

    def test_integration(self):
        err_result = self.sc.textFile('result/erroneous')
        err_expected = self.sc.textFile('expected_output/error_records')
        self.assertCountEqual(err_result.collect(), err_expected.collect())

        aggregated_result = self.sc.textFile('result/aggregated')
        aggregated_expected = self.sc.textFile('expected_output/aggregated')
        self.assertCountEqual(aggregated_result.collect(), aggregated_expected.collect())
