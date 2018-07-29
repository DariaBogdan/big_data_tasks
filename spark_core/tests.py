from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import unittest

from hw import MotelsHomeRecommendation

class TestMR(unittest.TestCase):
    INPUT_BIDS_SAMPLE = "bids_sample.txt"

    def setUp(self):
        self.sc = SparkContext.getOrCreate(SparkConf())
        self.spark = SparkSession(self.sc)

    def test_should_read_raw_bids(self):
        expected = self.sc.parallelize(
            [
                ["0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"],
                ["0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL"]
            ]
        )

        rawBids = MotelsHomeRecommendation.getRawBids(self, self.sc, self.INPUT_BIDS_SAMPLE)

        self.assertEqual(expected.collect(), rawBids.collect())

    def test_should_collect_errournes_records(self):
        rawBids = self.sc.parallelize(
            [
                ["1", "06-05-02-2016", "ERROR_1"],
                ["2", "15-04-08-2016", "0.89"],
                ["3", "07-05-02-2016", "ERROR_2"],
                ["4", "06-05-02-2016", "ERROR_1"],
                ["5", "06-05-02-2016", "ERROR_2"]

            ]
        )
        expected = self.sc.parallelize(
            [
                "06-05-02-2016,ERROR_1,2",
                "06-05-02-2016,ERROR_2,1",
                "07-05-02-2016,ERROR_2,1"
            ]
        )

        erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(self, rawBids)
        self.assertCountEqual(expected.collect(), erroneousRecords.collect())
