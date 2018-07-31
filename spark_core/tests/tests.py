from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import unittest

from hw import MotelsHomeRecommendation, expand, transform_date, to_euro
from classes import BidItem

class TestMR(unittest.TestCase):
    INPUT_BIDS_SAMPLE = "bids_sample.txt"
    INPUT_EXCHANGED_RATES_SAMPLE = "ex_sample.txt"
    INPUT_MOTELS_SAMPLE = "motels_sample.txt"

    def setUp(self):
        self.sc = SparkContext.getOrCreate(SparkConf())
        self.spark = SparkSession(self.sc)

    def test_get_exchanged_rates(self):
        result = MotelsHomeRecommendation.get_exchange_rates(self, self.sc, self.INPUT_EXCHANGED_RATES_SAMPLE)
        expected = self.sc.parallelize(
            [
                ['11-06-05-2016', '0.803'],
                ['11-05-08-2016', '0.873'],
                ['10-06-11-2015', '0.987']
            ]
        )
        self.assertCountEqual(expected.collect(), result.collect())

    def test_get_bids(self):
        rawBids = self.sc.parallelize(
            [
                ["0000002", "11-06-05-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35", "0.89", "0.92", "1.32", "2.07", "", "1.35", "0.89", "0.92", "1.32", "2.07"],
                ["0000001", "10-06-11-2015", "ERROR_NO_BIDS_FOR_HOTEL"]
            ]
        )
        exchangedRates = self.sc.parallelize(
            [
                ['11-06-05-2016', '0.803'],
                ['10-06-11-2015', '0.987']
            ]
        )
        expected = self.sc.parallelize(
            [
                '0000002,2016-05-06 11:00,US,1.662',
                '0000002,2016-05-06 11:00,CA,0.715'
            ]
        )
        result = MotelsHomeRecommendation.get_bids(self, rawBids, exchangedRates)
        self.assertEqual([str(x) for x in result.collect()], expected.collect())

    def test_get_motels(self):
        result = MotelsHomeRecommendation.get_motels(self, self.sc, self.INPUT_MOTELS_SAMPLE)
        expected = self.sc.parallelize(
            [
                ['0000001', 'Grand Mengo Casino'],
                ['0000002', 'Novelo Granja'],
                ['0000003', 'Tiny City Motor Inn']
            ]
        )
        self.assertCountEqual(expected.collect(), result.collect())

    def test_get_enriched(self):
        bids = self.sc.parallelize(
            [
                BidItem('0000002','2016-05-06 11:00','US','1.662'),
                BidItem('0000002','2016-05-06 11:00','CA','0.715')
            ]
        )
        motels = self.sc.parallelize(
            [
                ['0000002', 'Novelo Granja'],
            ]
        )
        expected = self.sc.parallelize(
            [
                '0000002,Novelo Granja,2016-05-06 11:00,US,1.662'
            ]
        )
        result = MotelsHomeRecommendation.get_enriched(self, bids, motels)
        self.assertEqual(result.collect(), expected.collect())

    def test_transform_date(self):
        input = '11-05-06-2011'
        expected = '2011-06-05 11:00'
        result = transform_date(input)
        self.assertEqual(expected, result)

    def test_to_euro(self):
        price_usd = '100'
        exchange_rate = '0.3333333333'
        expected = 33.333
        result = to_euro(price_usd, exchange_rate)
        self.assertEqual(expected, result)

    def test_to_euro_empty(self):
        price_usd = ''
        exchange_rate = '0.8'
        expected = ''
        result = to_euro(price_usd, exchange_rate)
        self.assertEqual(expected, result)

    def test_expand(self):
        rawBid = '0000002,11-05-08-2016,0.92,1.68,0.81,0.68,1.59,,1.63,1.77,2.06,0.66,1.53,,0.32,0.88,0.83,1.01'.split(',')
        exchange_rate = '0.8'
        result = expand((rawBid, exchange_rate))
        expected = [
            '0000002,2016-08-05 11:00,US,0.544',
            '0000002,2016-08-05 11:00,MX,1.272',
            '0000002,2016-08-05 11:00,CA,1.304'
        ]
        self.assertEqual([str(x) for x in result], expected)

    def test_should_read_raw_bids(self):
        expected = self.sc.parallelize(
            [
                ["0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"],
                ["0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL"]
            ]
        )

        rawBids = MotelsHomeRecommendation.get_raw_bids(self, self.sc, self.INPUT_BIDS_SAMPLE)

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

        erroneousRecords = MotelsHomeRecommendation.get_erroneous_records(self, rawBids)
        self.assertCountEqual(expected.collect(), erroneousRecords.collect())
