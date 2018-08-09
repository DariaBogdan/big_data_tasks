from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import unittest

from hw import MotelsHomeRecommendation


class TestMR(unittest.TestCase):
    INPUT_BIDS_SAMPLE = "bids_sample.txt"
    INPUT_EXCHANGED_RATES_SAMPLE = "ex_sample.txt"
    INPUT_MOTELS_SAMPLE = "motels_sample.txt"

    def setUp(self):
        self.sc = SparkContext.getOrCreate(SparkConf())
        self.spark = SparkSession.builder.getOrCreate()
        self.obj = MotelsHomeRecommendation('', '', '', '')
        self.maxDiff = None

    def test_get_exchanged_rates(self):
        result = self.obj.get_exchange_rates(self.spark, self.INPUT_EXCHANGED_RATES_SAMPLE)
        expected = self.spark.createDataFrame([('11-06-05-2016', 'Euro', 'EUR', '0.803'),
                                               ('11-05-08-2016', 'Euro', 'EUR', '0.873'),
                                               ('10-06-11-2015', 'Euro', 'EUR', '0.987')],
                                              self.obj.EXCHANGE_RATES_HEADER)
        self.assertEqual(expected.collect(), result.collect())

    def test_get_bids(self):
        rawBids = self.spark.createDataFrame([("0000002", "20-20-06-2016", "0.89", "0.92", "1.32", "2.07",
                                               "", "1.35", "0.89", "0.87", "1.22", "1.06", "0.93", "0.88",
                                               "1.36", "1.48", "1.14", "0.99"),
                                              ("0000001", "10-06-11-2015", "ERROR_NO_BIDS_FOR_HOTEL",
                                               None,None,None,None,None,None,None,None,
                                               None,None,None,None,None,None,None)],
                                             self.obj.BIDS_HEADER)
        exchangedRates = self.spark.createDataFrame([('20-20-06-2016', 'Euro', 'EUR', '0.803'),
                                                     ('10-06-11-2015', 'Euro', 'EUR', '0.987')],
                                                    self.obj.EXCHANGE_RATES_HEADER)
        expected = self.spark.createDataFrame([('0000002', 'US', '2016-06-20 20:00', '1.662'),
                                               ('0000002', 'CA', '2016-06-20 20:00', '0.715')],
                                              ['MotelID', 'loSa', 'BidDate', 'price'])
        result = self.obj.get_bids(rawBids, exchangedRates)
        self.assertEqual(expected.collect(), result.collect())

    def test_get_bids_same(self):
        rawBids = self.spark.createDataFrame([("0000002", "20-20-06-2016", "0.89", "0.92", "1.32", "0.89",
                                               "0.89", "1.35", "0.89", "0.87", "1.22", "1.06", "0.93", "0.88",
                                               "1.36", "1.48", "1.14", "0.99")],
                                             self.obj.BIDS_HEADER)
        exchangedRates = self.spark.createDataFrame([('20-20-06-2016', 'Euro', 'EUR', '0.803')],
                                                    self.obj.EXCHANGE_RATES_HEADER)
        expected = self.spark.createDataFrame([('0000002', 'US', '2016-06-20 20:00', '0.715'),
                                               ('0000002', 'MX', '2016-06-20 20:00', '0.715'),
                                               ('0000002', 'CA', '2016-06-20 20:00', '0.715')],
                                              ['MotelID', 'loSa', 'BidDate', 'price'])
        result = self.obj.get_bids(rawBids, exchangedRates)
        self.assertEqual(expected.collect(), result.collect())

    def test_get_motels(self):
        result = self.obj.get_motels(self.spark, self.INPUT_MOTELS_SAMPLE)
        expected = self.spark.createDataFrame([('0000001', 'Grand Mengo Casino'),
                                               ('0000002', 'Novelo Granja'),
                                               ('0000003', 'Tiny City Motor Inn')],
                                              ['MotelID', 'MotelName'])
        self.assertEqual(expected.collect(), result.collect())

    def test_get_enriched(self):
        bids =self.spark.createDataFrame([('0000002', 'US', '2016-06-20 20:00', '1.662'),
                                          ('0000002', 'CA', '2016-06-20 20:00', '0.715')],
                                         ['MotelID', 'loSa', 'BidDate', 'price'])
        motels = self.spark.createDataFrame([('0000002', 'Novelo Granja')],
                                              ['MotelID', 'MotelName'])
        expected = self.spark.createDataFrame([('0000002', 'Novelo Granja', '2016-06-20 20:00', 'US', '1.662')],
                                              ['MotelID', 'MotelName', 'BidDate', 'loSa', 'price'])
        result = self.obj.get_enriched(bids, motels)
        self.assertEqual(result.collect(), expected.collect())

    def test_should_read_raw_bids(self):
        expected = self.spark.createDataFrame([("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07",
                                               "0.99", "1.35", "0.89", "0.92", "1.32", "2.07", "0.99",
                                                "1.35", "2.07", "0.99", "1.35", "0.99"),
                                              ("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL",
                                               None,None,None,None,None,None,None,None,
                                               None,None,None,None,None,None,None)],
                                             self.obj.BIDS_HEADER)
        rawBids = self.obj.get_raw_bids(self.spark, self.INPUT_BIDS_SAMPLE)
        self.assertEqual(expected.collect(), rawBids.collect())

    def test_should_collect_errournes_records(self):
        rawBids = self.spark.createDataFrame([("0000001", "06-05-02-2016", "ERROR_1",
                                               None,None,None,None,None,None,None,None,
                                               None,None,None,None,None,None,None),
                                              ("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07",
                                               "", "1.35", "0.89", "0.87", "1.22", "1.06", "0.93", "0.88",
                                               "1.36", "1.48", "1.14", "0.99"),
                                              ("0000001", "07-05-02-2016", "ERROR_2",
                                               None,None,None,None,None,None,None,None,
                                               None,None,None,None,None,None,None),
                                              ("0000001", "06-05-02-2016", "ERROR_1",
                                               None, None, None, None, None, None, None, None,
                                               None, None, None, None, None, None, None),
                                              ("0000001", "06-05-02-2016", "ERROR_2",
                                               None, None, None, None, None, None, None, None,
                                               None, None, None, None, None, None, None)
                                              ],
                                             self.obj.BIDS_HEADER)
        expected = self.spark.createDataFrame([('06-05-02-2016', 'ERROR_2', 1),
                                               ('07-05-02-2016', 'ERROR_2', 1),
                                               ('06-05-02-2016', 'ERROR_1', 2)],
                                              ['BidDate', 'HU', 'count'])

        erroneousRecords = self.obj.get_erroneous_records(rawBids)
        self.assertEqual(expected.collect(), erroneousRecords.collect())
