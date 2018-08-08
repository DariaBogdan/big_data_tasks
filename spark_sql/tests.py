import numpy as np
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
import pandas as pd
from pandas.testing import assert_frame_equal
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
        print('\nresult!!\n', result.show(), result.toPandas())
        expected = pd.DataFrame({'ValidFrom': ['11-06-05-2016', '11-05-08-2016', '10-06-11-2015'],
                                 'CurrencyName': ['Euro']*3,
                                 'CurrencyCode': ['EUR']*3,
                                 'ExchangeRate': ['0.803', '0.873', '0.987']})
        print('\nexp!!\n', expected)
        assert_frame_equal(expected, result.toPandas())

    def test_get_bids(self):
        rawBids = pd.DataFrame({"MotelID": ["0000002", "0000001"],
                                "BidDate": ["20-20-06-2016", "10-06-11-2015"],
                                "HU": ["0.89", "ERROR_NO_BIDS_FOR_HOTEL"],
                                "UK": ["0.92", ""],
                                "NL": ["1.32", ""],
                                "US": ["2.07", ""],
                                "MX": ["", ""],
                                "AU": ["1.35", ""],
                                "CA": ["0.89", ""],
                                "CN": ["0.87", ""],
                                "KR": ["1.22", ""],
                                "BE": ["1.06", ""],
                                "I": ["0.93", ""],
                                "JP": ["0.88", ""],
                                "IN": ["1.36", ""],
                                "HN": ["1.48", ""],
                                "GY": ["1.14", ""],
                                "DE": ["0.99", ""]})
        rawBids = self.spark.createDataFrame(rawBids)
        exchangedRates = pd.DataFrame({'ValidFrom': ['20-20-06-2016', '10-06-11-2015'],
                                 'CurrencyName': ['Euro']*2,
                                 'CurrencyCode': ['EUR']*2,
                                 'ExchangeRate': ['0.803', '0.987']})
        exchangedRates = self.spark.createDataFrame(exchangedRates)
        expected = pd.DataFrame({'MotelID': ['0000002', '0000002'],
                                 'loSa': ['US', 'CA'],
                                 'BidDate': ['2016-06-20 20:00', '2016-06-20 20:00'],
                                 'price': np.array([1.662, 0.715]).astype('float32')})
        result = self.obj.get_bids(rawBids, exchangedRates)
        assert_frame_equal(expected, result.toPandas())

    def test_get_motels(self):
        result = self.obj.get_motels(self.spark, self.INPUT_MOTELS_SAMPLE)
        expected = pd.DataFrame({'MotelID': ['0000001', '0000002', '0000003'],
                                 'MotelName': ['Grand Mengo Casino', 'Novelo Granja', 'Tiny City Motor Inn']})
        assert_frame_equal(expected, result.toPandas())

    def test_get_enriched(self):
        bids = pd.DataFrame({'MotelID': ['0000002', '0000002'],
                             'loSa': ['US', 'CA'],
                             'BidDate': ['2016-05-06 11:00', '2016-05-06 11:00'],
                             'price': np.array([1.662, 0.715]).astype('float32')})
        bids = self.spark.createDataFrame(bids)
        motels = pd.DataFrame({'MotelID': ['0000002'],
                               'MotelName': ['Novelo Granja']})
        motels=self.spark.createDataFrame(motels)

        expected = pd.DataFrame({'MotelID': ['0000002'],
                                 'MotelName': ['Novelo Granja'],
                                 'BidDate': ['2016-05-06 11:00'],
                                 'loSa': ['US'],
                                 'price': [1.662]})

        result = self.obj.get_enriched(bids, motels)
        assert_frame_equal(result.toPandas(), expected)

    def test_should_read_raw_bids(self):
        expected = pd.DataFrame({"MotelID": ["0000002", "0000001"],
                                "BidDate": ["15-04-08-2016", "06-05-02-2016"],
                                "HU": ["0.89", "ERROR_NO_BIDS_FOR_HOTEL"],
                                "UK": ["0.92", None],
                                "NL": ["1.32", None],
                                "US": ["2.07", None],
                                "MX": [None, None],
                                "AU": ["1.35", None],
                                "CA": ["0.89", None],
                                "CN": ["0.92", None],
                                "KR": ["1.32", None],
                                "BE": ["2.07", None],
                                "I": [None, None],
                                "JP": ["1.35", None],
                                "IN": ["2.07", None],
                                "HN": [None, None],
                                "GY": ["1.35", None],
                                "DE": [None, None]})

        rawBids = self.obj.get_raw_bids(self.spark, self.INPUT_BIDS_SAMPLE)

        assert_frame_equal(expected, rawBids.toPandas())

    def test_should_collect_errournes_records(self):
        rawBids = pd.DataFrame({'MotelID': ['1', '2', '3', '4', '5'],
                                'BidDate': ['06-05-02-2016', '15-04-08-2016', '07-05-02-2016', '06-05-02-2016', '06-05-02-2016'],
                                'HU': ['ERROR_1', '0.89', 'ERROR_2', 'ERROR_1', 'ERROR_2']})
        rawBids = self.spark.createDataFrame(rawBids)

        expected = pd.DataFrame(
            {'BidDate': ['06-05-02-2016', '07-05-02-2016', '06-05-02-2016'],
             'HU': ['ERROR_2', 'ERROR_2', 'ERROR_1'],
             'count': [1,1,2]
            }
        )

        erroneousRecords = self.obj.get_erroneous_records(rawBids)
        assert_frame_equal(expected, erroneousRecords.toPandas())
