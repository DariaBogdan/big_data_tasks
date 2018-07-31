import os
from pyspark import SparkConf
from pyspark.context import SparkContext
import shutil
import tempfile
import unittest

from hw import MotelsHomeRecommendation

class TestMR(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.sc = SparkContext.getOrCreate(SparkConf())

    def test_integration(self):
        MotelsHomeRecommendation(
            'input/bids.txt',
            'input/exchange_rate.txt',
            'input/motels.txt',
            self.tmpdir
        ).process_data()

        err_result = self.sc.textFile(os.path.join(self.tmpdir, 'erroneous'))
        err_expected = self.sc.textFile(os.path.join('expected_output', 'error_records'))
        self.assertCountEqual(err_result.collect(), err_expected.collect())

        aggregated_result = self.sc.textFile(os.path.join(self.tmpdir, 'aggregated'))
        aggregated_expected = self.sc.textFile(os.path.join('expected_output', 'aggregated'))
        self.assertCountEqual(aggregated_result.collect(), aggregated_expected.collect())

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
