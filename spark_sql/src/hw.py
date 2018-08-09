import argparse
import os
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, unix_timestamp, from_unixtime, round, max, col
from pyspark.sql.types import StructField, StringType, StructType


class MotelsHomeRecommendation:
    DELIMITER = ","
    ERRONEOUS_DIR = "erroneous"
    AGGREGATED_DIR = "aggregated"
    BIDS_HEADER = ["MotelID", "BidDate", "HU", "UK", "NL", "US", "MX", "AU", "CA", "CN", "KR", "BE", "I", "JP", "IN",
                   "HN", "GY", "DE"]
    EXCHANGE_RATES_HEADER = ["ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate"]
    MOTELS_HEADER = ['MotelID', 'MotelName']
    INPUT_DATE_FORMAT = 'HH-dd-MM-yyyy'
    COUNTRIES = ['US', 'MX', 'CA']

    def __init__(self, bidsPath, exchangeRatesPath, motelsPath, outputBasePath):
        self.bidsPath = bidsPath
        self.exchangeRatesPath = exchangeRatesPath
        self.motelsPath = motelsPath
        self.outputBasePath = outputBasePath
        self.sc = SparkContext.getOrCreate(SparkConf())
        self.spark = SparkSession.builder.getOrCreate()
        self.sc.setLogLevel('INFO')

    def __del__(self):
        self.sc.stop()

    @classmethod
    def get_raw_bids(cls, spark, bidsPath):
        """ Read file to DataFrame

        :param sc: spark context
        :param bidsPath: path to file
        :return: DataFrame
        """
        schema = StructType(
            [
                StructField(
                    name=field_name,
                    dataType=StringType()
                )
                for field_name in cls.BIDS_HEADER
            ]
        )
        if bidsPath.endswith('.parquet'):
            df = spark.read.parquet(bidsPath)
        else:
            df = spark.read.csv(path=bidsPath,
                                schema=schema,
                                sep=cls.DELIMITER)
        return df

    @staticmethod
    def get_erroneous_records(rawBids):
        """ Calculates amount of specific errors for hour

        :param rawBids: raw from file 'bids.txt'
        :return: string with "{date},{error message},{amount}"
        """
        df = rawBids\
            .where(rawBids.HU.contains('ERROR'))\
            .groupBy(rawBids.BidDate, rawBids.HU)\
            .count()
        return df

    @classmethod
    def get_exchange_rates(cls, spark, exchangeRatesPath):
        """ Read file to DataFrame

        :param spark: spark
        :param exchangeRatesPath: path to file
        :return: DataFrame
        """
        schema = StructType(
            [
                StructField(name=field_name,
                            dataType=StringType()
                            )
                for field_name in cls.EXCHANGE_RATES_HEADER
            ]
        )
        if exchangeRatesPath.endswith('.parquet'):
            df = spark.read.parquet(exchangeRatesPath)
        else:
            df = spark.read.csv(path=exchangeRatesPath,
                                schema=schema,
                                sep=cls.DELIMITER)
        return df

    def get_bids(self, rawBids, exchangeRates):
        """

        :param rawBids: DataFrame with raw bids
        :param exchangeRates: DataFrame with exchanged rates
        :return: DataFrame with price in needed currency, date in needed format, each row contains only one country
        """
        # exclude errors
        df = rawBids.\
            where(~rawBids.HU.contains('ERROR'))

        schema = StructType(
            [
                StructField(
                    name='MotelID',
                    dataType=StringType()
                ),
                StructField(
                    name='BidDate',
                    dataType=StringType()
                ),
                StructField(
                    name='price',
                    dataType=StringType()
                ),
                StructField(
                    name='loSa',
                    dataType=StringType()
                )
            ]
        )
        df_res = self.spark.createDataFrame([], schema)
        for country in self.COUNTRIES:
            # select price for current country
            df_current = df.select(rawBids.MotelID,
                                   rawBids.BidDate,
                                   col(country).alias('price'),
                                   lit(country).alias('loSa'))
            df_res = df_res.union(df_current)

        df_res = df_res.join(exchangeRates, df_res.BidDate == exchangeRates.ValidFrom, 'left')

        # format date
        df_res = df_res.select('MotelID',
                               'BidDate',
                               from_unixtime(unix_timestamp('BidDate', self.INPUT_DATE_FORMAT),
                                             format='yyyy-MM-dd HH:mm').alias('newdate'),
                               'loSa',
                               'price',
                               'ExchangeRate')

        # exclude empty prices and show price in needed currency
        df_res = df_res.where(df_res.price!='').\
            select('MotelID',
                   'loSa',
                   df_res.newdate.alias('BidDate'),
                   round(df_res.price.cast('float') * df_res.ExchangeRate.cast('float'), 3).alias('price'))
        return df_res

    @classmethod
    def get_motels(cls, spark, motelsPath):
        """ Read file to DataFrame

        :param spark: spark
        :param motelsPath: path to file
        :return: DataFrame
        """
        schema = StructType(
            [
                StructField(
                    name=field_name,
                    dataType=StringType())
                for field_name in cls.MOTELS_HEADER
            ]
        )
        if motelsPath.endswith('.parquet'):
            df = spark.read.parquet(motelsPath)
        else:
            df = spark.read.csv(path=motelsPath,
                                schema=schema,
                                sep=cls.DELIMITER)

        return df

    @staticmethod
    def get_enriched(bids, motels):
        """  Enrich the data and find the maximum

        :param bids: DataFrame with bids
        :param motels: DataFrame with motels
        :return: string containing '{motelId},{motelName},{formatted date},{loSa},{max price}'
        """

        # Defines partitioning specification and ordering specification.
        windowSpec = \
            Window \
                .partitionBy('MotelID', 'BidDate')
        df = bids.\
            join(other=motels,
                 on='MotelID',
                 how='left')
        df = df.select('*', max(df.price.cast('float')).
                       over(windowSpec).alias('max_price'))
        df = df.where(df.price == df.max_price).\
            select('MotelID', 'MotelName', 'BidDate', 'loSa', 'price')
        return df

    def process_data(self):
        # Read the bid data from the provided file.
        rawBids = self.get_raw_bids(self.spark, self.bidsPath)

        # Collect the errors and save the result
        erroneousRecords = self.get_erroneous_records(rawBids)
        erroneousRecords.coalesce(16)\
            .write.format("csv")\
            .mode("append")\
            .save(os.path.join(self.outputBasePath,
                               self.ERRONEOUS_DIR))
        # .option("header", "true") \

        # Read the exchange rate information.
        exchangeRates = self.get_exchange_rates(self.spark, self.exchangeRatesPath)

        # Transform the rawBids and use the BidItem case class.u
        bids = self.get_bids(rawBids, exchangeRates)

        # Load motels data.
        motels = self.get_motels(self.spark, self.motelsPath)

        # Join the bids with motel names and utilize EnrichedItem case class.
        enriched = self.get_enriched(bids, motels)
        enriched.coalesce(16)\
            .write.format("csv") \
            .mode("append") \
            .save(os.path.join(self.outputBasePath,
                               self.AGGREGATED_DIR))
        # .option("header", "true") \


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bids_path', type=str,
                        help='path to bids.txt file', default='spark_core/bids.snappy.parquet')
    parser.add_argument('--exchange_rate_path', type=str,
                        help='path to exchange_rate.txt file', default='spark_core/exchange_rate.snappy.parquet')
    parser.add_argument('--motels_path', type=str,
                        help='path to motels.txt file', default='spark_core/motels.snappy.parquet')
    parser.add_argument('--result_path', type=str,
                        help='path to result folder', default='spark_core/result')
    args = parser.parse_args()

    MotelsHomeRecommendation(
        args.bids_path,
        args.exchange_rate_path,
        args.motels_path,
        args.result_path
    ).process_data()