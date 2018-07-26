from pyspark import SparkConf, Row
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class MotelsHomeRecommendation:
    ERRONEOUS_DIR = "erroneous"
    AGGREGATED_DIR = "aggregated"

    def getRawBids(sc, bidsPath):
        text_file = sc.textFile(bidsPath)
        return text_file.map(lambda row: row)

    def getErroneousRecords(rawBids):
        rdd = rawBids.map(lambda r: r) \
            .filter(lambda r: r.find('ERROR') > 0) \
            .map(lambda x: (','.join(x.split(',')[1:]), 1)) \
            .reduceByKey(lambda a, b: a + b)
        rdd.saveAsTextFile('rdd9')
        return rdd

    def getExchangeRates(sc, exchangeRatesPath):
        text_file = sc.textFile(exchangeRatesPath)
        return text_file.map(lambda r: [r.split(',')[i] for i in [0, 3]])

    def getBids(rawBids, exchangeRates):
        w = ['US', 'MX', 'CA']
        rdd = rawBids \
            .filter(lambda r: not "ERROR" in r) \
            .map(lambda r: [r.split(',')[i] for i in [0, 1, 5, 6, 8]]) \
            .flatMap(lambda x: [(x[1], x[0], x[i + 2], w[i]) for i in range(3)]) \
            .filter(lambda x: x[2] != '') \
            .map(lambda x: (x[0], x[1:])) \
            .leftOuterJoin(exchangeRates) \
            .map(lambda x: (x[1][0][0], (x[0], round(float(x[1][0][1]) * float(x[1][1]), 3), x[1][0][2])))
        return rdd

    def getMotels(sc, motelsPath):
        text_file = sc.textFile(motelsPath)
        return text_file.map(lambda r: [r.split(',')[i] for i in [0, 1]])

    def getEnriched(bids, motels):
        rdd = bids.leftOuterJoin(motels) \
            .map(lambda x: ((x[0], x[1][0]), (x[1][1], x[1][2]))) \
            .reduceByKey(lambda x, y: max(x, y, key=lambda x: x[0]))
        return rdd

    def processData(self, sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath):

        # Read the bid data from the provided file.
        rawBids = self.getRawBids(sc, bidsPath)

        # Collect the errors and save the result
        erroneousRecords = self.getErroneousRecords(rawBids)
        erroneousRecords.saveAsTextFile(f"{outputBasePath}/{ERRONEOUS_DIR}")

        # Read the exchange rate information.
        exchangeRates = self.getExchangeRates(sc, exchangeRatesPath)

        # Transform the rawBids and use the BidItem case class.
        bids = self.getBids(rawBids, exchangeRates)

        # Load motels data.
        motels = self.getMotels(sc, motelsPath)

        # Join the bids with motel names and utilize EnrichedItem case class.
        enriched = self.getEnriched(bids, motels)
        enriched.saveAsTextFile(f"{outputBasePath}/{AGGREGATED_DIR}")

    def main(self, args):
        bidsPath = args[0]
        motelsPath = args[1]
        exchangeRatesPath = args[2]
        outputBasePath = args[3]
        sc = SparkContext.getOrCreate(SparkConf())
        spark = SparkSession(sc)
        self.processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)



