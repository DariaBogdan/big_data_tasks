from pyspark import SparkConf, Row
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class MotelsHomeRecommendation:
    ERRONEOUS_DIR = "erroneous"
    AGGREGATED_DIR = "aggregated"

    def __init__(self, bidsPath, exchangeRatesPath, motelsPath, outputBasePath):
        self.bidsPath = bidsPath
        self.exchangeRatesPath = exchangeRatesPath
        self.motelsPath = motelsPath
        self.outputBasePath = outputBasePath
        self.sc = SparkContext.getOrCreate(SparkConf())
        self.spark = SparkSession(self.sc)

    def getRawBids(self, sc, bidsPath):
        text_file = sc.textFile(bidsPath)
        return text_file.map(lambda row: row)

    def getErroneousRecords(self, rawBids):
        rdd = rawBids.map(lambda r: r) \
            .filter(lambda r: r.find('ERROR') > 0) \
            .map(lambda x: x.split(',')) \
            .map(lambda x: Row(motelId=x[0], date=x[1], error=x[2])) \
            .map(lambda x: ((x.date, x.error), 1)) \
            .reduceByKey(lambda a, b: a + b)
        return rdd

    def getExchangeRates(self, sc, exchangeRatesPath):
        text_file = sc.textFile(exchangeRatesPath)
        return text_file.map(lambda r: [r.split(',')[i] for i in [0, 3]])

    def getBids(self, rawBids, exchangeRates):
        w = ['US', 'MX', 'CA']
        rdd = rawBids \
            .filter(lambda r: not "ERROR" in r) \
            .map(lambda r: [r.split(',')[i] for i in [0, 1, 5, 6, 8]]) \
            .flatMap(lambda x: [Row(date=x[1], motelId=x[0], bid=x[i + 2], country=w[i]) for i in range(3)]) \
            .filter(lambda x: x.bid != '') \
            .map(lambda x: (x.date, (x.motelId, x.bid, x.country))) \
            .leftOuterJoin(exchangeRates) \
            .map(lambda x: Row(date=x[0], motelId=x[1][0][0], bid=x[1][0][1], country=x[1][0][2], ex_rate=x[1][1])) \
            .map(lambda x: (x.motelId, (x.date, round(float(x.bid) * float(x.ex_rate), 3), x.country)))
        return rdd

    def getMotels(self, sc, motelsPath):
        text_file = sc.textFile(motelsPath)
        return text_file.map(lambda r: [r.split(',')[i] for i in [0, 1]])

    def getEnriched(self, bids, motels):
        rdd = bids.leftOuterJoin(motels) \
            .map(lambda x: (Row(motelId=x[0], date=x[1][0][0]), Row(bid=x[1][0][1], country=x[1][0][2], name=x[1][1]))) \
            .reduceByKey(lambda x, y: max(x, y, key=lambda y: y.bid)) \
            .map(lambda x: (x[0].motelId, x[1].name, x[0].date, x[1].country, x[1].bid))
        return rdd

    def processData(self):
        # Read the bid data from the provided file.
        rawBids = self.getRawBids(self.sc, self.bidsPath)

        # Collect the errors and save the result
        erroneousRecords = self.getErroneousRecords(rawBids)
        erroneousRecords.saveAsTextFile(f"{self.outputBasePath}/{self.ERRONEOUS_DIR}")

        # Read the exchange rate information.
        exchangeRates = self.getExchangeRates(self.sc, self.exchangeRatesPath)

        # Transform the rawBids and use the BidItem case class.u
        bids = self.getBids(rawBids, exchangeRates)

        # Load motels data.
        motels = self.getMotels(self.sc, self.motelsPath)

        # Join the bids with motel names and utilize EnrichedItem case class.
        enriched = self.getEnriched(bids, motels)
        enriched.saveAsTextFile(f"{self.outputBasePath}/{self.AGGREGATED_DIR}")

MotelsHomeRecommendation('bids.txt', 'exchange_rate.txt', 'motels.txt', 'result.txt').processData()