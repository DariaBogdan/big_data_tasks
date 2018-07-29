from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from classes import BidError, EnrichedItem, BidItem

COUNTRIES = ['US', 'MX', 'CA']


def transform_date(date):
    """ Transform date format from "hour-day-month-year" to "year-month-day hour:00"

    :param date: string with date in format "hour-day-month-year"
    :return: string with date in format "year-month-day hour:00"
    """
    hour, day, month, year = date.split('-')
    return f"{year}-{month}-{day} {hour}:00"


def to_euro(price_usd, exhange_rate):
    """ Transform USD to EUR and round result.

    :param price_usd: price in USD
    :param exhange_rate: exchange rate
    :return: float -- price in EUR
    """
    try:
        return round(float(price_usd) * float(exhange_rate), 3)
    except ValueError:
        return ''


def expand(x):
    """ Takes tuple: row from file 'bids.txt' and exchange
     rate for date from this row. Select only values for
     countries in COUNTRIES and expand inputed row to three
     rows: one row for each country. For each row transform
     date to needed format and USD to EUR.

    :param x: tuple
    :return: list of three BidItem elements
    """
    rawBid, exchangeRate = x
    motelId = rawBid[0]
    transformed_date = transform_date(rawBid[1])
    result = []
    for raw_idx, loSa in zip([5, 6, 8], COUNTRIES):
        result.append(
            BidItem(
                motelId=motelId,
                bidDate=transformed_date,
                loSa=loSa,
                price=to_euro(rawBid[raw_idx], exchangeRate)
            )
        )
    return result


class MotelsHomeRecommendation:
    ERRONEOUS_DIR = "erroneous"
    AGGREGATED_DIR = "aggregated"
    DELIMITER = ","

    def __init__(self, bidsPath, exchangeRatesPath, motelsPath, outputBasePath):
        self.bidsPath = bidsPath
        self.exchangeRatesPath = exchangeRatesPath
        self.motelsPath = motelsPath
        self.outputBasePath = outputBasePath
        self.sc = SparkContext.getOrCreate(SparkConf())
        self.spark = SparkSession(self.sc)

    def getRawBids(self, sc, bidsPath):
        """ Read file to RDD

        :param sc: spark context
        :param bidsPath: path to file
        :return: rdd
        """
        text_file = sc.textFile(bidsPath)
        rdd = text_file\
            .map(lambda r: r.split(","))
        return rdd

    def getErroneousRecords(self, rawBids):
        """ Calculates amount of specific errors for hour

        :param rawBids: raw from file 'bids.txt'
        :return: string with "{date},{error message},{amount}"
        """
        rdd = rawBids\
            .map(lambda x: BidError(date=x[1], errorMessage=x[2])) \
            .filter(lambda x: x.errorMessage.find('ERROR') >= 0) \
            .map(lambda x: ((x.date, x.errorMessage), 1)) \
            .reduceByKey(lambda a, b: a + b)\
            .map(lambda x: ','.join([','.join(x[0]), str(x[1])]))
        return rdd

    def getExchangeRates(self, sc, exchangeRatesPath):
        """ Read file to RDD

        :param sc: spark context
        :param exchangeRatesPath: path to file
        :return: rdd
        """
        text_file = sc.textFile(exchangeRatesPath)
        rdd = text_file\
            .map(lambda r: r.split(",")) \
            .map(lambda r: [r[i] for i in [0, 3]])  # select only needed values
        return rdd

    def getBids(self, rawBids, exchangeRates):
        rdd = rawBids \
            .filter(lambda r: not "ERROR" in r[2]) \
            .keyBy(lambda r: r[1]) \
            .leftOuterJoin(exchangeRates) \
            .values() \
            .flatMap(expand) \
            .filter(lambda r: r.price != '')
        return rdd

    def getMotels(self, sc, motelsPath):
        """ Read file to RDD

        :param sc: spark context
        :param motelsPath: path to file
        :return: rdd
        """
        text_file = sc.textFile(motelsPath)
        rdd = text_file \
            .map(lambda r: r.split(',')) \
            .map(lambda r: [r[i] for i in [0, 1]])  # select only motelId and motelName
        return rdd

    def getEnriched(self, bids, motels):
        """  Enrich the data and find the maximum

        :param bids: rdd with bids
        :param motels: rdd with motels
        :return: string containing '{motelId},{motelName},{formatted date},{loSa},{max price}'
        """
        rdd = bids \
            .keyBy(lambda x: x.motelId) \
            .leftOuterJoin(motels) \
            .map(lambda x: (EnrichedItem(
                            motelId=x[0],
                            bidDate=x[1][0].bidDate,
                            price=x[1][0].price,
                            loSa=x[1][0].loSa,
                            motelName=x[1][1]))) \
            .keyBy(lambda x: (x.bidDate, x.motelId)) \
            .reduceByKey(lambda x, y: max(x, y, key=lambda x: int(x.price))) \
            .values() \
            .map(str)
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


if __name__ == '__main__':
    MotelsHomeRecommendation(
        'bids.txt',
        'exchange_rate.txt',
        'motels.txt',
        'result.txt'
    ).processData()