import contextlib

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import kafka

from domain import MonitoringRecord


class KafkaProducerContextManger:
    def __init__(self):
        self.producer = kafka.KafkaProducer(
            bootstrap_servers='sandbox-hdp.hortonworks.com:6667',
            key_serializer=str.encode,
            value_serializer=MonitoringRecord.serializer()
        )

    def __enter__(self):
        return self.producer

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.flush()
        self.producer.close()


class SparkContextManger:
    def __init__(self, app_name='spark streaming app', mode='local[2]'):
        self.app_name = app_name
        self.mode = mode
        self.sc = None

    def __enter__(self):
        conf = SparkConf()#.setMaster(self.mode).setAppName(self.app_name)
        self.sc = SparkContext.getOrCreate(conf=conf)

        logger = self.sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.WARN)
        return self.sc

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sc.stop()

@contextlib.contextmanager
def get_streaming_context(sc):
    ssc = StreamingContext(sc, batchDuration=2)
    logger = ssc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.WARN)

    yield ssc
    ssc.start()
    ssc.awaitTermination()
