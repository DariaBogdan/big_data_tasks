from pyspark.streaming.kafka import KafkaUtils
from py4j.java_gateway import JavaGateway

from domain import MonitoringRecord
from utils import SparkContext, get_streaming_context, KafkaProducerContextManger

def process(records):
    gateway = JavaGateway()
    entry_point = gateway.entry_point

    with KafkaProducerContextManger() as producer:
        for key, record in records:
            java_record_enriched = entry_point.mappingFunc(record.deviceId, record.toJava(gateway.jvm))
            enriched_record = MonitoringRecord.fromJava(java_record_enriched)
            producer.send(
                topic='monitoringEnriched2',
                key=key,
                value=enriched_record)

def handler(ssc, topic):
    stream = KafkaUtils.createDirectStream(
        ssc,
        topics=[topic],
        kafkaParams={"metadata.broker.list": 'sandbox-hdp.hortonworks.com:6667'},
        valueDecoder=MonitoringRecord.deserializer())
    stream.foreachRDD(lambda rdd: rdd.foreachPartition(process))

def main():
    with SparkContext() as sc:
        with get_streaming_context(sc) as ssc:
            handler(ssc, 'monitoring20')

if __name__ == "__main__":
    main()