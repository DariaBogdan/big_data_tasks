import itertools
import os
import csv

from utils import KafkaProducerContextManger
from domain import MonitoringRecord

def read_csv_file(path):
    with open(path) as file:
        yield from csv.reader(file)


def handler(line, producer):
    monitoring_record = MonitoringRecord(*line)
    producer.send(topic='monitoring20',
                  key=monitoring_record.deviceId,
                  value=monitoring_record)

def main():
    lines = read_csv_file(os.path.join('..', 'data', 'one_device_2015-2017.csv'))
    with KafkaProducerContextManger() as producer:
        for line in lines:
            handler(line, producer)

if __name__ == "__main__":
    main()
