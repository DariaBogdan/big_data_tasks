import json
import sys

print(sys.version)

MAIN_SCHEMA = [
    {'name': 'stateCode', 'type': str,},
    {'name': 'countryCode', 'type': str,},
    {'name': 'siteNum', 'type': str, },
    {'name': 'parameterCode', 'type': str, },
    {'name': 'poc', 'type': str, },
    {'name': 'latitude', 'type': str, },
    {'name': 'longitude', 'type': str, },
    {'name': 'datum', 'type': str, },
    {'name': 'parameterName', 'type': str, },
    {'name': 'dateLocal', 'type': str, },
    {'name': 'timeLocal', 'type': str, },
    {'name': 'dateGMT', 'type': str, },
    {'name': 'timeGMT', 'type': str, },
    {'name': 'sampleMeasurement', 'type': str, },
    {'name': 'unitsOfMeasure', 'type': str, },
    {'name': 'mdl', 'type': str, },
    {'name': 'uncertainty', 'type': str, },
    {'name': 'qualifier', 'type': str, },
    {'name': 'methodType', 'type': str, },
    {'name': 'methodCode', 'type': str, },
    {'name': 'methodName', 'type': str, },
    {'name': 'stateName', 'type': str, },
    {'name': 'countyName', 'type': str, },
    {'name': 'dateOfLastChange', 'type': str, },
]

ADD_SCHEMA = [
    {'name': 'prediction', 'type': float, },
    {'name': 'error', 'type': float, },
    {'name': 'anomaly', 'type': float, },
    {'name': 'predictionNext', 'type': float, },
]

SCHEMA = MAIN_SCHEMA + ADD_SCHEMA


def get_dict(inst):
    return {element['name']: inst.__dict__[element['name']] for element in SCHEMA}


class MonitoringRecord:
    _encoding = 'utf8'

    def __init__(self, *args, **kwargs):
        if args:
            for i in zip(args, SCHEMA):
                setattr(self, i[1]['name'], i[1]['type'](i[0]))
        if kwargs:
            for name, val in kwargs.items():
                setattr(self, name, val)
        if len(args) == len(MAIN_SCHEMA) or len(kwargs) == len(MAIN_SCHEMA):
            for i in ADD_SCHEMA:
                setattr(self, i['name'], i['type'](0))

    def __repr__(self):
        return ','.join((str(getattr(self, element['name'])) for element in SCHEMA))

    @property
    def deviceId(self):
        return '{0.stateCode}-{0.countryCode}-{0.siteNum}-{0.parameterCode}-{0.poc}'.format(self)

    @classmethod
    def serializer(cls):
        return lambda inst: json.dumps(get_dict(inst)).encode(cls._encoding)

    @classmethod
    def deserializer(cls):
        return lambda i: cls(**json.loads(i.decode(cls._encoding)))

    def toJava(self, jvm):
        json_str = json.dumps(get_dict(self))
        return jvm.com.epam.bcc.htm.MonitoringRecord.fromJson(json_str)

    @classmethod
    def fromJava(cls, JavaMonitoringRecord):
        return cls(**json.loads(JavaMonitoringRecord.toJson()))