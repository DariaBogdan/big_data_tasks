import functools
import mrjob
from mrjob.job import MRJob
import re
from ua_parser import user_agent_parser

regex = '(ip\d+) - - (\[.*\]) (".*") (\d+) (-|\d+) (".*") (".*")'
pairwise_sum = lambda x, y: (x[0] + y[0], x[1] + y[1])


class MRParceLogs(MRJob):

    def get_bytes(self, string):
        return int(string) if string.isnumeric() else 0

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg(
            '--compress', choices=['true', 'false'], default='true',
            help='Compress output or not'
        )
        self.add_passthru_arg(
            '--output_format', choices=['sequence', 'csv'], default='csv',
            help='Choose output format'
        )

    def hadoop_output_format(self):
        if self.options.output_format == 'sequence':
            return 'org.apache.hadoop.mapred.SequenceFileOutputFormat'

    def jobconf(self):
        conf = super().jobconf()
        if self.options.compress == 'true':
            conf.update({
                'mapred.output.compress': 'true',
                'mapred.output.compression.codec': 'org.apache.hadoop.io.compress.SnappyCodec',
                'mapred.output.compression.type': 'BLOCK'
            })
        return conf

    def output_protocol(self):
        if self.options.output_format == 'csv':
            return mrjob.protocol.RawValueProtocol()
        else:
            return mrjob.protocol.JSONProtocol()

    def mapper(self, _, line):
        matches = re.match(regex, line)
        if matches:
            groups = matches.groups()
            self.increment_counter('Browsers', user_agent_parser.ParseUserAgent(groups[6])['family'], 1)
            yield groups[0], (1, self.get_bytes(groups[4]))
        else:
            self.increment_counter('Incorrect input', 'Incorrect input', 1)

    def combiner(self, ip, requests_and_bytes):
        yield ip, functools.reduce(pairwise_sum, requests_and_bytes)

    def reducer(self, ip, requests_and_bytes):
        requests, bytes = functools.reduce(pairwise_sum, requests_and_bytes)
        if self.options.output_format == 'csv':
            yield 1, '{},{},{}'.format(ip, round(bytes/requests, 2), bytes)
        else:
            yield ip, (round(bytes/requests, 2), bytes)


if __name__ == '__main__':
    MRParceLogs.run()