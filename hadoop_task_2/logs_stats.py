from mrjob.job import MRJob
import re
import functools

WORD_RE = re.compile(r"[\w']+")
regex = '(ip\d+) - - (\[.*\]) (".*") (\d+) (-|\d+) (".*") (".*")'

pairwise_sum = lambda x, y: (x[0] + y[0], x[1] + y[1])

class MRParceLogs(MRJob):

    def mapper(self, _, line):
        m = re.match(regex, line)
        if m:
            g = m.groups()
            yield g[0], ((1, int(g[4])) if g[4].isnumeric() else (1, 0))


    def combiner(self, ip, requests_and_bytes):
        yield ip, functools.reduce(pairwise_sum, requests_and_bytes)


    def reducer(self, ip, requests_and_bytes):
        requests, bytes = functools.reduce(pairwise_sum, requests_and_bytes)
        yield 1, (ip, round(bytes/requests, 2), bytes)


if __name__ == '__main__':
    MRParceLogs.run()