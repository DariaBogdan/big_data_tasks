from mrjob.job import MRJob
import re


WORD_RE = re.compile(r"[\w']+")
PARAM = 30

class MRMostUsedWord(MRJob):

    JOBCONF = {
        'mapreduce.job.reduces': 1,
        'mapred.output.key.comparator.class':
            'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
        'mapred.text.key.comparator.options': '-nr',  # compare keys numerically in reverse order
    }

    def mapper_init(self):
        self.yielded_mapper = 0

    def mapper(self, _, line):
        if self.yielded_mapper == PARAM:
            return

        d = {}
        for word in WORD_RE.findall(line):
            d[len(word)] = d.get(len(word), set())
            d[len(word)].add(word.lower())
        if len(d):
            max_len = max(d.keys())

            for i in range(min(PARAM, len(d[max_len]))):
                self.yielded_mapper += 1
                yield (max_len, d[max_len].pop())

    def combiner_init(self):
        self.yielded_combiner = 0

    def combiner(self, length, words):
        words = set(words)
        while words and self.yielded_combiner != PARAM:
            self.yielded_combiner +=1
            yield (length, words.pop())

    def reducer_init(self):
        self.yielded_reducer = 0

    def reducer(self, length, words):
        words = set(words)
        while words and self.yielded_reducer != PARAM:
            self.yielded_reducer += 1
            yield None, (length, words.pop())

if __name__ == '__main__':
    MRMostUsedWord.run()