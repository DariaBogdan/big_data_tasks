from mrjob.job import MRJob
import re


WORD_RE = re.compile(r"[\w']+")
PARAM = 30  # amount of the longest words to be shown


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

        # creating a dict to aggregate words by its length
        words_by_len = {}
        for word in WORD_RE.findall(line):
            words_by_len[len(word)] = words_by_len.get(len(word), set())
            words_by_len[len(word)].add(word.lower())

        if len(words_by_len):  # yielding only if string is not empty
            max_len = max(words_by_len.keys())

            # yielding only needed amount of words
            for _ in range(min(PARAM, len(words_by_len[max_len]))):
                self.yielded_mapper += 1
                yield (max_len, words_by_len[max_len].pop())

    def combiner_init(self):
        self.yielded_combiner = 0

    def combiner(self, length, words):
        # words comes from different blocks, so they can be repeated
        # creating set to save only unique words
        words = set(words)
        while words and self.yielded_combiner != PARAM:
            self.yielded_combiner += 1
            yield (length, words.pop())

    def reducer_init(self):
        self.yielded_reducer = 0

    def reducer(self, length, words):
        # creating set to save only unique words
        words = set(words)
        while words and self.yielded_reducer != PARAM:
            self.yielded_reducer += 1
            yield None, (length, words.pop())


if __name__ == '__main__':
    MRMostUsedWord.run()