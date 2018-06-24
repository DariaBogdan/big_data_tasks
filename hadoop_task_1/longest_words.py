from mrjob.job import MRJob
import re
import mrjob


WORD_RE = re.compile(r"[\w']+")


class MRLongestWord(MRJob):

    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    JOBCONF = {
        'mapreduce.job.reduces': 1,
        'mapred.output.key.comparator.class':
            'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
        'mapred.text.key.comparator.options': '-nr',  # compare keys numerically in reverse order
    }

    def configure_options(self):
        super().configure_options()
        self.add_passthrough_option(
            '--top',
            type='int',
            required=True,
            help='Amount of the longest words to be shown'
        )

    def mapper(self, _, line):
        # for every word yield word length as key and word as value
        for word in WORD_RE.findall(line):
            yield len(word), word.lower()

    def combiner_init(self):
        self.yielded_combiner = 0
        self.yielded_words_combiner = set()  # to show only unique words

    def combiner(self, length, words):
        while words and self.yielded_combiner != self.options.top:
            next_word = next(words)
            if next_word not in self.yielded_words_combiner:
                self.yielded_words_combiner.add(next_word)
                self.yielded_combiner += 1
                yield length, next_word

    def reducer_init(self):
        self.yielded_reducer = 0
        self.yielded_words_reducer = set() # to show only unique words

    def reducer(self, length, words):
        while words and self.yielded_reducer != self.options.top:
            next_word = next(words)
            if next_word not in self.yielded_words_reducer:
                self.yielded_words_reducer.add(next_word)
                self.yielded_reducer += 1
                yield length, next_word


if __name__ == '__main__':
    MRLongestWord.run()