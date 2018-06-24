"""
Finds several longest words.
Amount of words to be found are specified with --top argument
"""

from mrjob.job import MRJob
import re
import mrjob

# regular expression to find words in line
WORD_RE = re.compile(r"[\w']+")


class MRLongestWord(MRJob):
    """Map Reduce Job that finds several longest words.

    Attributes:
        yielded_combiner: amount of already yielded words.
        yielded_words_combiner: already yielded words.
        yielded_reducer: amount of already yielded words.
        yielded_words_reducer: already yielded words.

    """

    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    JOBCONF = {
        'mapreduce.job.reduces': 1,
        'mapred.output.key.comparator.class':
            'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
        'mapred.text.key.comparator.options': '-nr',  # compare keys numerically in reverse order
    }

    def configure_options(self):
        # add --top argument
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
        self.yielded_words_combiner = []

    def combiner(self, length, words):
        # yield only needed amount of words
        while words and self.yielded_combiner != self.options.top:
            next_word = next(words)
            # yield only if this word was not yielded yet
            if next_word not in self.yielded_words_combiner:
                self.yielded_words_combiner.append(next_word)
                self.yielded_combiner += 1
                yield length, next_word

    def reducer_init(self):
        self.yielded_reducer = 0
        self.yielded_words_reducer = []

    def reducer(self, length, words):
        # yield only needed amount of words
        while words and self.yielded_reducer != self.options.top:
            next_word = next(words)
            # yield only if this word was not yielded yet
            if next_word not in self.yielded_words_reducer:
                self.yielded_words_reducer.append(next_word)
                self.yielded_reducer += 1
                yield length, next_word


if __name__ == '__main__':
    MRLongestWord.run()