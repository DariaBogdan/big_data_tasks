"""
Finds several longest words.
Amount of words to be found are specified with --top argument
"""

from mrjob.job import MRJob, MRStep
import re
import mrjob

# regular expression to find words in line
WORD_RE = re.compile(r"[\w']+")


class MRLongestWord(MRJob):
    """Map Reduce Job that finds several longest words.

    Attributes:
        yielded: amount of already yielded words.
        yielded_words: already yielded words.

    """

    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    JOBCONF = {
        'mapreduce.job.reduces': 1,
        'mapred.output.key.comparator.class':
            'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
        'mapred.text.key.comparator.options': '-nr',  # compare keys numerically in reverse order
    }

    def configure_args(self):
        # add --top argument
        super().configure_args()
        self.add_passthru_arg(
            '--top',
            type=int,
            required=True,
            help='Amount of the longest words to be shown'
        )

    def mapper(self, _, line):
        # for every word yield word length as key and word as value
        for word in WORD_RE.findall(line):
            yield len(word), word.lower()

    def combiner_and_reducer_init(self):
        self.yielded = 0
        self.yielded_words = []

    def combiner_and_reducer(self, length, words):
        # yield only needed amount of words
        while words and self.yielded != self.options.top:
            next_word = next(words)
            # yield only if this word was not yielded yet
            if next_word not in self.yielded_words:
                self.yielded_words.append(next_word)
                self.yielded += 1
                yield length, next_word

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner_init=self.combiner_and_reducer_init,
                   combiner=self.combiner_and_reducer,
                   reducer_init=self.combiner_and_reducer_init,
                   reducer=self.combiner_and_reducer)
        ]


if __name__ == '__main__':
    MRLongestWord.run()