import unittest.mock
import types

from longest_words import MRLongestWord


class TestMR(unittest.TestCase):

    def test_mapper(self):
        input = 'hello my dear friend'
        res = MRLongestWord.mapper(None, None, input)

        expected_output = [(5, 'hello'), (2, 'my'), (4, 'dear'), (6, 'friend')]
        self.assertEqual(list(res), expected_output)
        self.assertIsInstance(res, types.GeneratorType)

    def test_combiner_and_reducer(self):
        job = MRLongestWord(args=['--top=2'])
        job.combiner_and_reducer_init()

        input = iter(['dear', 'bear', 'word', 'cold', 'wind'])
        res = MRLongestWord.combiner_and_reducer(job,
                                     length=4,
                                     words=input)

        expected_output = [(4, 'dear'), (4, 'bear')]
        self.assertEqual(list(res), expected_output)
        self.assertIsInstance(res, types.GeneratorType)
