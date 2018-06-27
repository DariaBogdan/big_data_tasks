import unittest.mock
import types

from longest_words import MRLongestWord


class TestMR(unittest.TestCase):

    def test_mapper_standard_input_line(self):
        job = MRLongestWord(args=['--top=2'])
        input = 'The story begins in the 1978. They met at school! Why?'
        res = job.mapper(None, input)

        expected_output = [(3, 'the'), (5, 'story'), (6, 'begins'), (2, 'in'), (3, 'the'), (4, '1978'), (4, 'they'),
                           (3, 'met'), (2, 'at'), (6, 'school'), (3, 'why')]
        self.assertEqual(list(res), expected_output)
        self.assertIsInstance(res, types.GeneratorType)

    def test_mapper_empty_input_line(self):
        job = MRLongestWord(args=['--top=2'])
        input = ''
        res = job.mapper(None, input)

        expected_output = []
        self.assertEqual(list(res), expected_output)
        self.assertIsInstance(res, types.GeneratorType)

    def test_mapper_input_line_without_words(self):
        job = MRLongestWord(args=['--top=2'])
        input = '!&&)((@&$*(@(@)(#**$&#$^@%%^@!&@^'
        res = job.mapper(None, input)

        expected_output = []
        self.assertEqual(list(res), expected_output)
        self.assertIsInstance(res, types.GeneratorType)

    def test_combiner_and_reducer_several_longest_correct(self):
        job = MRLongestWord(args=['--top=2'])
        job.combiner_and_reducer_init()

        input = iter(['dear', 'bear', 'word', 'cold', 'wind'])
        res = MRLongestWord.combiner_and_reducer(job,
                                     length=4,
                                     words=input)

        expected_output = [(4, 'dear'), (4, 'bear')]
        self.assertEqual(list(res), expected_output)
        self.assertIsInstance(res, types.GeneratorType)

    def test_combiner_and_reducer_only_one_longest_correct(self):
        job = MRLongestWord(args=['--top=1'])
        job.combiner_and_reducer_init()

        input = iter(['dear', 'bear', 'word', 'cold', 'wind'])
        res = MRLongestWord.combiner_and_reducer(job,
                                     length=4,
                                     words=input)

        expected_output = [(4, 'dear')]
        self.assertEqual(list(res), expected_output)
        self.assertIsInstance(res, types.GeneratorType)
