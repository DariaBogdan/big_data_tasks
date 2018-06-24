import unittest.mock
import types

from longest_words import MRLongestWord


class TestMapper(unittest.TestCase):

    def test_assert_stdout(self):
        res = MRLongestWord.mapper(None, None, 'hello my dear friend')

        expected_output = [(5, 'hello'), (2, 'my'), (4, 'dear'), (6, 'friend')]
        self.assertEqual(list(res), expected_output)
        self.assertIsInstance(res, types.GeneratorType)


class TestCombiner(unittest.TestCase):

    def test_assert_stdout(self):
        options_mocked = unittest.mock.Mock(top=2)
        self_mocked = unittest.mock.Mock(yielded_combiner=0,
                                         yielded_words_combiner=set(),
                                         options=options_mocked)

        res = MRLongestWord.combiner(self_mocked,
                                     length=4,
                                     words=iter(['dear', 'bear', 'word', 'cold', 'wind']))

        expected_output = [(4, 'dear'), (4, 'bear')]
        self.assertEqual(list(res), expected_output)
        self.assertIsInstance(res, types.GeneratorType)


class TestReducer(unittest.TestCase):

    def test_assert_stdout(self):
        options_mocked = unittest.mock.Mock(top=2)
        self_mocked = unittest.mock.Mock(yielded_reducer=0,
                                         yielded_words_reducer=set(),
                                         options=options_mocked)

        res = MRLongestWord.reducer(self_mocked,
                                    length=4,
                                    words=iter(['dear', 'bear', 'word', 'cold', 'wind']))

        expected_output = [(4, 'dear'), (4, 'bear')]
        self.assertEqual(list(res), expected_output)
        self.assertIsInstance(res, types.GeneratorType)