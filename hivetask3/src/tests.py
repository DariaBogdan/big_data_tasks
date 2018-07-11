import io
import unittest.mock

from parse_user_agent import parse_ua


class TestParser(unittest.TestCase):

    @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    def test_parcer_correct(self, mock_stdout):
        input = '234\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)'
        expected_output = '234\tOther\tWindows\tIE\n'
        parse_ua(input)
        self.assertEqual(expected_output, mock_stdout.getvalue())

    def test_parcer_incorrect_too_many_values(self):
        input = '1\t2\t3'
        with self.assertRaises(ValueError) as context:
            parse_ua(input)
        self.assertTrue('too many values to unpack (expected 2)' in str(context.exception))

    def test_parcer_incorrect_not_enough_values(self):
        input = '1'
        with self.assertRaises(ValueError) as context:
            parse_ua(input)
        self.assertTrue('not enough values to unpack (expected 2, got 1)' in str(context.exception))