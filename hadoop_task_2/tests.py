from io import BytesIO
import unittest

from logs_stats import MRParceLogs


class TestMR(unittest.TestCase):

    def test_mrjob_correct_line(self):

        mr_job = MRParceLogs(args=['--compress=false', '--output_format=csv'])

        fake_input = b'ip488 - - [25/Apr/2011:11:33:33 -0400] "GET /~techrat/vw_spotters/vw_beetle_f.jpg HTTP/1.1" ' \
                     b'200 28153 "http://host2/~techrat/vw_spotters/" "Mozilla/4.0 (compatible; MSIE 8.0; Windows ' \
                     b'NT 5.1; Trident/4.0; GTB6.6; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; ' \
                     b'.NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; MDDR; InfoPath.2)"'
        mr_job.sandbox(stdin=BytesIO(fake_input), stdout=None)

        results = []
        with mr_job.make_runner() as runner:
            runner.run()
            for line in runner.cat_output():
                key, value = mr_job.parse_output_line(line)
                results.append(value)

        self.assertEqual(results, ['ip488,28153.0,28153\n'])
        self.assertEqual(runner.counters(), [{'Browsers': {'IE': 1}}])


    def test_mrjob_incorrect_line(self):

        mr_job = MRParceLogs(args=['--compress=false', '--output_format=csv'])

        fake_input = b'ip488 - - [25/Apr/2011:11:33:33 -0400] "GET /~techrat/vw_spotters/vw_beetle_f.jpg HTTP/1.1" ' \
                     b'200 bytes "http://host2/~techrat/vw_spotters/" "Mozilla/4.0 (compatible; MSIE 8.0; Windows ' \
                     b'NT 5.1; Trident/4.0; GTB6.6; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; ' \
                     b'.NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; MDDR; InfoPath.2)"'
        mr_job.sandbox(stdin=BytesIO(fake_input), stdout=None, stderr=None)

        results = []
        with mr_job.make_runner() as runner:
            runner.run()
            for line in runner.cat_output():
                key, value = mr_job.parse_output_line(line)
                results.append(value)

        self.assertEqual(results, [])
        self.assertEqual(runner.counters(), [{'Incorrect input': {'Incorrect input': 1}}])