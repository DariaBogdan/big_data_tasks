from io import BytesIO
import itertools
import unittest
import types

from task32 import MRCity


class TestMR(unittest.TestCase):

    def test_mapper_correct_line_big_price(self):
        mr_job = MRCity()
        input = '2e72d1bd7185fb76d69c852c57436d37	20131019025500549	1	CAD06D3WCtf	' \
                'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)	113.117.187.*	' \
                '216	234	2	33235ca84c5fee9254e6512a41b3ad5e	8bbb5a81cc3d680dd0c27cf4886ddeae	' \
                'null	3061584349	728	90	OtherView	Na	5	7330	277	48	null	2259	' \
                '10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063'
        expected_output = [('234;Windows', 1)]
        res = mr_job.mapper(0, input)
        self.assertEqual(expected_output, list(res))

    def test_mapper_correct_line_low_price(self):
        mr_job = MRCity()
        input = '2e72d1bd7185fb76d69c852c57436d37	20131019025500549	1	CAD06D3WCtf	' \
                'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)	113.117.187.*	' \
                '216	234	2	33235ca84c5fee9254e6512a41b3ad5e	8bbb5a81cc3d680dd0c27cf4886ddeae	' \
                'null	3061584349	728	90	OtherView	Na	5	7330	148	48	null	2259	' \
                '10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063'
        expected_output = []
        res = mr_job.mapper(0, input)
        self.assertEqual(expected_output, list(res))

    def test_mapper_incorrect_line(self):
        mr_job = MRCity()
        input = b'2e72d1bd7185fb76d69c852c57436d37	20131019025500549	1	CAD06D3WCtf	' \
                b'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)	113.117.187.*	' \
                b'216	234	2	33235ca84c5fee9254e6512a41b3ad5e	8bbb5a81cc3d680dd0c27cf4886ddeae	' \
                b'null	3061584349	728	90	OtherView	Na	5	7330	nnnn	48	null	2259	' \
                b'10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063'
        mr_job.sandbox(stdin=BytesIO(input), stdout=None, stderr=None)
        expected_output = [0]
        results = []
        with mr_job.make_runner() as runner:
            runner.run()
            for line in runner.cat_output():
                key, value = mr_job.parse_output_line(line)
                results.append(value)

        self.assertEqual(results, expected_output)
        self.assertEqual(runner.counters(), [{'Incorrect data': {'Incorrect input line': 1}}])

    def test_combiner(self):
        mr_job = MRCity()
        key = '126;Windows'
        values = [1, 1, 1, 1, 1]
        expected_output = [(key, 5)]
        res = mr_job.combiner(key, values)
        self.assertEqual(expected_output, list(res))
        self.assertIsInstance(res, types.GeneratorType)

    def test_reducer(self):
        mr_job = MRCity()
        g1 = mr_job.reducer_init()
        self.assertEqual(mr_job.city_id, '0')
        self.assertEqual(mr_job.res, 0)

        g2 = mr_job.reducer('1;aaa', [2, 5])
        self.assertIsInstance(g2, types.GeneratorType)

        g3 = mr_job.reducer('1;bbb', [3])
        self.assertIsInstance(g3, types.GeneratorType)

        g4 = mr_job.reducer('2;aaa', [4])
        self.assertIsInstance(g4, types.GeneratorType)

        g5 = mr_job.reducer_final()

        self.assertEqual([('0', 0),('1', 10),('2', 4)], list(itertools.chain(g2, g3, g4, g5)))


    def test_reducer_with_city_name(self):
        mr_job = MRCity(['--city_names_file=city.en.txt'])
        mr_job.reducer_init()
        self.assertEqual(mr_job.city_id, '0')
        self.assertEqual(mr_job.res, 0)

        g2 = mr_job.reducer('126;aaa', [2, 5])
        self.assertIsInstance(g2, types.GeneratorType)

        g3 = mr_job.reducer('126;bbb', [3])
        self.assertIsInstance(g3, types.GeneratorType)

        g4 = mr_job.reducer('127;aaa', [4])
        self.assertIsInstance(g4, types.GeneratorType)

        g5 = mr_job.reducer_final()

        self.assertEqual([('unknown', 0), ('xiamen', 10), ('putian', 4)], list(itertools.chain(g2, g3, g4, g5)))
