"""
Calculates amount of high-bid-priced  (more than 250) impression events by city.
Reducer is defined by OperationSystemType from User Agent.
"""

from mrjob.job import MRJob
import re
from ua_parser import user_agent_parser

# correct input lines matches
regex = re.compile('([a-zA-Z0-9]+)\t'  # BidID -- 0
                   '([0-9]+)\t'  # Timestamp -- 1
                   '(\d)+\t'  # LogType -- 2
                   '(.+)\t'  # iPinYouID -- 3
                   '(.*)\t'  #User-Agent -- 4
                   '([0-9.*]*)\t'  #IP -- 5
                   '(\d+)\t'  # RegionID -- 6
                   '(\d+)\t'  # CityID -- 7
                   '([0-9a-zA_Z]+)\t'  # AdExchange -- 8
                   '([a-zA-Z0-9]+)\t'  # Domain -- 9
                   '([a-zA-Z0-9]+)\t'  # URl -- 10
                   '(null)\t'  # Anonymous URL -- 11
                   '([0-9_a-zA-Z]+)\t'  # Ad Slot ID -- 12
                   '(\d+)\t'  # Ad Slot Width -- 13
                   '(\d+)\t'  # Ad Slot Heigth -- 14
                   '([a-zA-Z]+)\t'  # Ad Slot Visibility -- 15
                   '(Na)\t'  # Ad Slot Format -- 16
                   '(\d+)\t'  # Ad Slot Floor Price -- 17
                   '([0-9a-zA-Z]+)\t'  # Creative ID -- 18
                   '(\d+)\t'  # Bidding Price -- 19
                   '(\d+)\t'  # Paying Price -- 20
                   '(null)\t'  # Landing Page URL -- 21
                   '(\d+)\t'  # Advertiser ID -- 22
                   '(.+)')  # User Profile IDs -- 23

# high price
PRICE_LIMIT = 250


class MRCity(MRJob):

    KEY_FIELD_SEPARATOR = ';'

    def configure_args(self):
        super().configure_args()
        # distributed cache
        self.add_file_arg('--city_names_file')
        # number of reducers
        self.add_passthru_arg(
            '--reducers_num',
            type=int,
            required=False,
            default=1,
            help='Amount of reducers to use'
        )

    def jobconf(self):
        conf = super().jobconf()
        conf.update({
            'mapreduce.job.reduces': self.options.reducers_num,
            'mapreduce.map.output.key.field.separator': self.KEY_FIELD_SEPARATOR,
            'mapreduce.partition.keypartitioner.options': '-k2',  # choose reducer by second part of the key

            'mapreduce.output.key.comparator.class':
                'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapreduce.text.key.comparator.options': '-k1n'  # sort keys to be able to manually group them in reducer
        })
        return conf

    def mapper(self, _, line):
        # Yields only if line is correct and price is greater than PRICE_LIMIT.
        # Key is composite: {city_id}{KEY_FIELD_SEPARATOR}{OS}.
        # Such key is used in KeyFieldBasedPartitioner;
        # using such partitioner we can choose reducer number by OS name.
        matches = re.match(regex, line)
        if matches:
            groups = matches.groups()
            city_id = groups[7]
            bid_price = int(groups[19])
            os = user_agent_parser.Parse(groups[4])['os']['family']
            if bid_price > PRICE_LIMIT:
                yield self.KEY_FIELD_SEPARATOR.join((city_id, os)), 1
        else:
            self.increment_counter('Incorrect data', 'Incorrect input line', 1)

    def partitioner(self):
        # Chooses reducer by only one part of composite key.
        return 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer_init(self):
        # In reducer we have to group values by only the first part of the composite key.
        # According to comparator options set in jobconf, keys are sorted.
        # The res variable is increasing while incoming city_id is the same as previous.
        # So we have to accumulate values and check if city_id has changed.
        # Here initial values of variables are set.
        self.res = 0
        self.city_id = "0"
        if self.options.city_names_file:
            # creating dict to replace city_id with city_name
            self.city_names_dict = {}
            with open(self.options.city_names_file, 'r') as city_file:
                for line in city_file:
                    city_id, city_name = line.split()
                    self.city_names_dict[city_id] = city_name

    def reducer(self, key, values):
        city_id = key.split(self.KEY_FIELD_SEPARATOR)[0]
        # if city has been changed, yield accumulated values for the previous city
        if city_id != self.city_id:
            if self.options.city_names_file:
                try:
                    yield self.city_names_dict[self.city_id], self.res
                except KeyError:
                    self.increment_counter('Incorrect data', 'Incorrect city id')
            else:
                yield self.city_id, self.res
            self.city_id = city_id
            self.res = sum(values)
        # if city has not been changed, accumulate values for this city
        else:
            self.res += sum(values)
        return

    def reducer_final(self):
        # We yield values only when city_id changes.
        # Thus we have to yield the last city here.
        if self.options.city_names_file:
            try:
                yield self.city_names_dict[self.city_id], self.res
            except KeyError:
                self.increment_counter('Incorrect data', 'Incorrect city id')
        else:
            yield self.city_id, self.res


if __name__ == '__main__':
    MRCity.run()