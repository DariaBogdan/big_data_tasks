from mrjob.job import MRJob
import re

regex = re.compile('([a-zA-Z0-9]+)\t'  # BidID -- 0
                   '([0-9]+)\t'  # Timestamp -- 1
                   '(\d)+\t'  # LogType -- 2
                   '([a-zA-Z0-9]+)\t'  # iPinYouID -- 3
                   '(.+)\t'  #User-Agent -- 4
                   '([0-9.*]*)\t'  #IP -- 5
                   '(\d+)\t'  # RegionID -- 6
                   '(\d+)\t'  # CityID -- 7
                   '(\d)+\t'  # AdExchange -- 8
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
PRICE_LIMIT = 250

class MRCity(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--city_names_file')

    def mapper(self, _, line):
        matches = re.match(regex, line)
        if matches:
            groups = matches.groups()
            city_id = groups[7]
            bid_price = int(groups[19])
            if bid_price > PRICE_LIMIT:
                yield city_id, 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        city_names_dict = {}
        if self.options.city_names_file:
            with open(self.options.city_names_file, 'r') as city_file:
                for line in city_file:
                    city_id, city_name = line.split()
                    city_names_dict[city_id] = city_name
            yield city_names_dict.get(key, 'NO_SUCH_KEY'), sum(values)
        else:
            yield key, sum(values)

if __name__ == '__main__':
    MRCity.run()