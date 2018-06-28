from mrjob.job import MRJob

PRICE_LIMIT = 250

class MRCity(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--city_names_file')

    def mapper(self, _, line):
        res = line.split('\t')
        city_id = res[7]
        b_id = int(res[19])
        if b_id > PRICE_LIMIT:
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