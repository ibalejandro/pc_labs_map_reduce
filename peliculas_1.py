from mrjob.job import MRJob


class MRAvgRating(MRJob):

  def mapper(self, _, line):
    splitted_line = line.split()
    yield splitted_line[0], int(splitted_line[3])

  def reducer(self, key, values):
    rating_sum = 0
    values_for_key = 0
    for rating in values:
      rating_sum += rating
      values_for_key += 1
    yield "User " + key, (values_for_key, rating_sum / values_for_key)


if __name__ == '__main__':
  MRAvgRating.run()
