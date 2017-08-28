from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWorstAvgRatingDay(MRJob):

  def steps(self):
    # 2 steps: calculate average rating and find the day for the minimum one.
    return [
      MRStep(mapper=self.mapper,
             reducer=self.reducer_calculate_avg_rating),
      MRStep(reducer=self.reducer_find_min_day)
    ]

  def mapper(self, _, line):
    splitted_line = line.split()
    yield splitted_line[4], int(splitted_line[3])

  def reducer_calculate_avg_rating(self, key, values):
    rating_sum = 0
    values_for_key = 0
    for rating in values:
      rating_sum += rating
      values_for_key += 1
    yield None, (key, rating_sum / values_for_key)

  def reducer_find_min_day(self, key, values):
    day_avg_rating_list = []
    for day, avg_rating in values:
      day_avg_rating_list.append((day, avg_rating))
    (min_avg_rating_day, min_avg_rating) = min(day_avg_rating_list, key=lambda x: x[1])
    yield "Day " + min_avg_rating_day, min_avg_rating


if __name__ == '__main__':
  MRWorstAvgRatingDay.run()
