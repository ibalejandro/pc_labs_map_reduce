from mrjob.job import MRJob
from mrjob.step import MRStep
import statistics


class MRBlackDay(MRJob):

  def steps(self):
    # 2 steps: calculate medians and find the minimum one.
    return [
      MRStep(mapper=self.mapper,
             reducer=self.reducer_calculate_medians),
      MRStep(reducer=self.reducer_find_min_median)
    ]

  def mapper(self, _, line):
    splitted_line = line.split()
    yield splitted_line[2], float(splitted_line[1])

  def reducer_calculate_medians(self, key, values):
    values_list = []
    for value in values:
      values_list.append(value)
    yield None, (key, statistics.median(values_list))

  def reducer_find_min_median(self, key, values):
    day_medians_list = []
    for day, median in values:
      day_medians_list.append((day, median))
    sorted(day_medians_list, key=lambda x: x[1])
    yield "Day " + day_medians_list[0][0], day_medians_list[0][1]


if __name__ == '__main__':
  MRBlackDay.run()
