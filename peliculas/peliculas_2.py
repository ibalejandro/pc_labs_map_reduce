from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMaxMoviesDay(MRJob):

  def steps(self):
    # 2 steps: count movies and find maximum movies number day.
    return [
      MRStep(mapper=self.mapper,
             reducer=self.reducer_count_movies),
      MRStep(reducer=self.reducer_find_max_day)
    ]

  def mapper(self, _, line):
    splitted_line = line.split()
    yield splitted_line[4], 1

  def reducer_count_movies(self, key, values):
    yield None, (key, sum(values))

  def reducer_find_max_day(self, key, values):
    day_movies_numb_list = []
    for day, movies_numb in values:
      day_movies_numb_list.append((day, movies_numb))
    (max_movie_numb_day, max_movie_numb) = max(day_movies_numb_list, key=lambda x: x[1])
    yield "Day " + max_movie_numb_day, max_movie_numb


if __name__ == '__main__':
  MRMaxMoviesDay.run()
