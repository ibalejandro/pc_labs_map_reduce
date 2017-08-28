from mrjob.job import MRJob
from mrjob.step import MRStep


class MRBestAndWorstMovieByGenre(MRJob):

  def steps(self):
    # 2 steps: calculate average rating by movie by genre and find the movie for the maximum and minimum one.
    return [
      MRStep(mapper=self.mapper,
             reducer=self.reducer_calculate_movie_avg_rating_by_genre),
      MRStep(reducer=self.reducer_find_max_and_min_movie)
    ]

  def mapper(self, _, line):
    _, movie, genre, rating, _ = line.split()
    yield genre, (movie, int(rating))

  def reducer_calculate_movie_avg_rating_by_genre(self, key, values):
    # Creates a dict to store movies as keys and its list of ratings as values.
    movies_dict = {}
    for movie, rating in values:
      if not movie in movies_dict:
        movies_dict[movie] = [rating]
      else:
        movies_dict[movie].append(rating)
    movies_avg_rating = []
    for movie, ratings_list in movies_dict.items():
      movies_avg_rating.append((movie, (sum(ratings_list) / len(ratings_list))))
    yield key, movies_avg_rating

  def reducer_find_max_and_min_movie(self, key, values):
    movies_avg_rating = []
    for movies_avg_rating_list in values:  # Takes out the list which is inside the values list.
      for movie, avg_rating in movies_avg_rating_list:
        movies_avg_rating.append((movie, avg_rating))
    (max_avg_rating_movie, max_avg_rating) = max(movies_avg_rating, key=lambda x: x[1])
    (min_avg_rating_movie, min_avg_rating) = min(movies_avg_rating, key=lambda x: x[1])
    yield "Genre: " + key, "Best movie: " + max_avg_rating_movie + ", Worst movie: " + min_avg_rating_movie


if __name__ == '__main__':
  MRBestAndWorstMovieByGenre.run()
