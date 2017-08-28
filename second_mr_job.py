from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")


# Finds the most commonly used word in the input.
class MRMostUsedWord(MRJob):

  # OUTPUT_PROTOCOL = JSONValueProtocol

  def steps(self):
    # 2 steps: count words and find the most frequent one.
    return [
      MRStep(mapper=self.mapper_get_words,
             combiner=self.combiner_count_words,
             reducer=self.reducer_count_words),
      MRStep(reducer=self.reducer_find_max_word)
    ]

  def mapper_get_words(self, _, line):
    # Yields each word in the line. Matches words using the RE.
    for word in WORD_RE.findall(line):
      yield (word.lower(), 1)

  def combiner_count_words(self, word, counts):
    # Optimization: sum the words we've seen so far.
    yield (word, sum(counts))

  def reducer_count_words(self, word, counts):
    # Sends all (num_occurrences, word) pairs to the same reducer.
    # num_occurrences is so we can easily use Python's max() function.
    yield None, (sum(counts), word)

  # Discard the key; it is just None.
  def reducer_find_max_word(self, _, word_count_pairs):
    # Each item of word_count_pairs is (count, word),
    # so yielding one results in key=counts, value=word.
    yield max(word_count_pairs)


if __name__ == '__main__':
  MRMostUsedWord.run()
