from mrjob.job import MRJob


class MRStableShares(MRJob):

  def mapper(self, _, line):
    splitted_line = line.split()
    yield splitted_line[0], (splitted_line[2], float(splitted_line[1]))

  def reducer(self, key, values):
    days_values_list = []
    for day, value in values:
      days_values_list.append((day, value))
    sorted_days_values_list_by_day = sorted(days_values_list, key=lambda x: x[0])
    if sorted_days_values_list_by_day == sorted(days_values_list, key=lambda x: x[1]):
      yield "Accion " + key, sorted_days_values_list_by_day


if __name__ == '__main__':
  MRStableShares.run()
