from mrjob.job import MRJob


class MRDaysMinMaxValues(MRJob):

  def mapper(self, _, line):
    splitted_line = line.split()
    yield splitted_line[0], (float(splitted_line[1]), splitted_line[2])

  def reducer(self, key, values):
    values_days_list = []
    for value, day in values:
      values_days_list.append((value, day))
    (min_value, min_value_day) = min(values_days_list, key=lambda x: x[0])
    (max_value, max_value_day) = max(values_days_list, key=lambda x: x[0])
    yield "Accion " + key, (min_value_day, max_value_day)


if __name__ == '__main__':
  MRDaysMinMaxValues.run()
