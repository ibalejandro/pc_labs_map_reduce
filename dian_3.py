from mrjob.job import MRJob


class MREmployeeSE(MRJob):

  def mapper(self, _, line):
    splitted_line = line.split()
    yield splitted_line[1], splitted_line[0]

  def reducer(self, key, values):
    se_set = set()
    for se in values:
      se_set.add(se)
    yield "Employee " + key, len(se_set)


if __name__ == '__main__':
  MREmployeeSE.run()
