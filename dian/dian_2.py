from mrjob.job import MRJob


class MREmployeeAvgSalary(MRJob):

  def mapper(self, _, line):
    splitted_line = line.split()
    yield splitted_line[1], int(splitted_line[2])

  def reducer(self, key, values):
    salary_sum = 0
    values_for_key = 0
    for salary in values:
      salary_sum += salary
      values_for_key += 1
    yield "Employee " + key, salary_sum / values_for_key


if __name__ == '__main__':
  MREmployeeAvgSalary.run()
