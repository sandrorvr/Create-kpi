from abc import ABC, abstractclassmethod

class BoxTest:
  def __init__(self, boxTest = []):
    self.boxTest = boxTest
    self.logTest = {}
  
  def insertTest(self, test):
    self.boxTest.append(test)
    return self
  
  def getLogTest(self):
    return self.logTest
  
  def saveLogCsv(self):
    arq = open('getLogTest.csv', 'w')
    for nameTest, statusTest in self.logTest.items():
      arq.write(f'{nameTest}, {statusTest.status}, {statusTest.lineError}')
      arq.write('\n')
      arq.close()

  def runTests(self):
    for tst in self.boxTest:
      nameTest, statusTest = tst.check()
      self.logTest[nameTest] = statusTest
    return self


class StatusTest:
  def __init__(self, erros):
    self.status = None
    self.lineError = erros
    self.start()
  
  def __str__(self):
    return self.status
  
  def start(self):
    if len(self.lineError) == 0:
      self.status = 'KPI_OK'
    else:
      self.status = 'KPI_NOt_OK'



