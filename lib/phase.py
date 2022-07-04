from tests import BoxTest
from stepsLoad import StepsLoad
from abc import ABC, abstractclassmethod

class Phase(ABC):
  def __init__(self, phaseTitle, mode, formatType='parquet', start=None, end=None, path=None, db='test', collection='test', tx=85823, step=1555200, sep='-'):
    self._df = None
    self.config = None
    self.phaseTitle = phaseTitle
    self.start = start
    self.end = end
    self.mode = mode
    self.path = path
    self.formatType = formatType
    self.db = db
    self.collection = collection
    self.tx = tx,
    self.step = step
    self.sep = sep
    self.tests = []
  
  def setMode(self, mode):
    if mode == 'load':
      self._df = self.getDataFrame()
      StepsLoad(
        self._df,
        self.config['columnDate'], 
        self.start, 
        self.end, 
        self.mode, 
        self.path, 
        self.formatType, 
        self.db, 
        self.collection,
        self.tx,
        self.step,
        self.sep).loop(self.getDataFrame, self.preprocess, self.runKpis)
    elif mode == 'test':
      return BoxTest(self.tests).runTests().getLogTest()
    else:
      raise ValueError('usage mode not found')

    
  def getConfigSpark(self):
    return self.config
  
  def setConfigSpark(self, configDict):
    if type(configDict) == dict:
      if 'spark' not in configDict.keys(): raise ValueError('Does not have spark field')
      self.config = configDict
      return self
    else:
      print(type(configDict))
      raise TypeError('Type Error')
  
  def transformInDataFramePandas(self, df):
    return self.config['spark'].createDataFrame(df)

  @abstractclassmethod
  def getDataFrame(self):
    pass

  @abstractclassmethod
  def preprocess(self, df):
    pass

  @abstractclassmethod
  def runKpis(self, df):
    pass

