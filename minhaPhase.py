from lib.phase import Phase
from kpis.kpi1 import Kpi1
import pandas as pd

class MinhaPhase(Phase):
  def __init__(self, phaseTitle, mode, formatType='parquet', start=None, end=None, path=None, db='test', collection='test'):
    super().__init__(phaseTitle, mode, formatType, start, end, path, db, collection)
  
  def getDataFrame(self):
    #return self.config['spark'].read.option("multiline","true").json('remuneracao.json')
    df = pd.read_csv('test.csv')
    df['DATETIME'] = pd.to_datetime(df['DATETIME'])
    df = self.transformInDataFramePandas(df)
    return df
  
  def preprocess(self, df):
    print('preprocessamento iniciado')
    return df
  
  def runKpis(self, df):
    result = Kpi1(df,'testkpi1')\
          .setLabels('ok')\
          .setLabels('notOk')\
          .run().getDF()
    
    return result