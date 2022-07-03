from saveDF import SaveDF
import pyspark.sql.functions as f
from datetime import datetime, timedelta

class StepsLoad:
  def __init__(self,df,columnDate, start, end, mode, path, formatType, db='test', collection='test', tx=85823, step=1555200,sep='-'):
    self._df = df
    self.start = self.maxDateOfColumnMin(columnDate, 'min') if start==None else self.parseDate(start, sep)
    self.end = self.maxDateOfColumnMin(columnDate, 'max') if end==None else self.parseDate(end, sep)
    self.mode = mode
    self.path = path
    self.formatType = formatType
    self.db = db
    self.collection = collection
    self.tx = tx
    self.step = self.OnlyOnce() if start == None else step #18 dias
    self.rodada = 0
    self.intervalo_R = timedelta(seconds=self.incrementNegative(self.rodada, self.step))
    self.X_begin = self.start
    self.X_after = self.X_begin + self.intervalo_R

  def parseDate(self, date, sep):
    if type(date) == datetime:
      return date
    elif type(date) == str:
      return datetime.strptime(date, f"%d{sep}%m{sep}%Y")
    else:
      raise TypeError('The value should be datetime or string')
  
  def OnlyOnce(self):
    return int((self.end - self.start).total_seconds())

  def maxDateOfColumnMin(self, columnDate, tp):
    if tp == 'max':
      return self._df.agg(f.max(columnDate)).collect()[0][0]
    else:
      return self._df.agg(f.min(columnDate)).collect()[0][0]
  
  def incrementNegative(self, rodada, step):#1
    return -rodada*self.tx+ step # -x * (2,9 dias) + 18 dias
  
  def incrementPositive(self, rodada):#2
    return rodada*self.tx+43200 # x * (2,9 dias) + (12 horas)
  
  def loop(self, callGetDataFrame, callpreprocess, callrunKpis):
    while self.X_begin < self.end:
      intervalBefore = f'{self.X_begin.year}-{self.X_begin.month}-{self.X_begin.day} {self.X_begin.hour}:{self.X_begin.minute}:{self.X_begin.second}'
      intervalAfter = f'{self.X_after.year}-{self.X_after.month}-{self.X_after.day} {self.X_after.hour}:{self.X_after.minute}:{self.X_after.second}'
      df = callGetDataFrame()
      df = df.filter((f.col('DATETIME')>= intervalBefore)&(f.col('DATETIME')< intervalAfter))
      try:
        df = callpreprocess(df)
        df = callrunKpis(df)

        SaveDF(df,self.mode, self.path, self.db, self.collection).setFormat(self.formatType).save()
        print('|OK|',intervalBefore, intervalAfter)
        
        self.rodada = 0
        self.X_begin = self.X_after
        self.X_after += timedelta(seconds=self.incrementNegative(self.rodada, self.step))

      except Exception as erro:
        print('\n')
        print(erro.args)
        print(self.db, self.collection)
        print('\n')
        print('|ERRO|',intervalBefore, intervalAfter)
        if self.X_after <=  self.X_begin or self.rodada > 5:
          print('Nao rodu')
          self.X_begin += timedelta(days=1)
          self.X_after = self.X_begin + timedelta(seconds=self.incrementNegative(self.rodada, self.step))
          self.rodada = 0
          continue

        self.rodada += 1
        self.X_after -= timedelta(seconds= self.incrementPositive(self.rodada))
