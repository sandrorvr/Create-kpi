from abc import ABC, abstractclassmethod
from pyspark.sql.functions import concat_ws, col

class KPI(ABC):
    def __init__(self, df, titleKPI, columnsId=None, columnsUsed=None):
        self._df = df
        self.columnsUsed = columnsUsed
        self.titleKPI = titleKPI
        self.columnsId = columnsId
        self.labels = []
    
    def __str__(self):
      return 'CLASS_KPI'
    
    def getDF(self):
      return self._df
    
    @abstractclassmethod
    def configKPI(self):
        pass

    def setLabels(self, lb):
        lb = lb.lower().strip().replace(' ', '_')
        self.labels.append(lb)
        return self
    
    def getLabels(self):
        return self.labels

    def setColumnsId(self):
        if type(self.columnsId) == list:
            self._df = self._df.withColumn(\
                                           '_'.join(self.columnsId),
                                           concat_ws('_', *[c for c in self.columnsId])
                                           )
            self.columnsId = '_'.join(self.columnsId)

        elif type(self.columnsId) != str:
            raise TypeError('informa uma string ou uma lista de string!')
        
        return self

    def save(self, mode, bd, collection):
      if self.columnsId != None:
        columns = [self.columnsId ,self.titleKPI]
      else:
        columns = self._df.columns
        
      self._df.select(columns)\
      .write.format("mongodb")\
      .mode(mode)\
      .option("database", bd)\
      .option("collection", collection)\
      .save()
      

    def run(self):
        self.configKPI()
        if self.columnsId != None:
            self.setColumnsId()
        return self 
