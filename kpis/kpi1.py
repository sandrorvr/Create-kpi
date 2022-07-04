from lib.kpi import KPI
import pyspark.sql.functions as f

class Kpi1(KPI):
  def __init__(self, df, titleKPI, columnsId=None):
    super().__init__(df, titleKPI, columnsId)
  
  def configKPI(self):
    self._df = self._df.toDF(*[c.replace('*','') for c in self._df.columns])
    self._df = self._df.withColumn('CPF', 
                                   f.regexp_replace('CPF','\*', '')
                                   ).withColumn('CPF', 
                                                f.regexp_replace('CPF','\.', '')
                                                ).withColumn('CPF', 
                                                             f.regexp_replace('CPF','-', '')
                                                             )
    self._df = self._df.withColumn(self.titleKPI, 
                                   f.when(
                                       f.substring('CPF', 1,1) == '8', 
                                       self.labels[0]
                                       ).otherwise(self.labels[1])
                                       )
