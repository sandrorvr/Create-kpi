from abc import ABC, abstractclassmethod

class SaveDF:
  def __init__(self, df, mode, path, db='test', collection='test'):
    self._df = df
    self.mode = mode
    self.path = path
    self.db = db
    self.collection = collection
  
  def setFormat(self, formatType):
    if formatType == 'mongodb':
      return SaveMongDb(self._df, self.mode, self.path, self.db, self.collection)
    
    elif formatType == 'parquet':
      return SaveParquet(self._df, self.mode, self.path)
    
    elif formatType == 'csv':
      return SaveCsv(self._df, self.mode, self.path)
    
    elif formatType == 'json':
      return SaveJson(self._df, self.mode, self.path)
    
    else:
      raise ValueError('File Type not find')

class SaveAbstract(ABC):
  def __init__(self, df, mode, path, db=None, collection=None):
    self._df = df
    self.mode = mode
    self.path = path
    self.db = db
    self.collection = collection
  
  @abstractclassmethod
  def save(self):
    pass

class SaveMongDb(SaveAbstract):
  def __init__(self, df, mode, path, db, collection):
    super().__init__(df, mode, path, db, collection)
  def save(self, columns = None):
    columns = self._df.columns if columns == None else columns
    self._df.select(columns)\
      .write.format("mongodb")\
      .mode(self.mode)\
      .option("database", self.db)\
      .option("collection", self.collection)\
      .save()


class SaveParquet(SaveAbstract):
  def __init__(self, df, mode, path):
    super().__init__(df, mode, path)
  def save(self, columns = None):
    columns = self._df.columns if columns == None else columns
    self._df.select(columns)\
      .write.mode(self.mode)\
      .parquet(self.path)


class SaveCsv(SaveAbstract):
  def __init__(self, df, mode, path):
    super().__init__(df, mode, path)
  def save(self, columns = None):
    columns = self._df.columns if columns == None else columns
    self._df.select(columns)\
      .write.mode(self.mode)\
      .csv(self.path)

class SaveJson(SaveAbstract):
  def __init__(self, df, mode, path):
    super().__init__(df, mode, path)
  def save(self, columns = None):
    columns = self._df.columns if columns == None else columns
    self._df.select(columns)\
      .write.mode(self.mode)\
      .json(self.path)

    