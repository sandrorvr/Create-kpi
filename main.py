from pyspark.sql import SparkSession

from minhaPhase import MinhaPhase

import pandas as pd


if __name__ == '__main__':
    MinhaPhase(
        'TEST', 
        'append', 
        path='agoraVai', 
        start='02-01-1965', 
        end='01-02-1966'
        ).\
        setConfigSpark(
            {
                'spark': SparkSession.builder.master("local[*]").getOrCreate(),
                'columnDate':'DATETIME'
            }
            ).setMode(mode='load')