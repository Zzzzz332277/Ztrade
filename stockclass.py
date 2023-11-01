import datetime

import talib
import pandas as pd
import numpy as np

#趋势类
class Trend:
    def __init__(self, start, end, len, direction):
        self.StartTime = start
        self.EndTime = end
        self.Length = len
        self.Direction = direction

# 适用于wind数据的candle
class StockClass:
    code = ''
    dayPriceData = pd.DataFrame()
    EMAData = pd.DataFrame()
    trendList= []
    startDate = datetime.date(1970,1,1) #这里默认给一个起始时间和结束时间
    endDate = datetime.date(1970,1,2)
    totalCashFlowIn=0
    market=''
    #def __init__(self, code,dayPriceDataFrame,emaDataFrame):  # 从数据库中取出的是一个dataframe
    #不接受输入的初始化函数
    def __init__(self):
        pass

    def __init__(self, code, dayPriceDataFrame,emaData,market):  # 从数据库中取出的是一个dataframe
        self.code = code
        self.trendList = [Trend]
        self.EMAData1 = emaData
        # 直接在这里完成stock的转置和处理
        self.dayPriceData = dayPriceDataFrame
        self.EMAData = emaData
        #############################获取时间########################
        self.startDate = self.dayPriceData['DATE'].iloc[0]
        self.endDate = self.dayPriceData['DATE'].iloc[-1]
        self.market=market
        '''
        暂时先不用ema跑流程
        # 更新EMA数据
        self.EMA['ema5'] = talib.EMA(self.StockDataFrame['CLOSE'], timeperiod=5)
        self.EMA['ema10'] = talib.EMA(self.StockDataFrame['CLOSE'], timeperiod=10)
        self.EMA['ema20'] = talib.EMA(self.StockDataFrame['CLOSE'], timeperiod=20)
        self.EMA['ema30'] = talib.EMA(self.StockDataFrame['CLOSE'], timeperiod=30)
        self.EMA['ema60'] = talib.EMA(self.StockDataFrame['CLOSE'], timeperiod=60)
        '''
    '''
    def CalculateEma(self):
        EMA['ema10'] = talib.SMA(self.StockData['CLOSE'], timeperiod=10)
        EMA['ema20'] = talib.SMA(self.StockData['CLOSE'], timeperiod=20)
        EMA['ema30'] = talib.SMA(self.StockData['CLOSE'], timeperiod=30)



    '''


