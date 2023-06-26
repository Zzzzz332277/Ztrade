import talib
import pandas as pd
import numpy as np

# 适用于wind数据的candle
class StockClass:
    code = ''
    dayPriceData = pd.DataFrame()
    EMAData = pd.DataFrame()

    #def __init__(self, code,dayPriceDataFrame,emaDataFrame):  # 从数据库中取出的是一个dataframe
    def __init__(self, code, dayPriceDataFrame):  # 从数据库中取出的是一个dataframe

        self.code = code
        # 直接在这里完成stock的转置和处理
        '''
        data = pd.DataFrame(stockdata.Data, index=stockdata.Fields)
        data = data.T  # 转置
        data['time'] = stockdata.Times

        self.StockDataFrame = data
        '''
        self.dayPriceData = dayPriceDataFrame
        #EMAData = emaDataFrame
        #############################获取时间########################
        startdate = self.dayPriceData['Date'].iloc[0]
        enddate = self.dayPriceData['Date'].iloc[-1]

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


class Trend:
    def __init__(self, start, end, len, direction):
        self.StartTime = start
        self.EndTime = end
        self.Length = len
        self.Direction = direction
