
import datetime

import pandas as pd
from PyQt5.QtCore import QObject, pyqtSignal
#from WindPy import *
from sqlalchemy import text

#import matplotlib.pyplot as plt
#import matplotlib.ticker as ticker
import database
import basic
import index
import zmain
import recognition
import stockclass
from talib import EMA
import zfutu
from futu import *
import main
import strategy
import backtest
from datetime import date, timedelta

################################################################################################
# 程序开始的地方，后面需要加入__main__的判断
################################################################################################

if __name__ == '__main__':
    #zmain.BackTest()
    #zmain.ZtradeHK()

    #zmain.ZtradeUS()
    #zmain.AindexAnalyze()
    zmain.USIndex()

    pass



def ZtradeHK():
    # f= open(, encoding="utf-8")
    TradeCalendar='HKEX'
    codeDF = pd.read_csv("D:\ztrade\codes.csv")
    # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
    codeList = codeDF['WindCodes'].tolist()
    startDate = date(2020, 1, 2)
    endDate = date(2024,3, 15)
    #endDate=date.today()

    #codeList=['3690.HK']

    pass
    #使用数据库初始化
    dtp = database.DataPrepare(database.con,database.engine,database.session,TradeCalendar)
    # 准备好待处理的stock类
    stocks = dtp.DataPreWindDB(codeList, startDate, endDate)
    # 识别的类
    #recog = recognition.Recognition()
    #recog.RecognitionProcess(stocks)
    #zft = zfutu.Zfutu(market='HK')
    #zft.ModifyFutuStockList(recog.resultTable)
    stg=strategy.Strategy()
    stg.SignalProcess(stocks)
    stg.WriteToExcel()
    pass
    '''
    for stock in stocks:

        prob, AccumGain, cashReturn = strategy.BBIStrategy(stock)
        print(f'成功率为：{prob},单次累加收益率为{AccumGain},复利收益率为{cashReturn}')
    pass
    '''
    for stock in stocks:
        resultArray=strategy.MACDTOPArcSignal(stock)
        signalProb=strategy.CalSignalProbability(resultArray,stock,'down')
        print(f'{stock.code}成功率为：{signalProb},')
    pass

def ZtradeUS():
    # f= open(, encoding="utf-8")
    TradeCalendar_US='NYSE'
    codeDF = pd.read_csv("D:\ztrade\heatChartUS.csv",encoding="gb2312")
    # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
    codeList = codeDF['WindCodes'].tolist()
    #print(codeList)
    startDate = date(2023, 1, 2)
    endDate = date(2024, 3,15)
    #endDate=date.today()-timedelta(days=1)

    #codeList=['AAPL.O']


    pass
    dtp_US = database.DataPrepare(database.conUS,database.engineUS,database.sessionUS,TradeCalendar_US)
    # 准备好待处理的stock类
    stocks_US = dtp_US.DataPreWindDB(codeList, startDate, endDate)
    #使用stock列表进行beta分析
    #result= index.BetaAnalyze(startDate,endDate,stocks_US)

    stg = strategy.Strategy()
    stg.SignalProcess(stocks_US)
    stg.WriteToExcel()
    pass
    # 识别的类
    for stock in stocks_US:
        resultArray = strategy.KDJBottomArcSignal(stock)
        signalProb = strategy.CalSignalProbability(resultArray, stock)
        print(f'{stock.code}成功率为：{signalProb},')


    pass

    recog_US = recognition.Recognition()
    recog_US.RecognitionProcess(stocks_US)




    zft_US = zfutu.Zfutu(market='US')
    zft_US.ModifyFutuStockList(recog_US.resultTable)

    #针对美股加入相关性与beta的分析
    pass

def AindexAnalyze():
    #这里要注意RSI是少一部分的
    startDate = date(2020, 1, 1)
    enddate = date(2024, 3, 3)
    #enddate=date.today()
    pass
    aIndex=index.Aindex(database.con, database.engine, database.session)
    indexList=aIndex.DataPreWindDB(startDate,enddate)
    for indexs in indexList:
        aIndex.ProbabilityProc(indexs)
        #resultArry=aIndex.EMA5BottomArcSignal(indexs)
        #prob=aIndex.CalSignalProbability(resultArry,indexs)
        #print(f'信号成功率为{prob}')
        prob,AccumGain,cashReturn=aIndex.BBIStrategy(indexs)
        print(f'成功率为：{prob},单次累加收益率为{AccumGain},复利收益率为{cashReturn}')

    pass

def BackTest():
    TradeCalendar = 'HKEX'
    codeDF = pd.read_csv("D:\ztrade\codes.csv")
    # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
    codeList = codeDF['WindCodes'].tolist()
    startDate = date(2023, 8, 12)
    endDate = date(2024, 3, 4)
    # endDate=date.today()

    codeList = ['0700.HK']

    pass
    # 使用数据库初始化
    dtp = database.DataPrepare(database.con, database.engine, database.session, TradeCalendar)
    # 准备好待处理的stock类
    stocks = dtp.DataPreWindDB(codeList, startDate, endDate)
    bbi=stocks[0].BBIData
    bs = backtest.BackTest()
    #startDate = date(2022, 3, 11)
    #endDate = date(2024, 3, 4)
    result = bs.GetMinuteData(code='HK.00700', start=startDate, end=endDate)
    #ema=bs.CalMinuteEMA(result)
    #ma=bs.CalMinuteMA(result)
    prob, AccumGain, cashReturn,tradeTimes =backtest.BBIStrategyMinute(result,bbi)
    print(f'成功率为：{prob},单次累加收益率为{AccumGain},复利收益率为{cashReturn},交易笔数为{tradeTimes}')

    pass

def USIndex():
    TradeCalendar_US = 'NYSE'

    indexCodesUS = ['^NDX', '^DJI', '^SPX']
    startDate = date(2024, 2, 3)
    # endDate = date(2024, 3,15)
    #endDate = date.today() - timedelta(days=1)
    endDate = date.today()

    gwdIndex = database.GetWindDaTA(database.conUS, database.engineUS, database.sessionUS, TradeCalendar_US)
    gwdIndex.SyncDataBaseDayPirceDataYFinance(indexCodesUS, startDate, endDate,'daypricedata')