
import datetime

import pandas as pd
from WindPy import *
from sqlalchemy import text

#import matplotlib.pyplot as plt
#import matplotlib.ticker as ticker
import database
import basic
import index
import main
import recognition
import stockclass
from talib import EMA
import zfutu
from futu import *
################################################################################################
# 程序开始的地方，后面需要加入__main__的判断
################################################################################################

if __name__ == '__main__':





    #main.ZtradeHK()

    main.ZtradeUS()
    #main.AindexAnalyze()

    pass

def ZtradeHK():
    # f= open(, encoding="utf-8")
    TradeCalendar='HKEX'
    codeDF = pd.read_csv("D:\ztrade\codes.csv")
    # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
    codeList = codeDF['WindCodes'].tolist()
    startDate = date(2023, 8, 1)
    #endDate = date(2024,1, 2)
    endDate=date.today()

    #codeList=['0001.HK']

    pass
    #使用数据库初始化
    dtp = database.DataPrepare(database.con,database.engine,database.session,TradeCalendar)
    # 准备好待处理的stock类
    stocks = dtp.DataPreWindDB(codeList, startDate, endDate)
    # 识别的类
    recog = recognition.Recognition()
    recog.RecognitionProcess(stocks)
    zft = zfutu.Zfutu(market='HK')
    zft.ModifyFutuStockList(recog.resultTable)
    pass

def ZtradeUS():
    # f= open(, encoding="utf-8")
    TradeCalendar_US='NYSE'
    codeDF = pd.read_csv("D:\ztrade\heatChartUS.csv",encoding="gb2312")
    # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
    codeList = codeDF['WindCodes'].tolist()

    startDate = date(2023, 8, 1)
    #endDate = date(2023, 12,29)
    endDate=date.today()-timedelta(days=1)

    #codeList=['AES.N']

    pass
    dtp_US = database.DataPrepare(database.conUS,database.engineUS,database.sessionUS,TradeCalendar_US)
    # 准备好待处理的stock类
    stocks_US = dtp_US.DataPreWindDB(codeList, startDate, endDate)
    #使用stock列表进行beta分析
    #result= index.BetaAnalyze(startDate,endDate,stocks_US)
    pass
    # 识别的类
    recog_US = recognition.Recognition()
    recog_US.RecognitionProcess(stocks_US)
    zft_US = zfutu.Zfutu(market='US')
    zft_US.ModifyFutuStockList(recog_US.resultTable)

    #针对美股加入相关性与beta的分析
    pass

def AindexAnalyze():
    startDate = date(2023, 8, 1)
    #endDate = date(2023, 12, 19)
    enddate=date.today()
    pass
    aIndex=index.Aindex(database.con, database.engine, database.session)
    indexList=aIndex.DataPreWindDB(startDate,enddate)
    for indexs in indexList:
        aIndex.ProbabilityProc(indexs)

    pass