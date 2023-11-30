
import datetime

import pandas as pd
from WindPy import *
from sqlalchemy import text

#import matplotlib.pyplot as plt
#import matplotlib.ticker as ticker
import database
import basic
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
    '''
    startDate = date(2023, 8, 1)
    endDate = date(2023, 11,17)
    TradeCalendar = 'HKEX'
    sql = f'select * from daypricedata where CODE = "0700.HK" AND DATE between "2023-8-1" and "2023-11-1"'
    outData = pd.DataFrame()
    outData = pd.read_sql(text(sql), con=database.con)
    outDataClose = outData['CLOSE']
    #tic = recognition.TechIndex(database.con,database.engine,database.session,TradeCalendar)
    #tic.CalAllRSI(['0700.HK'])
    gwd = database.GetWindDaTA(database.con,database.engine,database.session,TradeCalendar)
    rsidata=gwd.SyncDataBaseMACD(['0700.HK'],startDate,endDate)
    pass
    '''



    #main.ZtradeHK()

    main.ZtradeUS()



    #TradeCalendar='HKEX'

    #gwd=database.GetWindDaTA(database.con,database.engine,database.session,TradeCalendar)
    #gwd.UpdateTimePeriodCapitalSingle('0700.HK',startDate,endDate,'capitalflow')

    pass

def ZtradeHK():
    # f= open(, encoding="utf-8")
    TradeCalendar='HKEX'
    codeDF = pd.read_csv("D:\ztrade\codes.csv")
    # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
    codeList = codeDF['WindCodes'].tolist()
    startDate = date(2023, 8, 1)
    endDate = date(2023, 11, 30)

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
    endDate = date(2023, 11,29)

    #codeList=['AAPL.O']

    pass
    dtp_US = database.DataPrepare(database.conUS,database.engineUS,database.sessionUS,TradeCalendar_US)
    # 准备好待处理的stock类
    stocks_US = dtp_US.DataPreWindDB(codeList, startDate, endDate)
    # 识别的类
    recog_US = recognition.Recognition()
    recog_US.RecognitionProcess(stocks_US)
    zft_US = zfutu.Zfutu(market='US')
    zft_US.ModifyFutuStockList(recog_US.resultTable)
    pass