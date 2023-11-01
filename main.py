
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
################################################################################################
# 程序开始的地方，后面需要加入__main__的判断
################################################################################################

if __name__ == '__main__':
    main.ZtradeHK()
    #main.ZtradeUS()



def ZtradeHK():
    # f= open(, encoding="utf-8")
    TradeCalendar='HKEX'
    codeDF = pd.read_csv("D:\ztrade\codes.csv")
    # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
    codeList = codeDF['WindCodes'].tolist()

    startDate = date(2023, 8, 1)
    endDate = date(2023, 10, 31)

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
    TradeCalendar='NYSE'
    codeDF = pd.read_csv("D:\ztrade\codesUS.csv")
    # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
    codeList = codeDF['WindCodes'].tolist()

    startDate = date(2023, 8, 1)
    endDate = date(2023, 10, 30)

    #codeList=['AAPL.O']

    pass
    dtp = database.DataPrepare(database.conUS,database.engineUS,database.sessionUS,TradeCalendar)
    # 准备好待处理的stock类
    stocks = dtp.DataPreWindDB(codeList, startDate, endDate)
    # 识别的类
    recog = recognition.Recognition()
    recog.RecognitionProcess(stocks)
    zft = zfutu.Zfutu(market='US')
    zft.ModifyFutuStockList(recog.resultTable)
    pass