
import datetime

import pandas as pd
from WindPy import *
from sqlalchemy import text

#import matplotlib.pyplot as plt
#import matplotlib.ticker as ticker
import database
import basic
import recognition
import stockclass
from talib import EMA
import zfutu
################################################################################################
# 程序开始的地方，后面需要加入__main__的判断
################################################################################################

if __name__ == '__main__':

    gwd= database.GetWindDaTA()
    #f= open(, encoding="utf-8")

    codeDF=pd.read_csv("D:\ztrade\codes.csv")
    #codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
    codeList=codeDF['WindCodes'].tolist()

    startDate = date(2023,8,1)
    endDate = date(2023,10,20)

    #codeList=['1211.HK']

    pass
    dtp=database.DataPrepare()
    #准备好待处理的stock类
    stocks=dtp.DataPreWindDB(codeList,startDate,endDate)
    #识别的类
    recog=recognition.Recognition()
    recog.RecognitionProcess(stocks)
    zft=zfutu.Zfutu()
    zft.ModifyFutuStockList(recog.resultTable,'ztrade')
    pass



