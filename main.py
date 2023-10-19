
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
    #codeList = ['0700.HK', '3690.HK']
    #codelist =['3690.HK']



    startDate = date(2023,8,1)
    endDate = date(2023,10,19)
    #gwd.UpdateTimePeriodDataKDJ(codeList, startDate, endDate, 'kdj')
    #tic=recognition.TechIndex()
    #tic.CalcKDJ(codeList,database.session,database.con)

    #codeList=['0003.HK']
    #ti=recognition.TechIndex()
    #ti.CalAllEMA(testCodelist, database.session,database.con)


    pass
    dtp=database.DataPrepare()
    #准备好待处理的stock类
    stocks=dtp.DataPreWindDB(codeList,startDate,endDate)
    #识别的类
    recog=recognition.Recognition()
    recog.RecognitionProcess(stocks)
    zft=zfutu.Zfutu()
    #recog.resultTable['EmaDiffusion']=1
    zft.ModifyFutuStockList(recog.resultTable,'ztrade')
    pass


'''
#计算ema均线并绘制输出
EMA=pd.DataFrame()
EMA['ema10']=talib.SMA(data['CLOSE'],timeperiod=10)
EMA['ema20']=talib.SMA(data['CLOSE'],timeperiod=20)
EMA['ema30']=talib.SMA(data['CLOSE'],timeperiod=30)
#测试锤子形态：
#结果以非零形式存储
#shape_result=talib.CDLHAMMER(data['OPEN'].values,data['HIGH'].values,data['LOW'].values,data['CLOSE'].values)
#pos=np.nonzero(shape_result)
#for i in pos:
#    print(data['TIMES'][i])

daoshu_ema=RecognizeTrend(EMA['ema30'].values,data['TIMES'].values)
#绘制均线
#ax.plot(np.arange(0, len(MA10)), MA10)  # 绘制5日均线


#ax.xaxis.set_major_locator(ticker.MaxNLocator(20))

#ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
#plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
#plt.show()
#输出图像
fig = plt.figure(figsize=(12, 8))
plt.plot(daoshu_ema)
plt.show()

'''
