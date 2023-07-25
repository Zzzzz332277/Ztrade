
import datetime

import pandas as pd
from WindPy import *
#import matplotlib.pyplot as plt
#import matplotlib.ticker as ticker
import database
import basic
import recognition
import stockclass
from talib import EMA

################################################################################################
# 程序开始的地方，后面需要加入__main__的判断
################################################################################################

if __name__ == '__main__':
    # 运行wind
    #w.start()
    # gwd=GetWindDaTA()
    # DATA=gwd.UpdateData()
    # pass
    gwd= database.GetWindDaTA()
    #Base.metadata.create_all(engine)

    # ax = fig.add_subplot(111)
    # 获取data
    # 遍历获取
    #codelist = ['1024.HK', '3690.HK', '0700.HK', '0001.HK']
    codelist = ['1024.HK']

    startDate = date(2023,5,15)
    endDate = date(2023,7,15)

    complexData = gwd.GetTimePeriodData(codelist,startDate,endDate)
    complexDataEMA =gwd.GetTimePeriodDataEMA(codelist,startDate,endDate)
    #声明一个stock类的数组
    stocklist = [stockclass.StockClass for i in range(len(codelist))]
    stocklistIndex = 0
    buffEMA = pd.DataFrame()
    for code in codelist:
        buffdata = complexData[complexData['CODE']==code]

        periods =  gwd.emaPeriod #获取到设定好的ema值
        for period in periods:
            buffdataEMA = complexDataEMA[(complexDataEMA['CODE'] == code) & (complexDataEMA['PERIOD'] == period)]
            #对EMA数据的格式进行操作
            buffEMA['DATE'] = buffdataEMA['DATE']
            buffEMA[f'EMA{period}'] = buffdataEMA['EXPMA']


        #这里对buffdata要进行排序
        pass
        stocklist[stocklistIndex] = stockclass.StockClass(code,buffdata,buffEMA)
        stocklistIndex = stocklistIndex+1
    pass

    #识别的类
    recog=recognition.Recognition()
    recog.RecognitionProcess(stocklist)
    pass
   # stockdata = w.wsd("0700.HK","open,low,close,volume,amt,pct_chg,turn,mfd_buyamt_a,mfd_sellamt_a,mfd_buyvol_a,mfd_sellvol_a,mfd_netbuyamt,mfd_netbuyvol,mfd_buyamt_d,mfd_sellamt_d,mfn_sn_inflowdays,mfn_sn_outflowdays,mfd_sn_buyamt,mfd_sn_sellamt","2015-01-01", "2023-04-26", "unit=1;traderType=1;TradingCalendar=HKEX;PriceAdj=F")
    # 这一步将数据格式进行转置
    # data=pd.DataFrame(stockdata.Data,index=stockdata.Fields)
    # 引入时间
    #dpr=database.DataPreparation()
    #outdate=dpr.GetDataBase(codelist,startDate,endDate)

    # data=data.T #转置
    # data['TIMES']=stockdata.Times

    #stock = StockClass(stockdata)
    #recog = Recognition()
    #dataset = recog.RecognizeTrend(stock)
    #pass

# 将data转存，后续也要进行数据库处理

# data.to_csv('data.csv')

##########################################################################数据库处理


'''
engine=sqlalchemy.create_engine("mysql+pymysql://root:xinxikemysql@localhost:3306/testz")
con = engine.connect()
data.to_sql(name="testz_table",con=con,if_exists="append")
con.close()
'''
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
