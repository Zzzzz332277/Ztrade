
import datetime

import pandas as pd
from WindPy import *
from futu import RET_OK

#import matplotlib.pyplot as plt
#import matplotlib.ticker as ticker
import database
import basic
import recognition
import stockclass
from talib import EMA
import futu as ft
################################################################################################
# 程序开始的地方，后面需要加入__main__的判断
################################################################################################

if __name__ == '__main__':
    '''
    #################################与富途api进行连接########################################
    # 实例化行情上下文对象
    quote_ctx = ft.OpenQuoteContext(host="127.0.0.1", port=11111)
    # 上下文控制
    quote_ctx.start()  # 开启异步数据接收
    quote_ctx.set_handler(ft.TickerHandlerBase())  # 设置用于异步处理数据的回调对象(可派生支持自定义)

    ret, data = quote_ctx.get_user_security("每日关注")
    if ret == RET_OK:
        print(data)
        if data.shape[0] > 0:  # 如果自选股列表不为空
            print(data['code'][0])  # 取第一条的股票代码
            print(data['code'].values.tolist())  # 转为 list
    els e:
        print('error:', data)
    quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽
    #######################################################################################
    ###'''
    gwd= database.GetWindDaTA()

    #Base.metadata.create_all(engine)
    codelist = ['0700.HK','3690.HK']
    startDate = date(2022,9,10)
    endDate = date(2022,9,30)
    gwd.SyncDateBase(codelist,startDate,endDate,'codedateindex')
    #gwd.UpdateTimePeriodData(codelist,startDate,endDate,'daypricedata')
    #gwd.UpdateTimePeriodDataEMA(codelist,startDate,endDate,'ExpMA')

    #################################测试将数据存进去数据库###########################
    complexData = gwd.GetTimePeriodData(codelist,startDate,endDate)
    complexDataEMA =gwd.GetTimePeriodDataEMA(codelist,startDate,endDate)
    #声明一个stock类的数组
    stocklist = [stockclass.StockClass for i in range(len(codelist))]
    stocklistIndex = 0
    for code in codelist:
        buffdata =  pd.DataFrame() #重新声明，避免浅拷贝
        buffEMA = pd.DataFrame()
        buffdataEMA = pd.DataFrame()
        buffdata = complexData[complexData['CODE']==code]

        periods =  gwd.emaPeriod #获取到设定好的ema值
        for period in periods:
            buffdataEMA = complexDataEMA[(complexDataEMA['CODE'] == code) & (complexDataEMA['PERIOD'] == period)]
            #对EMA数据的格式进行操作
            buffEMA['DATE'] = buffdataEMA['DATE']
            buffEMA[f'EMA{period}'] = buffdataEMA['EXPMA']

        #这里对buffdata要进行排序
        pass
        stockClassBuff = stockclass.StockClass(code,buffdata,buffEMA)
        stocklist[stocklistIndex] = stockClassBuff
        stocklistIndex = stocklistIndex+1
    pass

    #识别的类
    recog=recognition.Recognition()
    recog.RecognitionProcess(stocklist)
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
