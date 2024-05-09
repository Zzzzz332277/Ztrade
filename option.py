import datetime

import numpy as np
import pandas as pd
from scipy.stats import norm
import datetime as dt

from futu import *
import time
import zfutu
import yfinance as yf
from sqlalchemy import text

import database
#from yahoo_earnings_calendar import YahooEarningsCalendar
data = yf.download("AMZN",tz='America/New_York', valid=True, start="2017-01-01", end="2017-04-30")
pass
#期权定价公式
def vanilla_option(S, K, T, r, sigma, option='call'):
    """
    S: spot price
    K: strike price
    T: time to maturity
    r: risk-free interest rate
    sigma: standard deviation of price of underlying asset
    """
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    d2 = (np.log(S/K) + (r - 0.5*sigma**2)*T)/(sigma * np.sqrt(T))

    if option == 'call':
        p = (S*norm.cdf(d1, 0.0, 1.0) - K*np.exp(-r*T)*norm.cdf(d2, 0.0, 1.0))
    elif option == 'put':
        p = (K*np.exp(-r*T)*norm.cdf(-d2, 0.0, 1.0) - S*norm.cdf(-d1, 0.0, 1.0))
    else:
        return None
    return p

#获取snapshot
def GetSnapshot(code):
    ret, data = zfutu.quote_ctx.get_market_snapshot(code)
    if ret == RET_OK:
        return data
    else:
        print('error:', data)

#宽跨策略，假设标的股票在财报日会有5-10个百分点的波动
def WideStraddleStrategy(code):
    #获取当天价格
    today=dt.date.today()
    todayStr= today.strftime("%Y-%m-%d")
    startDate = today - timedelta(days=60)
    startDateStr=startDate.strftime("%Y-%m-%d")

    #codeFutu = zfutu.CodeTransWind2FUTU(code)
    codeFutu=code
    ret, data, page_req_key = zfutu.quote_ctx.request_history_kline(codeFutu, start=startDateStr, end=todayStr,
                                                                   max_count=1000, page_req_key=None)
    #过滤掉无成交量的
    filter1 = OptionDataFilter()
    filter1.vol_min = 1
    if ret == RET_OK:
        if data.empty:
            # 抛出无数据的异常
            # raise myexception.ExceptionFutuNoData('Futu: No data.')
            # 未获取到数据也休眠，避免频繁调用接口
            time.sleep(0.5)
            return 0, 0, 0
        #当前价格
        priceS=data['close'].iloc[-1]


    #根据不同的到期日期的期权，计算对应的期权链的盈利表
    ret1, data1 = zfutu.quote_ctx.get_option_expiration_date(code=code)
    if ret1 == RET_OK:
        expiration_date_list = data1['strike_time'].values.tolist()
        futureData = pd.DataFrame(columns=['code','name','lot_size','stock_type','option_type','stock_owner','strike_time','strike_price','suspension','stock_id','index_option_type'])
        #太长的不截取,只保留一定长度的
        if len(expiration_date_list)>10:
            expiration_date_list=expiration_date_list[0:10]

        for date in expiration_date_list:
            #这里对应宽胯，直接取价外的期权
            ret2, data2 = zfutu.quote_ctx.get_option_chain(code=code, start=date, end=date,option_cond_type=OptionCondType.OUTSIDE)
            if ret2 == RET_OK:
                futureData=pd.concat([futureData, data2])
            else:
                print('error:', data2)
            time.sleep(0.5)
    #最后找出最可行的期权对，进行交易

    #宽胯的跨度，先默认为1
    resultTable=pd.DataFrame(columns=expiration_date_list)
    # 获取财报到期日期，这里假定为4.23日
    earningsDate = dt.date(2024, 4, 23)
    # 假定财报当天涨跌幅度10%
    change = 0.2
    spotPrice=(1+change)*priceS
    #必须计算出波动率

    closeArray=data['close']
    #sigma=data.iloc[:, 4].std()*np.sqrt(250)  # 显示第 3 列的标准差
    # 计算对数收益率
    logreturns = np.diff(np.log(closeArray))
    #print(logreturns)

    # 股票波动率：是对价格变动的一种衡量。
    # 年股票波动率：对数收益率的标准差除以对数收益率的平均值，然后再除以252个工作日的倒数的平方根。
    Volatility=np.std(logreturns)* np.sqrt(252)
    print(Volatility)

    for date in expiration_date_list:
        buffDataCall=futureData[(futureData['strike_time']==date) & (futureData['option_type']=='CALL') ]
        buffDataPut=futureData[(futureData['strike_time']==date) & (futureData['option_type']=='PUT') ]

        buffDataCall = buffDataCall.sort_values(by="strike_price", ascending=True)
        buffDataCall.reset_index(inplace=True, drop=True)

        buffDataPut = buffDataPut.sort_values(by="strike_price", ascending=False)
        buffDataPut.reset_index(inplace=True, drop=True)
        wideNum=5
        resultArry=np.zeros(wideNum)
        #计算到期还有多少天
        expirationDate=datetime.strptime(date,"%Y-%m-%d")
        expirationDate=datetime.date(expirationDate)
        tToday = (expirationDate - today).days / 365
        tEarningsDay=(expirationDate-earningsDate).days / 365
        for wide in range(1,wideNum+1):
            call=buffDataCall.iloc[wide-1]
            put=buffDataPut.iloc[wide-1]
            #财报当天的值
            #计算当下的价值
            callValueNow=vanilla_option(S=priceS,K=call['strike_price'],T=tToday,r=0,sigma=Volatility,option='call')
            putValueNow=vanilla_option(S=priceS,K=put['strike_price'],T=tToday,r=0,sigma=Volatility,option='put')

            #根据实际摆盘情况，获取实际的买入价格
            callSnapshot=GetSnapshot(call['code'])
            time.sleep(0.5)
            #判断是否无成交量和报价
            if callSnapshot['volume'].iloc[0]==0:
                continue
            callValueBuy=callSnapshot['ask_price'].iloc[0]

            putSnapshot = GetSnapshot(put['code'])
            time.sleep(0.5)
            #判断是否无成交量和报价
            if putSnapshot['volume'].iloc[0]==0:
                continue
            putValueBuy = putSnapshot['ask_price'].iloc[0]



            callValueEnd=vanilla_option(S=spotPrice,K=call['strike_price'],T=tEarningsDay,r=0,sigma=Volatility,option='call')
            putValueEnd=vanilla_option(S=spotPrice,K=put['strike_price'],T=tEarningsDay,r=0,sigma=Volatility,option='put')
            #同价值法，买入同等金额的
            gain=(callValueEnd/callValueBuy-1 + putValueEnd/putValueBuy -1)/2
            resultArry[wide-1]=gain
        resultTable[date]=resultArry

    return resultTable
    pass
# 获取7月份已经公布的所有财报日历

#平价套利
def EvenPriceArbitrage(code):
    today = dt.date.today()
    todayStr = today.strftime("%Y-%m-%d")

    # 过滤掉无成交量的
    filter1 = OptionDataFilter()
    filter1.vol_min = 1

    # 根据不同的到期日期的期权，计算对应的期权链的盈利表
    ret1, data1 = zfutu.quote_ctx.get_option_expiration_date(code=code)
    if ret1 == RET_OK:
        expiration_date_list = data1['strike_time'].values.tolist()
        futureData = pd.DataFrame(
            columns=['code', 'name', 'lot_size', 'stock_type', 'option_type', 'stock_owner', 'strike_time',
                     'strike_price', 'suspension', 'stock_id', 'index_option_type'])
        # 太长的不截取,只保留一定长度的
        maxlen=10
        if len(expiration_date_list) >maxlen:
            expiration_date_list = expiration_date_list[0:maxlen]

        for date in expiration_date_list:
            # 这里对应宽胯，直接取价外的期权
            ret2, data2 = zfutu.quote_ctx.get_option_chain(code=code, start=date, end=date,
                                                           option_cond_type=OptionCondType.ALL,data_filter=filter1)
            if ret2 == RET_OK:
                futureData = pd.concat([futureData, data2])
            else:
                print('error:', data2)
            time.sleep(0.5)

    #获取最新股价
    stockSnapchot = GetSnapshot(code)
    s = stockSnapchot['last_price'].iloc[0]

    for date in expiration_date_list:
        buffDataCall=futureData[(futureData['strike_time']==date) & (futureData['option_type']=='CALL') ]
        buffDataPut=futureData[(futureData['strike_time']==date) & (futureData['option_type']=='PUT') ]

        buffDataCall = buffDataCall.sort_values(by="strike_price", ascending=True)
        buffDataCall.reset_index(inplace=True, drop=True)

        buffDataPut = buffDataPut.sort_values(by="strike_price", ascending=True)
        buffDataPut.reset_index(inplace=True, drop=True)
        #计算到期还有多少天
        expirationDate=datetime.strptime(date,"%Y-%m-%d")
        expirationDate=datetime.date(expirationDate)
        tToday = (expirationDate - today).days / 365
        #折现比例
        fvRatio=pow(1.025,tToday)
        for i in range(0,buffDataCall.shape[0]):
            call=buffDataCall.iloc[i]
            K=call['strike_price']

            print(f'计算日期{date}，行权价{K}')

            put=buffDataPut[(buffDataPut['strike_price']==K) ]
            if put.empty:
                continue
            put=put.iloc[0]
            #计算K的现值
            fvK=K/fvRatio
            #判断是否无成交量和报价
            #ccode=call['code']
            callSnapshot = GetSnapshot(call['code'])
            time.sleep(0.5)
            # 判断是否无成交量和报价
            if callSnapshot['volume'].iloc[0] == 0:
                continue
            #pcode=put['code']
            putSnapshot = GetSnapshot(put['code'])
            time.sleep(0.5)
            # 判断是否无成交量和报价
            if putSnapshot['volume'].iloc[0] == 0:
                continue

            #多左空右,买左卖右
            c=callSnapshot['ask_price'].iloc[0]
            p=putSnapshot['bid_price'].iloc[0]
            CombineResult1=c+fvK-s-p
            if CombineResult1<0:
                print(CombineResult1)
                print('多左空右,买左卖右')
            #空左多右,卖左买右
            c = callSnapshot['bid_price'].iloc[0]
            p = putSnapshot['ask_price'].iloc[0]
            CombineResult2=c+fvK-s-p
            if CombineResult2 > 0:
                print(CombineResult2)
                print('空左多右,卖左买右')

#程序思路：根据财报列表，获取过往的财报日，然后计算平均涨跌幅，yfiance的财报财报日中有盘前盘后信息
def CalEarningsVol(code):
    data = yf.Ticker(code)
    earningsDate=data.earnings_dates
    earningsDateHistory=earningsDate.dropna()
    timeStamps=earningsDateHistory.index.tolist()
    dateList=list()
    timeList=list()
    for timeStamp in timeStamps:
        dt=timeStamp.to_pydatetime()
        buffDate=dt.date()
        buffTime=dt.time()
        buffDateTime=datetime(2020,3,1,12,0)
        compareTime=buffDateTime.time()
        #根据时间判断是盘前盘后，这里也要考虑没有给出时间的情况
        if buffTime>compareTime:
            beforeAfter='after'
        else:
            beforeAfter = 'before'
        dateList.append(buffDate)
        timeList.append(beforeAfter)
        #timeStamp=datetime.strptime(dt,"%Y-%m-%d %H:%M:%S")
    #print(earningsDateHistory['Earnings Date'])
    EarningsDateHistoryNew=pd.DataFrame(columns=['date','beforeafter'])
    EarningsDateHistoryNew['date']=dateList
    EarningsDateHistoryNew['beforeafter']=timeList


    #准备数据，需要同步数据

    #暂时先直接从数据库中查出，这里后面要改掉
    code='TSLA.O'
    volList=list()
    for i in range(0,EarningsDateHistoryNew.shape[0]):
        searchDate=EarningsDateHistoryNew['date'].iloc[i]
        searchDateStr = searchDate.strftime("%Y%m%d")
        beforeAfter=EarningsDateHistoryNew['beforeafter'].iloc[i]
        #财报当天的股价数据,这里code要转化一下
        dayPriceDataEarnDay = database.sessionUS.query(database.DayPriceData).filter(database.DayPriceData.CODE == code,database.DayPriceData.DATE==searchDate).first()
        # 获取前后多一个星期的冗余数据进行
        dbStartDate = searchDate - timedelta(days=7)
        dbstartDateStr = dbStartDate.strftime('%Y-%m-%d')
        dbEndDate = searchDate + timedelta(days=7)
        dbendDateStr = dbEndDate.strftime('%Y-%m-%d')
        sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{dbstartDateStr}" and "{dbendDateStr}"'
        outData = pd.DataFrame()
        outData = pd.read_sql(text(sql), con=database.conUS)
        outData = outData.sort_values(by="DATE", ascending=True)
        outData.reset_index(inplace=True, drop=True)

        # 在数据中定位位置
        pos = outData.loc[outData['DATE'] == searchDate].index[0]
        if beforeAfter == 'before':
            dayPriceDataEarnDay=outData.loc[pos]
            dayPriceDataBefore=outData.loc[pos-1]
        else:
            dayPriceDataBefore = outData.loc[pos]
            dayPriceDataEarnDay = outData.loc[pos + 1]
        change=dayPriceDataEarnDay['OPEN']/dayPriceDataBefore['CLOSE'] -1
        volList.append(change)
    pass

    #只保留


def time_cmp(first_time, second_time):
    print(first_time)
    print(second_time)
    return int(time.strftime("%H%M%S", first_time)) - int(time.strftime("%H%M%S", second_time))


#code='HK.00700'
#WideStraddleStrategy('US.TSLA')
#EvenPriceArbitrage('US.TSLA')

'''
ret, data = zfutu.quote_ctx.get_market_snapshot('US.TSLA240412C170000')
if ret == RET_OK:
    print(data)
    print(data['code'][0])    # 取第一条的股票代码
    print(data['code'].values.tolist())   # 转为 list
else:
    print('error:', data)
'''












