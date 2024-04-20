import time
from datetime import datetime, timedelta

import pandas as pd
#import crawler

import yfinance as yf
from sqlalchemy import text

import database



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


pass
CalEarningsVol('tsla')