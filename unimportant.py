import time
from datetime import datetime, timedelta

import pandas as pd
#import crawler

import yfinance as yf
from sqlalchemy import text

import database
import zfutu
from futu import *

import yfinance as yf

# 单股
data = yf.download("^NDX", start="2017-01-01", end="2017-04-30")

timeStamps=data.index.tolist()
dateList=list()
for timeStamp in timeStamps:
    dt = timeStamp.to_pydatetime()
    buffDate = dt.date()
    buffTime = dt.time()

    dateList.append(buffDate)

data['DATE']=dateList
data.drop(columns='Adj Close',inplace=True)         # same
data = data.reset_index(drop=True)
data.rename(columns={'Open': 'OPEN', 'Close': 'CLOSE', 'High': 'HIGH','Low':'LOW','Volume':'VOLUME'}, inplace=True)
data['CODE'] = "^NDX"

'''
ret, data, page_req_key = zfutu.quote_ctx.request_history_kline('US.NDX', start='2019-09-11', end='2019-09-18',
                                                                max_count=1000, page_req_key=None)

if ret == RET_OK:
    if data.empty:
        # 抛出无数据的异常
        # raise myexception.ExceptionFutuNoData('Futu: No data.')
        # 未获取到数据也休眠，避免频繁调用接口
        time.sleep(0.5)
'''


pass
#CalEarningsVol('tsla')