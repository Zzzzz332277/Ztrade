import mplfinance as mpf
import pandas as pd

import database
import datetime
testGDT=database.GetWindDaTA(database.con,database.engine,database.session,'HKEX')
codeList=['0001.HK']

startDate = datetime.date(2023, 8, 1)
endDate = datetime.date(2023, 10, 4)

data=testGDT.GetDataBase(codeList,startDate,endDate)
data_column=data.columns[2:8]
data=data[data_column]
data.columns=['datetime','open','high','low','close','volume']
data['datetime']=pd.to_datetime(data['datetime'])
data=data.set_index('datetime')
mpf.plot(data, type="candle")
pass