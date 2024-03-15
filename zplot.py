import mplfinance as mpf
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Kline
import database
import datetime
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.pyplot as plt

def plotCandle(code):
    testGDT=database.GetWindDaTA(database.conUS,database.engineUS,database.sessionUS,'NYSE')
    #兼容list,后续可能需要修改
    codeList=[]
    codeList.append(code)

    #startDate = datetime.date(2023, 8, 1)
    #endDate = datetime.date(2024,2, 13)
    startDate=datetime.date.today()- datetime.timedelta(days=120)
    endDate = datetime.date.today() - datetime.timedelta(days=1)

    data=testGDT.GetDataBase(codeList,startDate,endDate,'daypricedata')
    data_column=data.columns[2:8]
    data=data[data_column]
    data.columns=['datetime','open','close','high','low','volume']
    data['datetime']=pd.to_datetime(data['datetime'])
    data=data.set_index('datetime')

    my_color = mpf.make_marketcolors(up='w',
                                     down='g',
                                     alpha=0.6,
                                     edge={'up':'red','down':'green'},
                                     wick={'up':'red','down':'green'},
                                     volume={'up':'red','down':'green'},
                                    )
    # 设置图表的背景色
    my_style = mpf.make_mpf_style(marketcolors=my_color,
                                  figcolor='(0.82, 0.83, 0.85)',
                                  gridcolor='(0.92, 0.93, 0.95)',
                                  facecolor= '(1, 1,1)')
    plt.close('all')
    fig,axlist=mpf.plot(data, type="candle",title=str(code),style=my_style,volume=True,returnfig=True,figscale=1.48)
    return  fig,axlist

#plotCandle('TSLA.O')