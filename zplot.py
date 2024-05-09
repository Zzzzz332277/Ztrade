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


'''
 #这里替换为自己写的函数
        collections = _construct_candlestick_collections_zplot(xdates, opens, highs, lows, closes,volumes,
                                                         marketcolors=style['marketcolors'],config=config )
                                                         
                                                         
'''
'''
#进行修改，能够输出一个宽度变化的K线图
def _construct_candlestick_collections_zplot(dates, opens, highs, lows, closes,volumes, marketcolors=None, config=None):
    """Represent the open, close as a bar line and high low range as a
    vertical line.

    NOTE: this code assumes if any value open, low, high, close is
    missing they all are missing


    Parameters
    ----------
    opens : sequence
        sequence of opening values
    highs : sequence
        sequence of high values
    lows : sequence
        sequence of low values
    closes : sequence
        sequence of closing values
    marketcolors : dict of colors: up, down, edge, wick, alpha
    alpha : float
        bar transparency

    Returns
    -------
    ret : list
        (lineCollection, barCollection)
    """

    _check_input(opens, highs, lows, closes)

    if marketcolors is None:
        marketcolors = _get_mpfstyle('classic')['marketcolors']

    datalen = len(dates)

    avg_dist_between_points = (dates[-1] - dates[0]) / float(datalen)

    delta = config['_width_config']['candle_width'] / 2.0

    #根据volume计算寻找合适的宽度,采用平均
    volumeMax=np.mean(volumes)
    widthRatio=1
    barVerts = [((date - delta*widthRatio*(volume/volumeMax), open),
                 (date - delta*widthRatio*(volume/volumeMax), close),
                 (date + delta*widthRatio*(volume/volumeMax), close),
                 (date + delta*widthRatio*(volume/volumeMax), open))
                for date, open, close,volume in zip(dates, opens, closes,volumes)]

    rangeSegLow = [((date, low), (date, min(open, close)))
                   for date, low, open, close in zip(dates, lows, opens, closes)]

    rangeSegHigh = [((date, high), (date, max(open, close)))
                    for date, high, open, close in zip(dates, highs, opens, closes)]

    rangeSegments = rangeSegLow + rangeSegHigh

    alpha = marketcolors['alpha']

    overrides = config['marketcolor_overrides']
    faceonly = config['mco_faceonly']

    colors = _make_updown_color_list('candle', marketcolors, opens, closes, overrides)
    colors = [_mpf_to_rgba(c, alpha) for c in colors]  # include alpha
    if faceonly: overrides = None
    edgecolor = _make_updown_color_list('edge', marketcolors, opens, closes, overrides)
    wickcolor = _make_updown_color_list('wick', marketcolors, opens, closes, overrides)

    lw = config['_width_config']['candle_linewidth']

    rangeCollection = LineCollection(rangeSegments,
                                     colors=wickcolor,
                                     linewidths=lw,
                                     )

    barCollection = PolyCollection(barVerts,
                                   facecolors=colors,
                                   edgecolors=edgecolor,
                                   linewidths=lw
                                   )

    return [rangeCollection, barCollection]
'''