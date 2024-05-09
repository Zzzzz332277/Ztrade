import scipy
import numpy as np
import pandas as pd
from sqlalchemy import text
import database
import mplfinance as mpf
import matplotlib.pyplot as plt


input=[0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0]
gaussianKernel=scipy.ndimage.filters.gaussian_filter1d(input,2)

sql = f'select * from daypricedata where CODE = "TSLA.O" AND DATE between "2022-1-1" and "2024-3-1"'
outData = pd.DataFrame()
outData = pd.read_sql(text(sql), con=database.conUS)
outDataClose = outData['CLOSE'].tolist()

data_column = outData.columns[2:8]
data = outData[data_column]
data.columns = ['datetime', 'open', 'close', 'high', 'low', 'volume']
data['datetime'] = pd.to_datetime(data['datetime'])
data = data.set_index('datetime')

gaussianMA=np.convolve(outDataClose, gaussianKernel)
#截取前后边缘6位
gaussianMA=gaussianMA[6:-6]
add_plot = mpf.make_addplot(gaussianMA)

my_color = mpf.make_marketcolors(up='w',
                                 down='g',
                                 alpha=0.6,
                                 edge={'up': 'red', 'down': 'green'},
                                 wick={'up': 'red', 'down': 'green'},
                                 volume={'up': 'red', 'down': 'green'},
                                 )
# 设置图表的背景色
my_style = mpf.make_mpf_style(marketcolors=my_color,
                              figcolor='(0.82, 0.83, 0.85)',
                              gridcolor='(0.92, 0.93, 0.95)',
                              facecolor='(1, 1,1)')
mpf.plot(data, type="candle", style=my_style,addplot=add_plot,  mav=(5),volume=True, returnfig=True, figscale=1.48)
plt.show()  # 显示
plt.close()  # 关闭plt，释放内存
#return fig, axlist
pass
pass