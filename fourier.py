import numpy as np
import pandas as pd
import scipy
from matplotlib import pyplot as plt
from scipy.fftpack import fft
from matplotlib.pylab import mpl

from sqlalchemy import text

import database

mpl.rcParams['font.sans-serif'] = ['SimHei']   #显示中文
mpl.rcParams['axes.unicode_minus']=False       #显示负号

sql = f'select * from daypricedata where CODE = "NVDA.O" AND DATE between "2020-2-1" and "2024-3-17"'
outData = pd.DataFrame()
outData = pd.read_sql(text(sql), con=database.conUS)
outDataClose = outData['CLOSE'].tolist()


fft_y=fft(outDataClose)

N = 1037
x = np.arange(N)  # 频率个数

abs_y = np.abs(fft_y)  # 取复数的绝对值，即复数的模(双边频谱)
angle_y = np.angle(fft_y)  # 取复数的角度

normalization_y=abs_y/N            #归一化处理（双边频谱）
#去除常数项
normalization_y[0]=0
plt.figure()
plt.plot(x,normalization_y,'g')
plt.title('双边频谱(归一化)',fontsize=9,color='green')


plt.figure()
plt.plot(x, angle_y)
plt.title('双边相位谱（未归一化）')
plt.show()

pass