import numpy as np
from scipy import interpolate
from scipy.misc import derivative

import stockclass


# 这里设置判断的类，将形态判断的相关函数放在里面
class Recognition:
    def __init__(self):
        pass

    def RecognitionProcess(self,stocklist):
        for stock in stocklist:
            stockInProcess = stock#取出对象值
            #取出stocklist后，开始进行识别的操作链'
            diffusionRestul = self.EmaDiffusion(stockInProcess)
            #判断回踩
            backStepResult = self.BackStepEma(stockInProcess)

    def EmaDiffusion(self, stock):
        # 根据上下关系判断，均线的顺序也是从上到下降低,5条均线
        if self.RelativeRelationofTwoLine(stock.EMAData['EMA5'], stock.EMAData['EMA10']) == '1up2':
            if self.RelativeRelationofTwoLine(stock.EMAData['EMA10'], stock.EMAData['EMA20']) == '1up2':
                if self.RelativeRelationofTwoLine(stock.EMAData['EMA20'], stock.EMAData['EMA30']) == '1up2':
                    if self.RelativeRelationofTwoLine(stock.EMAData['EMA30'], stock.EMAData['EMA60']) == '1up2':
                        return 'diffusion'

        else:
            return 'undiffusion'

    # 对组件进行逻辑定义，类下有没有更小的类
    def RelativeRelationofTwoLine(self, line1, line2):

        if len(line1) != len(line2):
            print('参数错误，长度不相等')
            return 0
        # 判断是否相交,以及线段上下关系
        lineResult = line1 - line2
        k = 0
        for i in lineResult:
            if i > 0:
                k = k + 1

        if k == len(lineResult):
            return '1up2'  # 全是正数，代表1在2上面
        else:
            if k == 0:
                return '1down2'  # 全是负数，代表1在2下面
            else:
                return 'cross'  # 剩下就是正负交替，代表相交

    # 先单独测试趋势识别这段能不能跑的通,输入一组数组，输出为这组数组的趋势

    def RecognizeTrend(self, stock):
        # 函数设计
        # 插值进入数组内
        # 再根据导数求得序列的点，返回一段段趋势的位置
        # 多项式.deriv(m=n) 使用deriv函数
        # 需要使用scipy的interpid函数，顺序是先插值获取到函数，返回了一条曲线。
        # derivative of y with respect to x,用derivative函数求导
        # df_dx = derivative(f, x_fake, dx=1e-6)
        # 实际上返回的是线段
        line = stock.EMAData['EMA20']
        TIME = stock.dayPriceData['DATE']
        line.transpose()
        x = np.arange(1, len(line) + 1, 1)

        f = interpolate.interp1d(x, line, fill_value="extrapolate")
        x_fake = np.arange(1, len(line), 0.1)
        # derivative of y with respect to x
        df_dx = derivative(f, x_fake, dx=1e-6)
        """
        from sympy import diff, symbols
        t = symbols('x', real=True)
        for i in range(1, 4):
            print
            diff(t ** 5, t, i)
            print
            diff(t ** 5, t, i).subs(t, i), i
        """
        #fig = plt.figure(figsize=(12, 8))
        # plt.plot(df_dx)
        # plt.plot((f(x_fake)-40)/20)
        # plt.show()

        # for i in df_dx:
        #    if (df_dx[i] * df_dx[i+1]) < 0:
        #        positionArray[i]=1
        positionArray = 100 * df_dx[:-1] * df_dx[1:]
        positionArray[positionArray > 0] = 0
        # 将nan值全部赋0
        i = 0
        while i < len(positionArray):
            if np.isnan(positionArray[i]):
                positionArray[i] = 0
            i = i + 1

        #plt.plot(positionArray)
        # 获取到位置后还要进行映射，求取到原来输入的值
        pos = np.nonzero(positionArray)
        pos = np.array(pos, ndmin=0)
        pos = pos / 10  # 进行映射，除以10
        pos_1d = pos[0][:]

        for i in range(0, len(pos_1d)):
            pos_i_int = int(pos_1d[i])
            pos_i_float = pos_1d[i] - pos_i_int
            if pos_i_float > 0.5:
                pos_1d[i] = pos_i_int + 1
            else:
                pos_1d[i] = pos_i_int

        # 开始确定趋势的时间，起始时间，结束时间，方向
        # dataset = [Trend for _ in range(100)]
        dataset = []

        # trend=Trend(time[1],time[1],time[11]-time[1],'up')
        for i in range(0, len(pos_1d) - 1):
            index1 = int(pos_1d[i])
            index2 = int(pos_1d[i + 1])
            if line[index1] < line[index2]:
                direction = 'up'
            else:
                direction = 'down'

            dataset.append(stockclass.Trend(TIME[index1], TIME[index2], TIME[index2] - TIME[index1], direction))
            stock.trendList=dataset
        pass
        return dataset

    # 均线回踩
    def BackStepEma(self, stock):
        trendlist = self.RecognizeTrend(stock)  # 这里需要改一下，获取到一个trend类的数组
        # 当60日均线的趋势是向上的，而且在趋势中时
        lastTrend = trendlist[-1]
        if lastTrend.Direction == 'up':
            # if kline[close].today/ema20.today<1.1 根据一个比例来进行判断 当天的价格
            databuff = stock.dayPriceData['CLOSE']
            closePirce = databuff[-1]  # 取最后一个
            if databuff[-2] < databuff[-1]:
                print('不符合回踩标准')
                return "不是回踩"
            else:
                distance = []
                distance[0] = abs(closePirce - stock.EMAData['EMA5'].iloc[-1])
                distance[1] = abs(closePirce - stock.EMAData['EMA10'].iloc[-1])
                distance[2] = abs(closePirce - stock.EMAData['EMA20'].iloc[-1])
                pos = distance.index(min(distance))
                if pos == 0:
                    pass
                    # return '回踩5日均线' 回踩5日意义不大
                else:
                    if pos == 1:
                        return '回踩10日均线'
                    else:
                        return '回踩20日均线'

        else:
            return "不是上升趋势"
        # if kline[close].today/ema30.today<1.1 到底踩哪条均线，需要判断
        # 返回回踩的均线的值，返回状态是否均线回踩

    # 抄底
    def CatchBottom(self, stock):
        trend = self.RecognizeTrend(stock)  # 判断下降趋势，
        # 伪代码
        # 当下行趋势中
        # if 资金大量进入
        # 判断方式，连续几日的资金都大量进入（大量的量化）
