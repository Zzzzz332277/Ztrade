import datetime
import time
import warnings

import numpy as np
import pandas as pd
from scipy import interpolate
from scipy.misc import derivative
import database
import stockclass
#from WindPy import *
import talib
from sqlalchemy import text,create_engine, Table, Column, Integer, String, Float,Date, MetaData, ForeignKey, desc, inspect
import basic
import zfutu
from futu import *
import crawler
##################################测试使用pandas ewm计算ema#########################
'''
sql = f'select * from daypricedata where CODE = "0700.HK" AND DATE between "2023-8-1" and "2023-10-17"'
outData = pd.DataFrame()
outData = pd.read_sql(text(sql), con=database.con)
outDataClose = outData['CLOSE'].tolist()
tic=recognition.TechIndex()


ema_10 = np.array(outDataClose.ewm(span=20, min_periods=0, adjust=False, ignore_na=False).mean())
sql1EMA = f'select * from expma where CODE = "0700.HK" AND DATE between "2023-8-1" and "2023-10-11"'
outDataEMA = pd.read_sql(text(sql1EMA), con=database.con)
outDataEMA10=outDataEMA[outDataEMA['PERIOD']==20]
outDataEMA10value=np.array(outDataEMA10['EXPMA'])
delta=outDataEMA10value-ema_10
pass
'''

# gwd.UpdateTimePeriodDataKDJ(codeList, startDate, endDate, 'kdj')
# tic.CalcKDJ(codeList,database.session,database.con)




# 这里设置判断的类，将形态判断的相关函数放在里面
class Recognition:
    recogProcList= ['code', 'backstepema', 'EmaDiffusion', 'EMAUpCross','MoneyFlow','EMA10BottomArc','EMA10TOPArc','MACDBottomArc','MACDTopArc','EMADownCross',]
    def __init__(self):
        #必须声明为实例变量，放在外面就是类变量
        self.resultTable = pd.DataFrame(columns=self.recogProcList)
        pass

    def RecognitionProcess(self,stocklist):
        for stock in stocklist:
            stockInProcess = stock#取出对象值
            #加入成交额的判断
            turnVolumeTresh=5000000 #500万
            lastClose=stockInProcess.dayPriceData['CLOSE'].iloc[-1]
            lastVolume=stockInProcess.dayPriceData['VOLUME'].iloc[-1]
            #加入波动率超过40的判断
            vol60 = stock.vol60
            # 只插入波动率比40大的
            if float(vol60) < 0.4:
                continue

            #成交量超过500万才进行判断
            if lastClose*lastVolume>=turnVolumeTresh:
                t1=time.time()
                #这里自动生成字典
                dicBuff={i:0 for i in self.recogProcList}
                dicBuff['code']=stockInProcess.code
                #dicBuff={'code':stockInProcess.code,'backstepema':0, 'EmaDiffusion':0, 'EMAUpCross':0,'MoneyFlow':0,'EMA5BottomArc':0,'EMA5TOPArc'""}

                #取出stocklist后，开始进行识别的操作链'
                dicBuff['backstepema']= self.BackStepEma(stockInProcess)
                #判断回踩
                dicBuff['EmaDiffusion'] = self.EmaDiffusion(stockInProcess)
                #catchBottumResult = self.CatchBottom(stockInProcess)
                dicBuff['EMAUpCross'] = self.EMAUpCross(stockInProcess)
                #判断资金流入
                dicBuff['MoneyFlow'] = self.MoneyFLowFutu(stockInProcess)
                # 判断底部圆弧
                dicBuff['EMA10BottomArc'] = self.EMA10BottomArc(stockInProcess)
                #识别顶部圆弧
                dicBuff['EMA10TOPArc'] = self.EMA10TOPArc(stockInProcess)
                # 识别底部MACD弧
                dicBuff['MACDBottomArc'] = self.MACDBottomArc(stockInProcess)
                # 识别顶部MACD弧
                dicBuff['MACDTopArc'] = self.MACDTopArc(stockInProcess)
                # 识别顶部EMA均线死叉
                dicBuff['EMADownCross'] = self.EMADownCross(stockInProcess)

                #将结果添加到resultable
                self.resultTable.loc[len(self.resultTable)] = dicBuff
                t2 = time.time()
                print("运行时间：" + str((t2 - t1) / 1000000) + "秒")



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
        dataset = []

        if np.count_nonzero(positionArray)==0:
        #说明是单边，判断趋势方向并返回
            if line.iloc[-1]>line.iloc[0]:
                direction = 'up'
            else:
                direction = 'down'
            dataset.append(stockclass.Trend(TIME.iloc[0], TIME.iloc[-1],  TIME.iloc[-1]-TIME.iloc[0], direction))
            stock.trendList = dataset
            #直接返回
            return dataset

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
        # trend=Trend(time[1],time[1],time[11]-time[1],'up')
        #将pos_1d前方添加一位0表示首位，末尾添加最后一列的位置代表末位
        pos_1d=np.insert(pos_1d,0,0)
        pos_1d=np.append(pos_1d,len(line)-1)

        for i in range(0, len(pos_1d)-1):
            index1 = int(pos_1d[i])
            index2 = int(pos_1d[i + 1])
            if line[index1] < line[index2]:
                direction = 'up'
            else:
                direction = 'down'

            dataset.append(stockclass.Trend(TIME[index1], TIME[index2], TIME[index2] - TIME[index1], direction))
        stock.trendList=dataset
        return dataset

    # 均线回踩
    def BackStepEma(self, stock):
        print(f"开始识别均线回踩：{stock.code}")
        trendlist = self.RecognizeTrend(stock)  # 这里需要改一下，获取到一个trend类的数组
        # 当60日均线的趋势是向上的，而且在趋势中时
        lastTrend = trendlist[-1]
        if lastTrend.Direction == 'up':
            databuff = stock.dayPriceData['CLOSE']
            #增加下影线和蜡烛实体的比例判断，因为阳柱阴柱处理方式不同，用min来处理
            downShadowLen=abs(min(stock.dayPriceData['CLOSE'].iloc[-1],stock.dayPriceData['OPEN'].iloc[-1])-stock.dayPriceData['LOW'].iloc[-1])
            candleLen=abs(stock.dayPriceData['CLOSE'].iloc[-1]-stock.dayPriceData['OPEN'].iloc[-1])
            closePirce = databuff.iloc[-1]  # 取最后一个
            if (databuff.iloc[-2] < databuff.iloc[-1]) or (downShadowLen < candleLen):
                print('不符合回踩标准')
                return 0
            #阴线且成交量大于之前的一根
            if stock.dayPriceData['CLOSE'].iloc[-1]<stock.dayPriceData['OPEN'].iloc[-1] and stock.dayPriceData['VOLUME'].iloc[-1]>stock.dayPriceData['VOLUME'].iloc[-2]:
                print('最后一根增量下跌，不符合')
                return 0
            else:
                distanceList = []
                distanceToEma5 = abs(closePirce - stock.EMAData['EMA5'].iloc[-1])
                distanceToEma10 = abs(closePirce - stock.EMAData['EMA10'].iloc[-1])
                distanceToEma20 = abs(closePirce - stock.EMAData['EMA20'].iloc[-1])
                distanceList.append(distanceToEma5)
                distanceList.append(distanceToEma10)
                distanceList.append(distanceToEma20)

                pos = distanceList.index(min(distanceList))
                if pos == 0:
                    pass
                    print(f"{stock.code}回踩5日均线")
                    return 1
                else:
                    if pos == 1:
                        print(f"{stock.code}回踩10日均线")
                        return 1
                    else:
                        print(f"{stock.code}回踩20日均线")
                        return 1

        else:
            print("不是上升趋势")
            return 0
        # 返回回踩的均线的值，返回状态是否均线回踩

    def EmaDiffusion(self, stock):
        print(f"开始识别均线发散：{stock.code}")
        # 根据上下关系判断，均线的顺序也是从上到下降低,4条均线
        # 加入内外盘筛选的阈值，目前只针对香港
        if stock.market == 'HKEX':
            sellBid, buyBid, turnOverRate = crawler.GetEastMoneyData(stock.code)
            if sellBid > buyBid:
                return 0
        #加入对于最后一根均线多空力量判断的分析，如果最后一根是下跌的阴线，且放量。就排除
        if stock.dayPriceData['CLOSE'].iloc[-1]<stock.dayPriceData['CLOSE'].iloc[-2] and stock.dayPriceData['CLOSE'].iloc[-1]<stock.dayPriceData['OPEN'].iloc[-1]:
            if stock.dayPriceData['VOLUME'].iloc[-1]>stock.dayPriceData['VOLUME'].iloc[-2]:
                print('最后一根增量下跌，不符合')
                return 0
        #进行波动率筛选,低于33%波动率筛选掉：
        if float(stock.vol60)<0.33:
            return 0

        if stock.EMAData['EMA5'].iloc[-1] > stock.EMAData['EMA10'].iloc[-1]:
            if stock.EMAData['EMA10'].iloc[-1] > stock.EMAData['EMA20'].iloc[-1]:
                if stock.EMAData['EMA20'].iloc[-1] > stock.EMAData['EMA30'].iloc[-1]:
                    # 再判断各个均线是否是扩散状态的
                    EMA5difussion = stock.EMAData['EMA5'].iloc[-1] > stock.EMAData['EMA5'].iloc[-2] and stock.EMAData['EMA5'].iloc[-2] > stock.EMAData['EMA5'].iloc[-3]
                    EMA10difussion = stock.EMAData['EMA10'].iloc[-1] > stock.EMAData['EMA10'].iloc[-2] and stock.EMAData['EMA10'].iloc[-2] > stock.EMAData['EMA10'].iloc[-3]
                    EMA20difussion = stock.EMAData['EMA20'].iloc[-1] > stock.EMAData['EMA20'].iloc[-2] and stock.EMAData['EMA20'].iloc[-2] > stock.EMAData['EMA20'].iloc[-3]
                    EMA30difussion = stock.EMAData['EMA30'].iloc[-1] > stock.EMAData['EMA30'].iloc[-2] and stock.EMAData['EMA30'].iloc[-2] > stock.EMAData['EMA30'].iloc[-3]
                    EMA60difussion = stock.EMAData['EMA60'].iloc[-1] > stock.EMAData['EMA60'].iloc[-2] and stock.EMAData['EMA60'].iloc[-2] > stock.EMAData['EMA60'].iloc[-3]
                    if (EMA5difussion and EMA10difussion and EMA20difussion and EMA30difussion and EMA60difussion):
                        #这里也对最后收盘价进行验证
                        if stock.dayPriceData['CLOSE'].iloc[-1]>stock.dayPriceData['CLOSE'].iloc[-2]:
                            print(f"{stock.code}均线发散")
                            return 1

        print(f"{stock.code}均线不发散")
        return 0
        '''
        if self.RelativeRelationofTwoLine(stock.EMAData['EMA5'], stock.EMAData['EMA10']) == '1up2':
            if self.RelativeRelationofTwoLine(stock.EMAData['EMA10'], stock.EMAData['EMA20']) == '1up2':
                if self.RelativeRelationofTwoLine(stock.EMAData['EMA20'], stock.EMAData['EMA30']) == '1up2':
                    if self.RelativeRelationofTwoLine(stock.EMAData['EMA30'], stock.EMAData['EMA60']) == '1up2':

                        print(f"{stock.code}均线发散")
                        return 1
        else:
            print(f"{stock.code}均线不发散")
            return 0
        '''
    # 抄底
    def EMAUpCross(self, stock):
        print(f'{stock.code}开始均线金叉识别底部')
        trendlist = self.RecognizeTrend(stock)  # 判断下降趋势，
        lastTrend = trendlist[-1]
        databuff = stock.dayPriceData['CLOSE']
        closePirce = databuff.iloc[-1]  # 取最后一个
        if lastTrend.Direction == 'up':
            print('不是下降趋势')
            return 0
        else:
            if (stock.EMAData['EMA5'].iloc[-1] - stock.EMAData['EMA10'].iloc[-1]>0) and (stock.EMAData['EMA5'].iloc[-3] - stock.EMAData['EMA10'].iloc[-3]<0):
                print(f"{stock.code}底部5日10日均线金叉")
                return 1
            else:
                return 0

        # 伪代码
        # 当下行趋势中
        # if 资金大量进入
        # 判断方式，连续几日的资金都大量进入（大量的量化）

    def MoneyFLowWIND(self,stock):
        cashFlowStartDate = stock.startDate
        cashFlowEndDate = stock.endDate
        statrTimeStr = cashFlowStartDate.strftime('%Y-%m-%d')
        endTimeStr = cashFlowEndDate.strftime('%Y-%m-%d')
        cashFLow=[]
        treshHold=0.25
        #获取当天的成交量和成交额
        lastClose = stock.dayPriceData['CLOSE'].iloc[-1]
        lastOpen = stock.dayPriceData['OPEN'].iloc[-1]
        lastVolume = stock.dayPriceData['VOLUME'].iloc[-1]
        turnVolume=((lastClose+lastOpen)/2)*lastVolume
        w.start()
        for i in range(0,4):
            #只获取最后一天的
            cashFLowbuff=w.wsd(stock.code, "mfd_netbuyamt", endTimeStr, endTimeStr, f"traderType={i+1};TradingCalendar={self.tradingCalendar};PriceAdj=F,ShowBlank=-1")
            cashFLowList=cashFLowbuff.Data[0]
            cashFLow.append(cashFLowList[0])
        #if (cashFLow[0])>0 and (cashFLow[1])>0 and (cashFLow[2])>0:
        totalFlowIn=cashFLow[0]+cashFLow[1]+cashFLow[2]+cashFLow[3]
        flowInRatio=totalFlowIn/turnVolume
        if flowInRatio>treshHold:
            print('主力资金流入，可能存在抄底机会')
            return 1

        print('未见明显主力资金流入')
        return 0

    def MoneyFLowFutu(self,stock):
        #富途默认是当天的结果，没有日期参数
        treshHold = 0.19
        #先进行转化code
        codeFutu=zfutu.CodeTransWind2FUTU(stock.code)
        ret, data = zfutu.quote_ctx.get_capital_distribution(codeFutu)
        if ret == RET_OK:
            #获取四个分类的资金流入
            print("获取资金成功")
            cashFLowSup=data['capital_in_super'].values-data['capital_out_super'].values
            cashFLowBig=data['capital_in_big'].values-data['capital_out_big'].values
            cashFLowMid=data['capital_in_mid'].values-data['capital_out_mid'].values
            cashFLowSmall=data['capital_in_small'].values-data['capital_out_small'].values
        else:
            print('error:', data)
            return 0
        #睡一秒，富途接口调用限制30S30次
        time.sleep(1)
        #获取当天的成交量和成交额
        lastClose = stock.dayPriceData['CLOSE'].iloc[-1]
        lastOpen = stock.dayPriceData['OPEN'].iloc[-1]
        lastVolume = stock.dayPriceData['VOLUME'].iloc[-1]
        turnVolume=((lastClose+lastOpen)/2)*lastVolume

        totalCashFlowIn=cashFLowSup+cashFLowBig+cashFLowMid+cashFLowSmall
        #将每日的资金结果存入stock类中
        stock.totalCashFlowIn=totalCashFlowIn
        stock.superCashFlowIn=cashFLowSup
        stock.bigCashFlowIn=cashFLowBig

        if cashFLowSup<0 or cashFLowBig<0:
            print('大单流入资金不足')
            return 0

        if totalCashFlowIn<10000000:
            print('流入资金不足1000万')
            return 0
        flowInRatio=totalCashFlowIn/turnVolume
        if flowInRatio>treshHold:
            print('主力资金流入，可能存在抄底机会')
            return 1

        print('未见明显主力资金流入')
        return 0

    #EMA均线的圆形底
    def EMA5BottomArc(self,stock):
        print('开始识别弧形底部')

        #if stock.market=='HKEX':
            # 流入资金100万的阈值,这里只针对HK股票
            #if stock.totalCashFlowIn<1000000:
                #return 0
            # 加入一个资金流入的筛选判断
            #if stock.superCashFlowIn < 0 or stock.bigCashFlowIn < 0:
                #return 0

        #对于最后一条K线的判断
        lastClose = stock.dayPriceData['CLOSE'].iloc[-1]
        lastOpen = stock.dayPriceData['OPEN'].iloc[-1]
        lastHigh = stock.dayPriceData['HIGH'].iloc[-1]
        lastLow = stock.dayPriceData['LOW'].iloc[-1]

        #是阴线
        if lastClose<lastOpen:
            return 0
        #上下影线计算，到这步直接是阳线
        upShadowLen=abs(lastHigh-lastClose)
        downShadowLen=abs(lastLow-lastOpen)
        candleLen=abs(lastOpen-lastClose)
        #上影线不能超过1/2长度
        if upShadowLen>(candleLen/2):
            return 0
        '''
        # 倒数第二根是阴线，且最后一根阳线成交量未超过倒数第二根
        if stock.dayPriceData['CLOSE'].iloc[-2] < stock.dayPriceData['OPEN'].iloc[-2] and stock.dayPriceData['VOLUME'].iloc[-1] < stock.dayPriceData['VOLUME'].iloc[-2]:
            print('上涨成交量不足，不符合')
            return 0
        '''
        type='5'
        if type != '3' and type != '5':
            print('type 错误')
            return 0

        if type=='5':
            #5个值
            data=stock.EMAData['EMA5']
            arr=np.zeros(5)
            for i in range(0,5):
                #取最后5个
                arr[i]=stock.EMAData['EMA5'].iloc[i-5]
            posMin=np.argmin(arr)
            if posMin==2:
                if arr[1]<arr[0] and arr[3]<arr[4]:
                    print("弧形底")
                    return 1
            if posMin==3:
                if arr[1]<arr[0] and arr[2]<arr[1]:
                    print("弧形底")
                    return 1
            print("不是弧形底")
            return 0

        if type == '3':
            # 3个值
            data = stock.EMAData['EMA5']
            arr=np.zeros(3)
            for i in range(0, 3):
                arr[i] = stock.EMAData['EMA5'].iloc[i - 3]
            posMin = np.argmin(arr)
            if posMin == 1:
                print("弧形底")
                return 1

            print("不是弧形底")
            return 0

    #EMA10弧形底 EMA均线的圆形底
    def EMA10BottomArc(self, stock):
        print('开始识别弧形底部')

        # if stock.market=='HKEX':
        # 流入资金100万的阈值,这里只针对HK股票
        # if stock.totalCashFlowIn<1000000:
        # return 0
        # 加入一个资金流入的筛选判断
        # if stock.superCashFlowIn < 0 or stock.bigCashFlowIn < 0:
        # return 0

        # 对于最后一条K线的判断
        lastClose = stock.dayPriceData['CLOSE'].iloc[-1]
        lastOpen = stock.dayPriceData['OPEN'].iloc[-1]
        lastHigh = stock.dayPriceData['HIGH'].iloc[-1]
        lastLow = stock.dayPriceData['LOW'].iloc[-1]

        # 是阴线
        if lastClose < lastOpen:
            return 0
        # 上下影线计算，到这步直接是阳线
        upShadowLen = abs(lastHigh - lastClose)
        downShadowLen = abs(lastLow - lastOpen)
        candleLen = abs(lastOpen - lastClose)
        # 上影线不能超过1/2长度
        if upShadowLen > (candleLen / 2):
            return 0
        '''
        # 倒数第二根是阴线，且最后一根阳线成交量未超过倒数第二根
        if stock.dayPriceData['CLOSE'].iloc[-2] < stock.dayPriceData['OPEN'].iloc[-2] and stock.dayPriceData['VOLUME'].iloc[-1] < stock.dayPriceData['VOLUME'].iloc[-2]:
            print('上涨成交量不足，不符合')
            return 0
        '''
        type = '5'
        if type != '3' and type != '5':
            print('type 错误')
            return 0

        if type == '5':
            # 5个值
            data = stock.EMAData['EMA10']
            arr = np.zeros(5)
            for i in range(0, 5):
                # 取最后5个
                arr[i] = stock.EMAData['EMA10'].iloc[i - 5]
            posMin = np.argmin(arr)
            if posMin == 2:
                if arr[1] < arr[0] and arr[3] < arr[4]:
                    print("弧形底")
                    return 1
            if posMin == 3:
                if arr[1] < arr[0] and arr[2] < arr[1]:
                    print("弧形底")
                    return 1
            print("不是弧形底")
            return 0

        if type == '3':
            # 3个值
            data = stock.EMAData['EMA10']
            arr = np.zeros(3)
            for i in range(0, 3):
                arr[i] = stock.EMAData['EMA10'].iloc[i - 3]
            posMin = np.argmin(arr)
            if posMin == 1:
                print("弧形底")
                return 1

            print("不是弧形底")
            return 0
#################################################识别下降类的程序##################################################
    #识别弧形顶
    def EMA5TOPArc(self,stock):
        print('开始识别弧形顶部')

        # 对于最后一条K线的判断
        lastClose = stock.dayPriceData['CLOSE'].iloc[-1]
        lastOpen = stock.dayPriceData['OPEN'].iloc[-1]
        lastHigh = stock.dayPriceData['HIGH'].iloc[-1]
        lastLow = stock.dayPriceData['LOW'].iloc[-1]

        # 是阳线
        if lastClose > lastOpen:
            return 0
        # 上下影线计算，到这步直接是阴线
        upShadowLen = abs(lastHigh - lastOpen)
        downShadowLen = abs(lastLow - lastClose)
        candleLen = abs(lastOpen - lastClose)
        # 下影线不能超过1/2长度
        if downShadowLen > (candleLen / 2):
            return 0
        # 这里先锁死5个值
        type = '5'
        if type != '3' and type != '5':
            print('type 错误')
            return 0

        if type == '5':
            # 5个值
            data = stock.EMAData['EMA5']
            arr = np.zeros(5)
            for i in range(0, 5):
                # 取最后5个
                arr[i] = stock.EMAData['EMA5'].iloc[i - 5]
            posMax = np.argmax(arr)
            if posMax == 2:
                if arr[1] > arr[0] and arr[3] > arr[4]:
                    print("弧形顶")
                    return 1
            if posMax == 3:
                if arr[1] > arr[0] and arr[2] > arr[1]:
                    print("弧形顶")
                    return 1
            print("不是弧形顶")
            return 0

        if type == '3':
            # 3个值
            data = stock.EMAData['EMA5']
            arr=np.zeros(3)
            for i in range(0, 3):
                arr[i] = stock.EMAData['EMA5'].iloc[i - 3]
            posMax = np.argmax(arr)
            if posMax == 1:
                print("弧形顶")
                return 1

            print("不是弧形顶")
            return 0

    def EMA10TOPArc(self,stock):
        print('开始识别弧形顶部')

        # 对于最后一条K线的判断
        lastClose = stock.dayPriceData['CLOSE'].iloc[-1]
        lastOpen = stock.dayPriceData['OPEN'].iloc[-1]
        lastHigh = stock.dayPriceData['HIGH'].iloc[-1]
        lastLow = stock.dayPriceData['LOW'].iloc[-1]

        # 是阳线
        if lastClose > lastOpen:
            return 0
        # 上下影线计算，到这步直接是阴线
        upShadowLen = abs(lastHigh - lastOpen)
        downShadowLen = abs(lastLow - lastClose)
        candleLen = abs(lastOpen - lastClose)
        # 下影线不能超过1/2长度
        if downShadowLen > (candleLen / 2):
            return 0
        # 这里先锁死5个值
        type = '5'
        if type != '3' and type != '5':
            print('type 错误')
            return 0

        if type == '5':
            # 5个值
            data = stock.EMAData['EMA10']
            arr = np.zeros(5)
            for i in range(0, 5):
                # 取最后5个
                arr[i] = stock.EMAData['EMA10'].iloc[i - 5]
            posMax = np.argmax(arr)
            if posMax == 2:
                if arr[1] > arr[0] and arr[3] > arr[4]:
                    print("弧形顶")
                    return 1
            if posMax == 3:
                if arr[1] > arr[0] and arr[2] > arr[1]:
                    print("弧形顶")
                    return 1
            print("不是弧形顶")
            return 0

        if type == '3':
            # 3个值
            data = stock.EMAData['EMA10']
            arr=np.zeros(3)
            for i in range(0, 3):
                arr[i] = stock.EMAData['EMA10'].iloc[i - 3]
            posMax = np.argmax(arr)
            if posMax == 1:
                print("弧形顶")
                return 1

            print("不是弧形顶")
            return 0

    def MACDBottomArc(self, stock):
        macd = stock.MACDData
        resultArry = np.zeros(macd.shape[0])
        # 需要考虑两边的情况，后续是看3日后的值，所以边界-3
        arr = np.zeros(5)
        for i in range(0, 3):
            # 取最后3个
            arr[i] = macd['BAR'].iloc[i- 3]
        posMin = np.argmin(arr)
        # 3个值都小于0
        if posMin == 1:
            print("弧形底")
            return 1

        print("不是弧形底")
        return 0

    def MACDTopArc(self, stock):
        macd = stock.MACDData
        resultArry = np.zeros(macd.shape[0])
        # 需要考虑两边的情况，后续是看3日后的值，所以边界-3
        arr = np.zeros(3)
        for i in range(0, 3):
            # 取最后3个
            arr[i] = macd['BAR'].iloc[i - 3]
        posMmax = np.argmax(arr)
        # 3个值都小于0
        if posMmax == 1:
            print("弧形顶")
            return 1
        print("不是弧形顶")
        return 0
                            # 识别顶部的均线下穿
    def EMADownCross(self, stock):
        print(f'{stock.code}开始均线死叉识别顶部')
        trendlist = self.RecognizeTrend(stock)  # 判断下降趋势，
        lastTrend = trendlist[-1]
        databuff = stock.dayPriceData['CLOSE']
        closePirce = databuff.iloc[-1]  # 取最后一个
        if lastTrend.Direction == 'down':
            print('不是上升趋势')
            return 0
        else:
            if (stock.EMAData['EMA5'].iloc[-1] - stock.EMAData['EMA10'].iloc[-1]<0) and (stock.EMAData['EMA5'].iloc[-3] - stock.EMAData['EMA10'].iloc[-3]>0):
                print(f"{stock.code}底部5日10日均线死叉")
                return 1
            else:
                return 0




class MainIndexAnalysis():
    def __init__(self):
        pass

    def SyncMainIndexData(self):
        pass



#预留的以后用来计算技术指标的类
class TechIndex():
    def __init__(self, con, engine, session, tradingcalendar):
        # 初始化时配置数据库连接
        self.con = con
        self.engine = engine
        self.session = session
        self.tradingCalendar = tradingcalendar
        pass


    '''
    计算类的模块的处理逻辑，主要是为了解决之前的数据缺失的问题。需要从数据库中多取一个月数据，然后进行判断。
    整体的计算类模块的逻辑是，先获取到daypricedata日线数据，然后指标数据是少于daypricedata的，然后进行填补。
    '''
    def CalcKDJ(self,codelist):
        for code in codelist:
            print(f'计算{code} kdj')
            # code在表中存在的情况下，在表中获取startdate和enddate
            index = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE=='kdj').first()
            dayPriceDataIndex = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
            if dayPriceDataIndex == None:
                print(f'{code}没有日线数据,无法计算')
                continue
            # 前方数据未对齐的情况
            if index != None:
                # 前方数据未对齐的情况
                if dayPriceDataIndex.STARTDATE < index.STARTDATE:
                    print(f'{code}前方数据未对齐,删除macd数据重新计算')
                    delRet = self.session.query(database.KDJ).filter(database.KDJ.CODE == code).delete()
                    delRetTechIndex = self.session.query(database.TechDateIndex).filter(
                        database.TechDateIndex.CODE == code,
                        database.TechDateIndex.TECHINDEXTYPE == 'kdj').delete()
                    self.session.commit()
                    # 将index设none
                    index = None
                # 前端ema比daypricedata多且后端数据对齐了
                elif index.ENDDATE == dayPriceDataIndex.ENDDATE:
                    print(f'{code}已有kdj数据，不用计算')
                    continue

            if index == None:
                #说明里面没有数据，直接取所有daypricedata进行计算，并更新index
                dbStartDate = dayPriceDataIndex.STARTDATE
                dbEndDate = dayPriceDataIndex.ENDDATE
                startDateStr = dbStartDate.strftime('%Y-%m-%d')
                endDateStr = dbEndDate.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{startDateStr}" and "{endDateStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=self.con)
                outData = outData.sort_values(by="DATE", ascending=True)
                #必须重置索引，否则日期数据会对齐错误
                outData.reset_index(inplace=True, drop=True)

                if outData.shape[0]<10:
                    print('数据不足，无法进行计算求取数据')
                    continue
                # KDJ 值对应的函数是 STOCH
                # 检查输入的pdf是否有nan
                if self.CheckDataHasNan(outData):
                    print("数据中有nan，不完整，无法计算")
                    continue
                KDJResult=self.CalKDJTalib(outData,code)
                KDJResult.to_sql(name='kdj', con=self.engine, if_exists="append", index=False)
                self.con.commit()
                # 更新index的结果
                self.UpdateTechIndex(self.session,code,dbStartDate,dbEndDate,'kdj')

            else:
                KDJStartDate = index.STARTDATE
                KDJEndDate = index.ENDDATE
                #取出日线数据的起始终止值
                dayPriceDataIndex = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
                dpdStartDate=dayPriceDataIndex.STARTDATE
                dpdendDate=dayPriceDataIndex.ENDDATE
                if KDJEndDate>=dpdendDate:
                    print('已有KDJ数据，不用计算')
                    continue

                #获取之前的多一个月的冗余数据进行
                dbStartDate=KDJEndDate-timedelta(days=30)
                dbstartDateStr = dbStartDate.strftime('%Y-%m-%d')
                dbendDateStr = dpdendDate.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{dbstartDateStr}" and "{dbendDateStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=self.con)
                outData = outData.sort_values(by="DATE", ascending=True)
                outData.reset_index(inplace=True, drop=True)

                #在数据中定位位置
                posStart=outData.loc[outData['DATE']==KDJEndDate].index[0]
                if posStart<12:
                    print('前方日线数据不足，无法计算指标，请补充前方日线数据')
                    continue
                #因为计算后有多余数值，确定好截取的位置
                posStartSlice = posStart+1
                #posEnd =outData.loc[outData['DATE']==dbEndDate].index[0]

                outDataBuff=outData
                KDJResult = pd.DataFrame()
                # 检查输入的pdf是否有nan
                if self.CheckDataHasNan(outDataBuff):
                    print("数据中有nan，不完整，无法计算")
                    continue
                KDJResult=self.CalKDJTalib(outDataBuff,code)
                KDJResultSlice=KDJResult[posStartSlice:]
                KDJResultSlice.to_sql(name='kdj', con=self.engine, if_exists="append", index=False)
                self.con.commit()

                # 更新index的结果
                self.UpdateTechIndex(self.session,code,KDJStartDate,dpdendDate,'kdj')

    def CalKDJTalib(self,inputDF,code):
        KDJResult = pd.DataFrame()
        KDJResult['K'], KDJResult['D'] = talib.STOCH(
            inputDF['HIGH'].values,
            inputDF['LOW'].values,
            inputDF['CLOSE'].values,
            fastk_period=9,
            slowk_period=5,
            slowk_matype=1,
            slowd_period=5,
            slowd_matype=1)
        # 求出J值，J = (3*K)-(2*D)
        KDJResult['K'].fillna(0, inplace=True)
        KDJResult['D'].fillna(0, inplace=True)
        KDJResult['J'] = list(map(lambda x, y: 3 * x - 2 * y, KDJResult['K'], KDJResult['D']))
        KDJResult.index = range(len(KDJResult))
        # 保存结果
        KDJResult['DATE'] = inputDF['DATE']
        KDJResult['CODE'] = code
        return KDJResult

    def CalAllEMA(self,codelist):
        #w.start()
        for code in codelist:
            print(f'计算{code} EMA')
            emaPreValueDict={} #用来存放各个周期的第一个值的字典
            # code在表中存在的情况下，在表中分别获取日线数据和ema数据的startdate和enddate
            EMAindex = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE == 'EXPMA').first()
            dayPriceDataIndex = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
            if dayPriceDataIndex == None:
                print(f'{code}没有日线数据,无法计算')
                continue
            if EMAindex != None and EMAindex.ENDDATE==dayPriceDataIndex.ENDDATE:
                print(f'{code}已有ema数据，不用计算')
                continue

            #没有EMA但是有日线数据的情况下
            if EMAindex == None:
                # 说明里面没有数据，需要从wind获取ema的第一个值,获取的是前一天的值
                PreValueDate = dayPriceDataIndex.STARTDATE-timedelta(days=1)
                PreValueDateStr=PreValueDate.strftime('%Y-%m-%d')
                #最终放入数据库的index
                EMAIndexStartDate=dayPriceDataIndex.STARTDATE
                #需要计算ema的区间
                EmaCalStartDate=dayPriceDataIndex.STARTDATE
                EmaCalEndDate=dayPriceDataIndex.ENDDATE
                for period in basic.emaPeriod:
                    buffData = w.wsd(code, "EXPMA", PreValueDateStr, PreValueDateStr,
                                     f"EXPMA_N={period};TradingCalendar={self.tradingCalendar};PriceAdj=F")
                    # 这里取出的是没有时间的。
                    print(f"获取{code}的{period}ema数据")
                    buff=buffData.Data[0]
                    emaPreValueDict[period]=buff[0]

            #有EMA也有日线数据的情况下
            else:
                EMAIndexStartDate = EMAindex.STARTDATE
                EMAIndexEndDate = EMAindex.ENDDATE
                # 需要计算ema的区间
                EmaCalStartDate = EMAIndexEndDate+timedelta(days=1)
                EmaCalEndDate = dayPriceDataIndex.ENDDATE
                # 查询EMA取出前一个值并计算
                for period in basic.emaPeriod:
                    EMAFirst = self.session.query(database.ExpMA).filter(database.ExpMA.CODE == code,database.ExpMA.DATE==EMAIndexEndDate,database.ExpMA.PERIOD==period).first()
                    emaPreValueDict[period]=EMAFirst.EXPMA
            #获取到各个周期第一个值后，获取darpricedata的值,并进行EMA的计算

            EmaCalStartDateStr=EmaCalStartDate.strftime('%Y-%m-%d')
            EmaCalEndDateStr=EmaCalEndDate.strftime('%Y-%m-%d')
            sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{EmaCalStartDateStr}" and "{EmaCalEndDateStr}"'
            outData = pd.DataFrame()
            outData = pd.read_sql(text(sql), con=self.con)
            outData=outData.sort_values(by="DATE",ascending=True)
            closeDataList=outData['CLOSE'].tolist()
            #存储计算结果
            EMACalResult=pd.DataFrame()
            concatDataframe = pd.DataFrame(columns=["EXPMA", "DATE", "CODE", "PERIOD"])
            for key in emaPreValueDict:
                EMACalResultList=self.CalSingleEMA(list=closeDataList,period=key,prevalue=emaPreValueDict[key])
                EMACalResult['DATE']=outData['DATE']
                EMACalResult['CODE']=code
                EMACalResult['PERIOD']=key
                EMACalResult['EXPMA']=EMACalResultList
                concatDataframe = pd.concat([concatDataframe, EMACalResult])

            #写入数据库,并更新index
            concatDataframe.to_sql(name='expma', con=self.engine, if_exists="append", index=False)
            #pymysql更新后需要提交事务，避免查不到数据
            self.con.commit()
            self.UpdateTechIndex(self.session,code,EMAIndexStartDate,EmaCalEndDate,'expma')

    #涉及wind的部分进行修改
    def CalAllEMAUpdate(self,codelist):
        for code in codelist:
            print(f'计算{code} EMA')


            emaPreValueDict={} #用来存放各个周期的第一个值的字典
            # code在表中存在的情况下，在表中分别获取日线数据和ema数据的startdate和enddate
            EMAindex = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE == 'EXPMA').first()
            dayPriceDataIndex = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
            if dayPriceDataIndex == None:
                print(f'{code}没有日线数据,无法计算')
                continue

            if EMAindex != None:
                # 前方数据未对齐的情况
                if dayPriceDataIndex.STARTDATE < EMAindex.STARTDATE:
                    print(f'{code}前方数据未对齐,删除ema数据重新计算')
                    delRetExpma = self.session.query(database.ExpMA).filter(database.ExpMA.CODE == code).delete()
                    delRetTechIndex = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,
                                                                                        database.TechDateIndex.TECHINDEXTYPE == 'EXPMA').delete()
                    self.session.commit()
                    # 将index设none
                    EMAindex = None
                #前端ema比daypricedata多且后端数据对齐了
                elif EMAindex.ENDDATE==dayPriceDataIndex.ENDDATE:
                    print(f'{code}已有ema数据，不用计算')
                    continue
            '''     
            if EMAindex != None and EMAindex.ENDDATE==dayPriceDataIndex.ENDDATE:
                if dayPriceDataIndex.STARTDATE < EMAindex.STARTDATE:
                    print(f'{code}前方数据未对齐,删除ema数据重新计算')
                    delRetExpma= self.session.query(database.ExpMA).filter(database.ExpMA.CODE == code).delete()
                    delRetTechIndex=self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE == 'EXPMA').delete()
                    self.session.commit()
                    #将index设none
                    EMAindex=None
                else:
                    print(f'{code}已有ema数据，不用计算')
                    continue
            '''
            #没有EMA但是有日线数据的情况下
            if EMAindex == None:
                # 说明里面没有数据,重新计算所有数据
                #取出日线数据
                dayPriceDataStartStr = dayPriceDataIndex.STARTDATE.strftime('%Y-%m-%d')
                dayPriceDataEndStr = dayPriceDataIndex.ENDDATE.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{dayPriceDataStartStr}" and "{dayPriceDataEndStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=self.con)
                outData = outData.sort_values(by="DATE", ascending=True)
                outData.reset_index(inplace=True, drop=True)

                closeDataSeries = outData['CLOSE']
                #这里其实应该放在for里面更新，否则在像计算RSI这样周期不相等的数据时就会出问题
                EMACalResult = pd.DataFrame()
                concatDataframe = pd.DataFrame(columns=["EXPMA", "DATE", "CODE", "PERIOD"])
                for period in basic.emaPeriod:
                    print(f"获取{code}的{period}ema数据")

                    # 这里求出的ema6和ema12在数据够多的情况下才准确，前部不准
                    EMABuff = closeDataSeries.ewm(span=period, min_periods=0, adjust=False, ignore_na=False).mean()
                    EMACalResultList=EMABuff.tolist()
                    EMACalResult['DATE'] = outData['DATE']
                    EMACalResult['CODE'] = code
                    EMACalResult['PERIOD'] = period
                    EMACalResult['EXPMA'] = EMACalResultList
                    concatDataframe = pd.concat([concatDataframe, EMACalResult])
                    #计算后连接到大的dataframe
                # 写入数据库,并更新index
                concatDataframe.to_sql(name='expma', con=self.engine, if_exists="append", index=False)
                # pymysql更新后需要提交事务，避免查不到数据
                self.con.commit()
                self.UpdateTechIndex(self.session, code, dayPriceDataIndex.STARTDATE, dayPriceDataIndex.ENDDATE, 'expma')

            #有EMA也有日线数据的情况下
            else:
                EMAIndexStartDate = EMAindex.STARTDATE
                EMAIndexEndDate = EMAindex.ENDDATE
                # 需要计算ema的区间
                EmaCalStartDate = EMAIndexEndDate+timedelta(days=1)
                EmaCalEndDate = dayPriceDataIndex.ENDDATE
                # 查询EMA取出前一个值并计算
                for period in basic.emaPeriod:
                    #start_time = time.time()

                    EMAFirst = self.session.query(database.ExpMA).filter(database.ExpMA.CODE == code,database.ExpMA.DATE==EMAIndexEndDate,database.ExpMA.PERIOD==period).first()

                    emaPreValueDict[period]=EMAFirst.EXPMA
                    #end_time = time.time()

                    #print("耗时: {:.2f}秒".format(end_time - start_time))

                #获取到各个周期第一个值后，获取darpricedata的值,并进行EMA的计算


                EmaCalStartDateStr=EmaCalStartDate.strftime('%Y-%m-%d')
                EmaCalEndDateStr=EmaCalEndDate.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{EmaCalStartDateStr}" and "{EmaCalEndDateStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=self.con)
                outData=outData.sort_values(by="DATE",ascending=True)
                outData.reset_index(inplace=True, drop=True)

                closeDataList=outData['CLOSE'].tolist()
                #存储计算结果
                EMACalResult=pd.DataFrame()
                concatDataframe = pd.DataFrame(columns=["EXPMA", "DATE", "CODE", "PERIOD"])

                for key in emaPreValueDict:
                    EMACalResultList=self.CalSingleEMA(list=closeDataList,period=key,prevalue=emaPreValueDict[key])
                    EMACalResult['DATE']=outData['DATE']
                    EMACalResult['CODE']=code
                    EMACalResult['PERIOD']=key
                    EMACalResult['EXPMA']=EMACalResultList
                    concatDataframe = pd.concat([concatDataframe, EMACalResult])

                #写入数据库,并更新index
                concatDataframe.to_sql(name='expma', con=self.engine, if_exists="append", index=False)
                #pymysql更新后需要提交事务，避免查不到数据
                self.con.commit()
                self.UpdateTechIndex(self.session,code,EMAIndexStartDate,EmaCalEndDate,'expma')

    def CalSingleEMA(self,list,period,prevalue):
        data = [0 for _ in range(len(list))]
        α=2/(period+1)
        for i in range(0,len(list)):
            if i == 0:
                data[i] = α * list[i] + (1 - α) * prevalue
            else:
                data[i] = α * list[i] + (1 - α) * data[i - 1]
        return data# 从首开始循环

    #计算MA值
    def CalAllMAUpdate(self, codelist):
        for code in codelist:
            print(f'计算{code} MA')
            maPreValueDict = {}  # 用来存放各个周期的第一个值的字典
            # code在表中存在的情况下，在表中分别获取日线数据和MA数据的startdate和enddate
            MAindex = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,
                                                                         database.TechDateIndex.TECHINDEXTYPE == 'MA').first()
            dayPriceDataIndex = self.session.query(database.CodeDateIndex).filter(
                database.CodeDateIndex.CODE == code).first()
            if dayPriceDataIndex == None:
                print(f'{code}没有日线数据,无法计算')
                continue

            if MAindex != None:
                # 前方数据未对齐的情况
                if dayPriceDataIndex.STARTDATE < MAindex.STARTDATE:
                    print(f'{code}前方数据未对齐,删除ma数据重新计算')
                    delRetma = self.session.query(database.MA).filter(database.MA.CODE == code).delete()
                    delRetTechIndex = self.session.query(database.TechDateIndex).filter(
                        database.TechDateIndex.CODE == code,
                        database.TechDateIndex.TECHINDEXTYPE == 'MA').delete()
                    self.session.commit()
                    # 将index设none
                    MAindex = None
                # 前端ema比daypricedata多且后端数据对齐了
                elif MAindex.ENDDATE == dayPriceDataIndex.ENDDATE:
                    print(f'{code}已有ma数据，不用计算')
                    continue

            # 没有MA但是有日线数据的情况下
            if MAindex == None:
                # 说明里面没有数据,重新计算所有数据
                # 取出日线数据
                dayPriceDataStartStr = dayPriceDataIndex.STARTDATE.strftime('%Y-%m-%d')
                dayPriceDataEndStr = dayPriceDataIndex.ENDDATE.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{dayPriceDataStartStr}" and "{dayPriceDataEndStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=self.con)
                outData = outData.sort_values(by="DATE", ascending=True)
                outData.reset_index(inplace=True, drop=True)

                closeDataSeries = outData['CLOSE']

                #这里加入判断，普通MA计算数据值必须大于MA的最大周期值
                if len(closeDataSeries)<basic.maPeriod[-1]:
                    print(f'{code}数据不足，无法计算')
                    continue

                # 这里其实应该放在for里面更新，否则在像计算RSI这样周期不相等的数据时就会出问题
                MACalResult = pd.DataFrame()
                concatDataframe = pd.DataFrame(columns=["MA", "DATE", "CODE", "PERIOD"])
                for period in basic.maPeriod:
                    print(f"计算{code}的{period}MA数据")
                    MABuff = talib.MA(closeDataSeries,period)
                    #需要前部的NAN值替换掉
                    MABuff=MABuff.fillna(closeDataSeries.iloc[period-1])
                    MACalResultList = MABuff.tolist()
                    MACalResult['DATE'] = outData['DATE']
                    MACalResult['CODE'] = code
                    MACalResult['PERIOD'] = period
                    MACalResult['MA'] = MACalResultList
                    concatDataframe = pd.concat([concatDataframe, MACalResult])
                    # 计算后连接到大的dataframe
                # 写入数据库,并更新index
                concatDataframe.to_sql(name='ma', con=self.engine, if_exists="append", index=False)
                # pymysql更新后需要提交事务，避免查不到数据
                self.con.commit()
                self.UpdateTechIndex(self.session, code, dayPriceDataIndex.STARTDATE, dayPriceDataIndex.ENDDATE,
                                     'ma')

            # 有MA也有日线数据的情况下,默认是具有足够的数据的，但是也需要检查
            else:
                MAIndexStartDate = MAindex.STARTDATE
                MAIndexEndDate = MAindex.ENDDATE
                # 需要计算MA的区间
                MACalStartDate = MAIndexEndDate + timedelta(days=1)
                MACalEndDate = dayPriceDataIndex.ENDDATE
                MACalResult = pd.DataFrame()
                concatDataframe = pd.DataFrame(columns=["MA", "DATE", "CODE", "PERIOD"])
                # 取出之前的日线数据参与计算，这里需要注意，MA计算是窗口滑动式，必须要前面有数据，要多取前面period的数据，而且数据需要保证无重复，缺失，否则计算错误
                for period in basic.maPeriod:
                    #需要取出的数据日期

                    closeDataPre=self.session.query(database.MA).filter(database.MA.CODE == code,database.MA.PERIOD == period).order_by(database.MA.DATE.desc()).limit(period-1).all()
                    #找出这period个数据的起始值
                    closeDataPreDate=closeDataPre[-1].DATE
                    #再根据时间戳去取出要用close数据
                    dayPriceDataStartStr = closeDataPreDate.strftime('%Y-%m-%d')
                    dayPriceDataEndStr = dayPriceDataIndex.ENDDATE.strftime('%Y-%m-%d')
                    sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{dayPriceDataStartStr}" and "{dayPriceDataEndStr}"'
                    outData = pd.DataFrame()
                    outData = pd.read_sql(text(sql), con=self.con)
                    outData = outData.sort_values(by="DATE", ascending=True)
                    outData.reset_index(inplace=True, drop=True)

                    closeDataSeries = outData['CLOSE']

                    #然后计算结果，写入新的数据
                    print(f"计算{code}的{period}MA数据")
                    MABuff = talib.MA(closeDataSeries, period)
                    # 需要前部的NAN值删除掉
                    MABuff=MABuff.dropna()
                    MACalResultList = MABuff.tolist()
                    MACalResult['DATE'] = outData['DATE'].iloc[(period-1):].reset_index(drop=True)
                    MACalResult['CODE'] = code
                    MACalResult['PERIOD'] = period
                    MACalResult['MA'] = MACalResultList
                    concatDataframe = pd.concat([concatDataframe, MACalResult])


                concatDataframe.to_sql(name='ma', con=self.engine, if_exists="append", index=False)
                # pymysql更新后需要提交事务，避免查不到数据
                self.con.commit()
                self.UpdateTechIndex(self.session, code, MAIndexStartDate, MACalEndDate, 'ma')


    def CalAllRSI(self,codelist):
        for code in codelist:
            print(f'计算{code} RSI')
            # code在表中存在的情况下，在表中获取startdate和enddate
            RSIindex = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,
                                                                         database.TechDateIndex.TECHINDEXTYPE == 'RSI').first()
            dayPriceDataIndex = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
            if dayPriceDataIndex == None:
                print(f'{code}没有日线数据,无法计算')
                continue

                # 前方数据未对齐的情况
            if RSIindex != None:
                # 前方数据未对齐的情况
                if dayPriceDataIndex.STARTDATE < RSIindex.STARTDATE:
                    print(f'{code}前方数据未对齐,删除RSI数据重新计算')
                    delRetRSI = self.session.query(database.RSI).filter(database.RSI.CODE == code).delete()
                    delRetTechIndex = self.session.query(database.TechDateIndex).filter(
                        database.TechDateIndex.CODE == code,
                        database.TechDateIndex.TECHINDEXTYPE == 'rsi').delete()
                    self.session.commit()
                    # 将index设none
                    RSIindex = None
                # 前端ema比daypricedata多且后端数据对齐了
                elif RSIindex.ENDDATE == dayPriceDataIndex.ENDDATE:
                    print(f'{code}已有rsi数据，不用计算')
                    continue


            if RSIindex == None:
                #说明里面没有数据，直接取所有daypricedata进行计算，并更新index
                dbStartDate = dayPriceDataIndex.STARTDATE
                dbEndDate = dayPriceDataIndex.ENDDATE
                startDateStr = dbStartDate.strftime('%Y-%m-%d')
                endDateStr = dbEndDate.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{startDateStr}" and "{endDateStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=self.con)
                outData = outData.sort_values(by="DATE", ascending=True)
                outData.reset_index(inplace=True, drop=True)

                # 检查输入的pdf是否有nan
                if self.CheckDataHasNan(outData):
                    print("数据中有nan，不完整，无法计算")
                    continue

                periods=[6,12,24]
                closeDataList = outData['CLOSE']
                # 存储计算结果
                concatDataframe = pd.DataFrame(columns=["DATE", "CODE", 'UPSMA','DOWNSMA',"PERIOD"])
                #这里计算出来的，由于计算均线和去除第一位的缘故，会分别少6,12,24个值，为了对齐，需要截取使保持一致
                for period in periods:
                    SMACalResult = pd.DataFrame()
                    UPSMA,DOWNSMA = self.CalSingleRSI(closeDataList, period=period)
                    #这里计算出来的upsma和downsma少一位，需要将date第一位去除
                    dateSeris=outData['DATE']
                    dateSerisCut=dateSeris[24:]
                    #重置索引
                    #给upsma和downsma前方添加数据，与日线数据对齐
                    prefixArryUp=np.array([UPSMA[0]]*period)
                    prefixArryDown=np.array([DOWNSMA[0]]*period)
                    prefixSeriesUp=pd.Series(prefixArryUp)
                    prefixSeriesDown=pd.Series(prefixArryDown)
                    UPSMA=pd.concat([prefixSeriesUp,UPSMA],axis=0)
                    DOWNSMA=pd.concat([prefixSeriesDown,DOWNSMA],axis=0)
                    UPSMA.reset_index(inplace=True,drop=True)
                    DOWNSMA.reset_index(inplace=True,drop=True)

                    pass

                    #不需要进行cut
                    '''
                    dateSerisCut.reset_index(inplace=True,drop=True)
                    UPSMACut=UPSMA[(24-period):]
                    DOWNSMACut=DOWNSMA[(24-period):]
                    UPSMACut.reset_index(inplace=True,drop=True)
                    DOWNSMACut.reset_index(inplace=True,drop=True)
                    '''
                    SMACalResult['DATE'] = dateSeris
                    SMACalResult['CODE'] = code
                    SMACalResult['PERIOD'] = period
                    SMACalResult['UPSMA'] = UPSMA
                    SMACalResult['DOWNSMA']=DOWNSMA
                    SMACalResult['RSI']=100*UPSMA/(UPSMA+DOWNSMA)
                    concatDataframe = pd.concat([concatDataframe, SMACalResult])

                # 写入数据库,并更新index
                concatDataframe.to_sql(name='rsi', con=self.engine, if_exists="append", index=False)
                self.con.commit()
                # 更新index的结果
                #因为算出来比前面少一位，所以需要将起始位置减一天
                RSIStartDate = dateSeris.iloc[0]
                RSIEndDate = dateSeris.iloc[-1]
                self.UpdateTechIndex(self.session,code,RSIStartDate,dbEndDate,'rsi')

            #有日线数据也有RSI数据的情况下
            else:
                RSIIndexStartDate = RSIindex.STARTDATE
                RSIIndexEndDate = RSIindex.ENDDATE
                # 需要计算rsi的区间,要往前多取一天，把之前最后一天RSI对应的DAYPRICEDATA也取出来
                RSICalStartDate = RSIIndexEndDate
                RSICalEndDate = dayPriceDataIndex.ENDDATE
                # 查询rsi取出前一个值并计算
                periods=[6,12,24]
                # 获取到各个周期第一个值后，获取darpricedata的值,并进行EMA的计算

                RSICalStartDateStr = RSICalStartDate.strftime('%Y-%m-%d')
                RSICalEndDateStr = RSICalEndDate.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{RSICalStartDateStr}" and "{RSICalEndDateStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=self.con)
                outData = outData.sort_values(by="DATE", ascending=True)
                outData.reset_index(inplace=True, drop=True)

                closeDataList = outData['CLOSE']
                # 存储计算结果
                concatDataframe = pd.DataFrame(columns=["DATE", "CODE", 'UPSMA', 'DOWNSMA', "PERIOD"])
                for period in periods:
                    SMACalResult = pd.DataFrame()
                    RSIFirst = self.session.query(database.RSI).filter(database.RSI.CODE == code,
                                                                       database.RSI.DATE == RSIIndexEndDate,
                                                                       database.RSI.PERIOD == period).first()
                    UPSMAFirst=RSIFirst.UPSMA
                    DOWNSMAFirst=RSIFirst.DOWNSMA
                    UPSMA,DOWNSMA = self.CalSingleRSIIterate(price=closeDataList, period=period, upsma=UPSMAFirst,downsma=DOWNSMAFirst)
                    dateSeris = outData['DATE']
                    dateSeris = dateSeris.drop(0)
                    dateSeris.reset_index(inplace=True,drop=True)
                    SMACalResult['DATE'] = dateSeris
                    SMACalResult['CODE'] = code
                    SMACalResult['PERIOD'] = period
                    SMACalResult['UPSMA'] = UPSMA
                    SMACalResult['DOWNSMA'] = DOWNSMA
                    SMACalResult['RSI']=100*UPSMA/(UPSMA+DOWNSMA)

                    concatDataframe = pd.concat([concatDataframe, SMACalResult])

                    # 写入数据库,并更新index
                concatDataframe.to_sql(name='rsi', con=self.engine, if_exists="append", index=False)
                # pymysql更新后需要提交事务，避免查不到数据
                self.con.commit()
                self.UpdateTechIndex(self.session, code, RSIIndexStartDate, RSICalEndDate, 'rsi')

    def CalSingleRSI(self,price, period):
        #这里注意传进来的是series,list没有shift和index可用,因为是依次减去的关系，这里计算的rsi第一位没有
        clprcChange = price - price.shift(1)
        clprcChange = clprcChange.dropna()

        indexprc = clprcChange.index
        upPrc = pd.Series(0, index=indexprc)
        upPrc[clprcChange > 0] = clprcChange[clprcChange > 0]

        downPrc = pd.Series(0, index=indexprc)
        downPrc[clprcChange < 0] = -clprcChange[clprcChange < 0]
        risdata = pd.concat([price, clprcChange, upPrc, downPrc], axis=1)
        risdata.columns = ['price', 'PrcChange', 'upPrc', 'downPrc']
        risdata = risdata.dropna()

        #这里需要修改一下，试着先计算出前period个数个平均值，作为后续的ewm的初始值，看是否有影响
        firstUpPrc=np.mean(upPrc.values[1:period])
        firstUpPrcSeris=pd.Series([firstUpPrc])
        firstDownPrc=np.mean(downPrc.values[1:period])
        firstDownPrcSeris=pd.Series([firstDownPrc])
        #这里格外注意，切片-1不是取最后一个
        upPrcBack=upPrc[(period):]
        downPrcBack=downPrc[(period):]
        upPrcConcat=pd.concat([firstUpPrcSeris,upPrcBack], axis=0)
        downPrcConcat=pd.concat([firstDownPrcSeris,downPrcBack], axis=0)
        #SMUP = []
        #SMDOWN = []
        '''
        for i in range(period, len(upPrc) + 1):
            #这里有问题，算移动平均的时候，这里的SMA和同花顺的算法不一样
            SMUP.append(np.mean(upPrc.values[(i - period): i], dtype=np.float32))
            SMDOWN.append(np.mean(downPrc.values[(i - period): i], dtype=np.float32))
        '''
        SMUP=upPrcConcat.ewm(alpha=1/period,adjust=False).mean()
        SMDOWN=downPrcConcat.ewm(alpha=1/period,adjust=False).mean()
        rsi = [100 * SMUP.iloc[i] / (SMUP.iloc[i] + SMDOWN.iloc[i]) for i in range(0, len(SMUP))]
        rsipd=pd.DataFrame(rsi)
        #indexRsi = indexprc[(period - 1):]
        rsi = pd.Series(rsi)
        return SMUP,SMDOWN

    #通过迭代方法计算RSI
    def CalSingleRSIIterate(self,price, period,upsma,downsma):
        clprcChange = price - price.shift(1)
        clprcChange = clprcChange.dropna()

        indexprc = clprcChange.index
        upPrc = pd.Series(0, index=indexprc)
        upPrc[clprcChange > 0] = clprcChange[clprcChange > 0]

        downPrc = pd.Series(0, index=indexprc)
        downPrc[clprcChange < 0] = -clprcChange[clprcChange < 0]

        upData = [0 for _ in range(len(upPrc))]
        downData= [0 for _ in range(len(upPrc))]
        α =1/(period)
        for i in range(0, len(upPrc)):
            if i == 0:
                upData[i] = α * upPrc.iloc[i] + (1 - α) * upsma
                downData[i]=α * downPrc.iloc[i] + (1 - α) * downsma
            else:
                upData[i] = α * upPrc.iloc[i] + (1 - α) * upData[i - 1]
                downData[i]=α * downPrc.iloc[i] + (1 - α) * downData[i - 1]
        return pd.Series(upData),pd.Series(downData)  # 从首开始循环

    def CalAllMACD(self,codelist):
        for code in codelist:
            print(f'计算{code} MACD')
            #emaPreValueDict = {}  # 用来存放各个周期的第一个值的字典
            # code在表中存在的情况下，在表中分别获取日线数据和ema数据的startdate和enddate
            MACDindex = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,
                                                                         database.TechDateIndex.TECHINDEXTYPE == 'macd').first()
            dayPriceDataIndex = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
            if dayPriceDataIndex == None:
                print(f'{code}没有日线数据,无法计算')
                return 0
                # 前方数据未对齐的情况
            if MACDindex != None:
                # 前方数据未对齐的情况
                if dayPriceDataIndex.STARTDATE < MACDindex.STARTDATE:
                    print(f'{code}前方数据未对齐,删除macd数据重新计算')
                    delRet = self.session.query(database.MACD).filter(database.MACD.CODE == code).delete()
                    delRetTechIndex = self.session.query(database.TechDateIndex).filter(
                        database.TechDateIndex.CODE == code,
                        database.TechDateIndex.TECHINDEXTYPE == 'macd').delete()
                    self.session.commit()
                    # 将index设none
                    MACDindex = None
                # 前端ema比daypricedata多且后端数据对齐了
                elif MACDindex.ENDDATE == dayPriceDataIndex.ENDDATE:
                    print(f'{code}已有macd数据，不用计算')
                    continue

            # 没有EMA但是有日线数据的情况下
            if MACDindex == None:
                # 说明里面没有数据，需要取全部数据重新计算MACD
                dayPriceDataStartStr = dayPriceDataIndex.STARTDATE.strftime('%Y-%m-%d')
                dayPriceDataEndStr = dayPriceDataIndex.ENDDATE.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{dayPriceDataStartStr}" and "{dayPriceDataEndStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=self.con)
                outData = outData.sort_values(by="DATE", ascending=True)
                outData.reset_index(inplace=True, drop=True)

                closeDataSeries = outData['CLOSE']
                #这里求出的ema6和ema12在数据够多的情况下才准确，前部不准
                EMA12 = closeDataSeries.ewm(span=12, min_periods=0, adjust=False, ignore_na=False).mean()
                EMA26 = closeDataSeries.ewm(span=26, min_periods=0, adjust=False, ignore_na=False).mean()

                DIF=EMA12-EMA26
                #DEA = DIF.ewm(9, adjust=False).mean()
                DEA=self.CalSingleEMA(list=DIF.tolist(),period=9,prevalue=DIF.iloc[0])
                DEA=pd.Series(DEA)
                BAR = 2 * (DIF - DEA)

                MACDRestult = pd.DataFrame()

                MACDRestult['DATE'] = outData['DATE']
                MACDRestult['CODE'] = code
                MACDRestult['EMA12'] = EMA12
                MACDRestult['EMA26'] = EMA26
                MACDRestult['DIF'] = DIF
                MACDRestult['DEA'] = DEA
                MACDRestult['BAR'] = BAR

                # 写入数据库,并更新index
                MACDRestult.to_sql(name='macd', con=self.engine, if_exists="append", index=False)
                # pymysql更新后需要提交事务，避免查不到数据
                self.con.commit()
                self.UpdateTechIndex(self.session, code, dayPriceDataIndex.STARTDATE, dayPriceDataIndex.ENDDATE, 'macd')
            # 有EMA也有日线数据的情况下
            else:
                MACDIndexStartDate = MACDindex.STARTDATE
                MACDIndexEndDate = MACDindex.ENDDATE
                # 需要计算ema的区间
                MACDCalStartDate = MACDIndexEndDate + timedelta(days=1)
                MACDCalEndDate = dayPriceDataIndex.ENDDATE
                # 查询macd取出前一个值并计算
                MACDFirst = self.session.query(database.MACD).filter(database.MACD.CODE == code,database.MACD.DATE == MACDIndexEndDate).first()
                EMA12PreValue = MACDFirst.EMA12
                EMA26PreValue = MACDFirst.EMA26
                DEAPreValue=MACDFirst.DEA
                #计算各个值
                dayPriceDataStartStr = MACDCalStartDate.strftime('%Y-%m-%d')
                dayPriceDataEndStr = MACDCalEndDate.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{dayPriceDataStartStr}" and "{dayPriceDataEndStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=self.con)
                outData = outData.sort_values(by="DATE", ascending=True)
                outData.reset_index(inplace=True, drop=True)

                closeDataSeries = outData['CLOSE']

                EMA12=pd.Series(self.CalSingleEMA(list=closeDataSeries.tolist(),period=12,prevalue=EMA12PreValue))
                EMA26=pd.Series(self.CalSingleEMA(list=closeDataSeries.tolist(),period=26,prevalue=EMA26PreValue))

                DIF=EMA12-EMA26
                DEA=self.CalSingleEMA(list=DIF.tolist(),period=9,prevalue=DEAPreValue)
                BAR = 2 * (DIF - DEA)

                MACDRestult = pd.DataFrame()

                MACDRestult['DATE'] = outData['DATE']
                MACDRestult['CODE'] = code
                MACDRestult['EMA12'] = EMA12
                MACDRestult['EMA26'] = EMA26
                MACDRestult['DIF'] = DIF
                MACDRestult['DEA'] = DEA
                MACDRestult['BAR'] = BAR

                # 写入数据库,并更新index
                MACDRestult.to_sql(name='macd', con=self.engine, if_exists="append", index=False)
                # pymysql更新后需要提交事务，避免查不到数据
                self.con.commit()
                self.UpdateTechIndex(self.session, code, MACDIndexStartDate, MACDCalEndDate, 'macd')

    def UpdateTechIndex(self,session,code,startDate,endDate,techIndexType):
        queryResult = session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE==techIndexType).all()
        if len(queryResult) == 0:
            codeDateIndexbuff = database.TechDateIndex(CODE=code, STARTDATE=startDate, ENDDATE=endDate,
                                                       TECHINDEXTYPE=techIndexType)
            session.add(codeDateIndexbuff)
        else:
            session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE==techIndexType).update(
                {"STARTDATE": startDate, "ENDDATE": endDate, "TECHINDEXTYPE": techIndexType})
        session.commit()

    def CheckDataHasNan(self,dataframe):
        check_for_nan = dataframe.isnull().values.any()
        if check_for_nan==True:
            return 1
        else:
            return 0


#验证macd的计算是否准确
'''
sql = f'select * from daypricedata where CODE = "0700.HK" AND DATE between "2023-8-1" and "2023-12-29"'
outData = pd.DataFrame()
outData = pd.read_sql(text(sql), con=database.con)
outDataClose = outData['CLOSE'].tolist()
#tic=recognition.TechIndex()
outDataCloseSeries= outData['CLOSE']

#span刚好与周期数相等
ema_10 = np.array(outDataCloseSeries.ewm(span=10, min_periods=0, adjust=False, ignore_na=False).mean())
ema_10_talib= talib.EMA(np.array(outDataClose), timeperiod=10)
sql1EMA = f'select * from expma where CODE = "0700.HK" AND DATE between "2023-8-1" and "2023-12-29"'
outDataEMA = pd.read_sql(text(sql1EMA), con=database.con)
outDataEMA10=outDataEMA[outDataEMA['PERIOD']==10]
outDataEMA10value=np.array(outDataEMA10['EXPMA'])
delta=outDataEMA10value-ema_10_talib
delta2=outDataEMA10value-ema_10

ema12= np.array(outDataCloseSeries.ewm(span=12, min_periods=0, adjust=False, ignore_na=False).mean())
ema26= np.array(outDataCloseSeries.ewm(span=26, min_periods=0, adjust=False, ignore_na=False).mean())
df=pd.DataFrame()
df['MACD'],df['MACDsignal'],df['MACDhist'] = talib.MACD(np.array(outDataClose), fastperiod=6, slowperiod=12, signalperiod=9)



pass


TradeCalendar='HKEX'
ti=TechIndex(database.con,database.engine,database.session,TradeCalendar)
ti.CalSingleMACD('0700.HK')
pass
'''