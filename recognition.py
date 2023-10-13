import datetime
import time
import warnings

import numpy as np
import pandas as pd
from scipy import interpolate
from scipy.misc import derivative
import database
import stockclass
from WindPy import *
import talib
from sqlalchemy import text,create_engine, Table, Column, Integer, String, Float,Date, MetaData, ForeignKey, desc, inspect


# 这里设置判断的类，将形态判断的相关函数放在里面
class Recognition:
    resultTable = pd.DataFrame(columns=['code', 'backstepema', 'EmaDiffusion', 'EMAUpCross','MoneyFlow'])
    #resultDate = datetime.date(1970, 1, 1)
    def __init__(self):
        pass

    def RecognitionProcess(self,stocklist):
        for stock in stocklist:
            stockInProcess = stock#取出对象值
            #加入成交额的判断
            turnVolumeTresh=5000000 #500万
            lastClose=stockInProcess.dayPriceData['CLOSE'].iloc[-1]
            lastVolume=stockInProcess.dayPriceData['VOLUME'].iloc[-1]
            #成交量超过一亿才进行判断
            if lastClose*lastVolume>=turnVolumeTresh:
                dicBuff={'code':stockInProcess.code,'backstepema':0, 'EmaDiffusion':0, 'EMAUpCross':0,'MoneyFlow':0}

                #取出stocklist后，开始进行识别的操作链'
                dicBuff['backstepema']= self.EmaDiffusion(stockInProcess)
                #判断回踩
                dicBuff['EmaDiffusion'] = self.BackStepEma(stockInProcess)
                #catchBottumResult = self.CatchBottom(stockInProcess)
                dicBuff['EMAUpCross'] = self.EMAUpCross(stockInProcess)
                #判断资金流入
                dicBuff['MoneyFlow'] = self.MoneyFLow(stockInProcess)

                self.resultTable.loc[len(self.resultTable)] = dicBuff


    def EmaDiffusion(self, stock):
        print(f"开始识别均线发散：{stock.code}")
        # 根据上下关系判断，均线的顺序也是从上到下降低,5条均线
        if self.RelativeRelationofTwoLine(stock.EMAData['EMA5'], stock.EMAData['EMA10']) == '1up2':
            if self.RelativeRelationofTwoLine(stock.EMAData['EMA10'], stock.EMAData['EMA20']) == '1up2':
                if self.RelativeRelationofTwoLine(stock.EMAData['EMA20'], stock.EMAData['EMA30']) == '1up2':
                    if self.RelativeRelationofTwoLine(stock.EMAData['EMA30'], stock.EMAData['EMA60']) == '1up2':
                        print(f"{stock.code}均线发散")
                        return 1
        else:
            print(f"{stock.code}均线不发散")
            return 0

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
            # if kline[close].today/ema20.today<1.1 根据一个比例来进行判断 当天的价格
            databuff = stock.dayPriceData['CLOSE']
            closePirce = databuff.iloc[-1]  # 取最后一个
            if databuff.iloc[-2] < databuff.iloc[-1]:
                print('不符合回踩标准')
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
        # if kline[close].today/ema30.today<1.1 到底踩哪条均线，需要判断
        # 返回回踩的均线的值，返回状态是否均线回踩

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
            if stock.EMAData['EMA5'].iloc[-1] - stock.EMAData['EMA10'].iloc[-1]>0:
                print(f"{stock.code}底部5日10日均线金叉")
                return 1
            else:
                return 0

        # 伪代码
        # 当下行趋势中
        # if 资金大量进入
        # 判断方式，连续几日的资金都大量进入（大量的量化）

    def MoneyFLow(self,stock):
        cashFlowStartDate = stock.startDate
        cashFlowEndDate = stock.endDate
        statrTimeStr = cashFlowStartDate.strftime('%Y-%m-%d')
        endTimeStr = cashFlowEndDate.strftime('%Y-%m-%d')
        cashFLow=[]
        w.start()
        for i in range(0,4):
            #只获取最后一天的
            cashFLowbuff=w.wsd(stock.code, "mfd_netbuyamt", endTimeStr, endTimeStr, f"unit=1;traderType={i+1};TradingCalendar=HKEX;PriceAdj=F,ShowBlank=-1")
            cashFLowList=cashFLowbuff.Data[0]
            cashFLow.append(cashFLowList[0])
        if (cashFLow[0])>0 and (cashFLow[1])>0 and (cashFLow[2])>0:
            print('主力资金流入，可能存在抄底机会')
            return 1
        else:
            print('未见明显主力资金流入')
            return 0

#预留的以后用来计算技术指标的类
class TechIndex():
    def __init__(self):
        pass


    '''
    计算类的模块的处理逻辑，主要是为了解决之前的数据缺失的问题。需要从数据库中多取一个月数据，然后进行判断。
    整体的计算类模块的逻辑是，先获取到daypricedata日线数据，然后指标数据是少于daypricedata的，然后进行填补。
    '''
    def CalcKDJ(self,codelist,session,con):
        for code in codelist:
            print(f'计算{code}kdj')
            # code在表中存在的情况下，在表中获取startdate和enddate
            index = session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE=='KDJ').first()
            if index == None:
                #说明里面没有数据，直接取所有daypricedata进行计算，并更新index
                index = session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
                dbStartDate = index.STARTDATE
                dbEndDate = index.ENDDATE
                startDateStr = dbStartDate.strftime('%Y-%m-%d')
                endDateStr = dbEndDate.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{startDateStr}" and "{endDateStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=con)
                if outData.shape[0]<10:
                    print('数据不足，无法进行计算求取数据')
                    return 0
                # KDJ 值对应的函数是 STOCH
                KDJResult=self.CalKDJTalib(outData,code)
                KDJResult.to_sql(name='kdj', con=database.engine, schema='ztrade', if_exists="append", index=False)
                # 更新index的结果
                self.UpdateTechIndex(session,code,dbStartDate,dbEndDate,'KDJ')

            else:
                #后续考虑没有数据的情况，需要进行第一次计算以填充进数据,如果为空会影响到更新index的问题
                KDJStartDate = index.STARTDATE
                KDJEndDate = index.ENDDATE
                #取出日线数据的起始终止值
                dayPriceDataIndex = session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
                dpdStartDate=dayPriceDataIndex.STARTDATE
                dpdendDate=dayPriceDataIndex.ENDDATE
                if KDJEndDate>=dpdendDate:
                    print('已有KDJ数据，不用计算')
                    return 0

                #获取之前的多一个月的冗余数据进行
                dbStartDate=KDJEndDate-timedelta(days=30)
                dbstartDateStr = dbStartDate.strftime('%Y-%m-%d')
                dbendDateStr = dpdendDate.strftime('%Y-%m-%d')
                sql = f'select * from daypricedata where CODE = "{code}" AND DATE between "{dbstartDateStr}" and "{dbendDateStr}"'
                outData = pd.DataFrame()
                outData = pd.read_sql(text(sql), con=con)
                #在数据中定位位置
                posStart=outData.loc[outData['DATE']==KDJEndDate].index[0]
                if posStart<12:
                    print('日线数据不足，无法计算指标')
                    return 0
                #因为计算后有多余数值，确定好截取的位置
                posStartSlice = posStart+1
                #posEnd =outData.loc[outData['DATE']==dbEndDate].index[0]

                outDataBuff=outData
                KDJResult = pd.DataFrame()
                KDJResult=self.CalKDJTalib(outDataBuff,code)
                KDJResultSlice=KDJResult[posStartSlice:-1]
                KDJResultSlice.to_sql(name='kdj', con=database.engine, schema='ztrade', if_exists="append", index=False)
                # 更新index的结果
                self.UpdateTechIndex(session,code,KDJStartDate,dpdendDate,'KDJ')

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

    def UpdateTechIndex(self,session,code,startDate,endDate,techIndexType):
        queryResult = session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE==techIndexType).all()
        if len(queryResult) == 0:
            codeDateIndexbuff = database.TechDateIndex(CODE=code, STARTDATE=startDate, ENDDATE=endDate,
                                                       TECHINDEXTYPE=techIndexType)
            session.add(codeDateIndexbuff)
        else:
            session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE==techIndexType).update(
                {"STARTDATE": startDate, "ENDDATE": startDate, "TECHINDEXTYPE": techIndexType})
        session.commit()
            


