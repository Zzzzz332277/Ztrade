import pandas as pd
import numpy as np
import talib
from sqlalchemy import text
import numpy as np
import database
##############################################进行遍历的信号判断###############################################



class KLineSignal():
    def __init__(self):
        pass

    #参考talib直接传入数组
    def BigBaldRed(self,openseries,highseries,lowseries,closeseries):
        reultArray=np.zeros(len(openseries))
        for i in range(len(openseries)):
            open=openseries[i]
            high = highseries[i]
            close=closeseries[i]
            low=lowseries[i]

            bodyLen=abs(open-close)
            #阳线
            if close>=open:
                upShadowLen=abs(high-close)
                downShadowLen=abs(low-open)
                rise = bodyLen/open
            #阴线
            else:
                upShadowLen = abs(high - open)
                downShadowLen = abs(low - close)
                reultArray[i]=0
                continue
            #判断涨幅
            if rise>=0.05 and upShadowLen==0 and downShadowLen==0:
                reultArray[i]=1
            else:
                reultArray[i]=0

        return reultArray
    #底部长腿
    def LongDownLeg(self,openseries,highseries,lowseries,closeseries):
        reultArray=np.zeros(len(openseries))
        for i in range(len(openseries)):
            open=openseries[i]
            high = highseries[i]
            close=closeseries[i]
            low=lowseries[i]

            #阳线
            if close>=open:
                downShadowLen=abs(low-open)
                bodyLen=abs(close-open)
                upShadowLen=abs(high-close)
            #阴线
            else:
                downShadowLen = abs(low - close)
                bodyLen = abs(close - open)
                upShadowLen = abs(high - open)

            #判断涨幅
            if downShadowLen>=(bodyLen+upShadowLen):
                reultArray[i]=1
            else:
                reultArray[i]=0
        return reultArray

class Strategy():
    def __init__(self):
        self.signalProbTableALL = pd.DataFrame(columns=['code', 'RSIOverBuy', 'RSIOverSell', 'KDJOverBuy', 'KDJOverSell',
                                                   'KDJTopArcSignal', 'KDJBottomArcSignal', 'EMA5TopArcSignal',
                                                   'EMA5BottomArcSignal', 'MACDTopArcSignal', 'MACDBottomArcSignal'])

    def SignalProcess(self,stocklist):
        for stock in stocklist:
            print(f'开始计算{stock.code}信号成功率')
            signalTable=pd.DataFrame()
            signalProb=pd.DataFrame(index=[0],columns=['RSIOverBuy','RSIOverSell','KDJOverBuy','KDJOverSell','KDJTopArcSignal','KDJBottomArcSignal','EMA5TopArcSignal','EMA5BottomArcSignal','MACDTopArcSignal','MACDBottomArcSignal'])
            resultArry=[]
            dateSeries=stock.dayPriceData['DATE']
            signalTable['DATE']=dateSeries
            #依次搜索信号并计算概率
            resultArry=self.RSIOverBuySellSignal(stock, 'buy')
            signalTable['RSIOverBuy'] =resultArry
            signalProb['RSIOverBuy']=self.CalSignalProbability(resultArry, stock,'down')

            resultArry = self.RSIOverBuySellSignal(stock, 'sell')
            signalTable['RSIOverSell'] = resultArry
            signalProb['RSIOverSell'] = self.CalSignalProbability(resultArry, stock, 'up')

            resultArry = self.KDJOverBuySellSignal(stock, 'buy')
            signalTable['KDJOverBuy'] = resultArry
            signalProb['KDJOverBuy'] = self.CalSignalProbability(resultArry, stock, 'down')

            resultArry = self.KDJOverBuySellSignal(stock,'sell')
            signalTable['KDJOverSell'] = resultArry
            signalProb['KDJOverSell'] = self.CalSignalProbability(resultArry, stock, 'up')

            resultArry = self.KDJArcSignal(stock, 'top')
            signalTable['KDJTopArcSignal'] = resultArry
            signalProb['KDJTopArcSignal'] = self.CalSignalProbability(resultArry, stock, 'down')

            resultArry = self.KDJArcSignal(stock, 'bottom')
            signalTable['KDJBottomArcSignal'] = resultArry
            signalProb['KDJBottomArcSignal'] = self.CalSignalProbability(resultArry, stock, 'up')

            resultArry = self.EMA5ArcSignal(stock, 'top')
            signalTable['EMA5TopArcSignal'] = resultArry
            signalProb['EMA5TopArcSignal'] = self.CalSignalProbability(resultArry, stock, 'down')

            resultArry = self.EMA5ArcSignal(stock, 'bottom')
            signalTable['EMA5BottomArcSignal'] = resultArry
            signalProb['EMA5BottomArcSignal'] = self.CalSignalProbability(resultArry, stock, 'up')

            resultArry = self.MACDArcSignal(stock, 'top')
            signalTable['MACDTopArcSignal'] = resultArry
            signalProb['MACDTopArcSignal'] = self.CalSignalProbability(resultArry, stock, 'down')

            resultArry = self.MACDArcSignal(stock, 'bottom')
            signalTable['MACDBottomArcSignal'] = resultArry
            signalProb['MACDBottomArcSignal'] = self.CalSignalProbability(resultArry, stock, 'up')

            #统计各个信号的成功率
            stock.signalTable=signalTable
            stock.signalSucessProb=signalProb
            signalProb.insert(0,'code',stock.code)
            #结果写入总的表中
            self.signalProbTableALL = pd.concat([self.signalProbTableALL,signalProb])
            pass

    def WriteToExcel(self):
        #writer = pd.ExcelWriter('test.xlsx')
        print('开始写入数据')
        self.signalProbTableALL.to_excel('Analysis.xlsx', sheet_name="data", index=False, na_rep=0, inf_rep=0)

        #self.signalProbTableALL.to_excel(writer, sheet_name='data')
        # 最后保存
        #writer.save()
        #writer.close()

    def LoadExcel(self,stocklist):
        ProbDF = pd.read_csv("D:\ztrade\Analysis2020.1.1-2024.3.15_us.csv")
        # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
        #将读取到的table分配给各个stock
        pass
        for stock in stocklist:
            stock.signalSucessProb=ProbDF[ProbDF['code']==stock.code]


    def RSIOverBuySellSignal(self,stock,type):
        rsi = stock.RSIData
        resultArry = np.zeros(rsi.shape[0])
        for i in range(rsi.shape[0]):
            RSI6 = rsi['RSI6'].iloc[i]
            if type=='sell':
                if RSI6 < 15:
                    resultArry[i] = 1
            if type=='buy':
                if RSI6 > 85:
                    resultArry[i] = 1
        # 找出连续1的起始位置
        posArry = resultArry.copy()
        for i in range(1, resultArry.shape[0]):
            if resultArry[i] == 1 and resultArry[i - 1] == 1:
                posArry[i] = 0

        return posArry



    def KDJOverBuySellSignal(self,stock,type):
        kdj = stock.KDJData
        resultArry = np.zeros(kdj.shape[0])
        for i in range(kdj.shape[0]):
            J = kdj['J'].iloc[i]
            if type == 'sell':
                if J < 10:
                    resultArry[i] = 1
            if type=='buy':
                if J > 90:
                    resultArry[i] = 1
        #找出连续1的起始位置
        posArry = resultArry.copy()
        for i in range(1,resultArry.shape[0]):
            if resultArry[i]==1 and resultArry[i-1]==1:
                posArry[i]=0

        return posArry


    def KDJUpcrossSignal(self, stock):
        kdj = stock.KDJData
        ResultArry = np.zeros(kdj.shape[0])
        for i in range(kdj.shape[0]):
            # 判断上穿
            if kdj['J'].iloc[i - 1] <= kdj['K'].iloc[i - 1] and kdj['J'].iloc[i] > kdj['K'].iloc[i]:
                ResultArry[i] = 1
        return ResultArry

        # EMA均线的圆形底

    def KDJArcSignal(self,stock,type):
        kdj = stock.KDJData
        ResultArry = np.zeros(kdj.shape[0])
        for i in range(2,kdj.shape[0]):
            # 必须下半部分
            if type == 'bottom':
                if kdj['J'].iloc[i - 2] <50 and kdj['J'].iloc[i-1] <50 and kdj['J'].iloc[i] <50:
                    if kdj['J'].iloc[i - 2] > kdj['J'].iloc[i-1] and kdj['J'].iloc[i] > kdj['J'].iloc[i-1]:
                        ResultArry[i] = 1
            if type=='top':
                if kdj['J'].iloc[i - 2] > 50 and kdj['J'].iloc[i - 1] > 50 and kdj['J'].iloc[i] > 50:
                    if kdj['J'].iloc[i - 2] < kdj['J'].iloc[i - 1] and kdj['J'].iloc[i] < kdj['J'].iloc[i - 1]:
                        ResultArry[i] = 1
        return ResultArry


    def EMA5ArcSignal(self, stock,type):
        ema = stock.EMAData
        ResultArry = np.zeros(ema.shape[0])
        # 需要考虑两边的情况，后续是看3日后的值，所以边界-3
        for i in range(2, ema.shape[0] - 3):
            if type == 'bottom':
                arr = np.zeros(5)
                for j in range(0, 5):
                    # 取最后5个
                    arr[j] = ema['EMA5'].iloc[i - 2 + j]
                posMin = np.argmin(arr)
                if posMin == 3:
                    if arr[1] < arr[0] and arr[2] < arr[1]:
                        ResultArry[i + 2] = 1
            if type == 'top':
                arr = np.zeros(5)
                for j in range(0, 5):
                    # 取最后5个
                    arr[j] = ema['EMA5'].iloc[i - 2 + j]
                posMax = np.argmax(arr)
                if posMax == 3:
                    if arr[1] > arr[0] and arr[2] > arr[1]:
                        ResultArry[i + 2] = 1
        return ResultArry

    def MACDArcSignal(self, stock,type):
        macd = stock.MACDData
        resultArry = np.zeros(macd.shape[0])
        # 需要考虑两边的情况，后续是看3日后的值，所以边界-3
        for i in range(1, macd.shape[0] - 1):
            arr = np.zeros(3)
            for j in range(0, 3):
                # 取最后5个
                arr[j] = macd['BAR'].iloc[i - 1 + j]
            if type=='bottom':
                posMin = np.argmin(arr)
            #3个值都小于0
                if posMin == 1 and (np.array(arr)<0).any():
                        resultArry[i] = 1
            if type=='top':
                posMin = np.argmax(arr)
                # 3个值都>于0
                if posMin == 1 and (np.array(arr) > 0).any():
                    resultArry[i] = 1
        posArry = resultArry.copy()
        for i in range(1, resultArry.shape[0]):
            if resultArry[i] == 1 and resultArry[i - 1] == 1:
                posArry[i] = 0

        return posArry



    def CalSignalProbability(self, resultarry, stock,direction):
        success = 0
        signalCount = 0
        dayPriceData = stock.dayPriceData

        for i in range(len(resultarry) - 3):
            if resultarry[i] >= 1:
                # 取三天后的价格判断
                signalCount = signalCount + 1
                pricePre = dayPriceData['CLOSE'].iloc[i]
                priceAfter = dayPriceData['CLOSE'].iloc[i + 3]
                if direction=='up':
                    if priceAfter > pricePre:
                        success = success + 1
                if direction=='down':
                    if priceAfter < pricePre:
                        success = success + 1
                    else:
                        failDate=dayPriceData['DATE'].iloc[i]
                        #print(f'失败日期{failDate}')

        if signalCount != 0:
            SignalProb = round(success / signalCount,3)

            return SignalProb
        else:
            return 99
    #只用daypricedata
    def CalSignalProbabilityOnlyData(self, resultarry, daypricedata,direction):
        success = 0
        signalCount = 0
        dayPriceData = daypricedata

        for i in range(len(resultarry) - 3):
            #为了适配talib,加入大于1的判断
            if resultarry[i] >= 1:
                # 取三天后的价格判断
                signalCount = signalCount + 1
                pricePre = dayPriceData['CLOSE'].iloc[i]
                priceAfter = dayPriceData['CLOSE'].iloc[i + 3]
                if direction=='up':
                    if priceAfter > pricePre:
                        success = success + 1
                if direction=='down':
                    if priceAfter < pricePre:
                        success = success + 1
                    else:
                        failDate=dayPriceData['DATE'].iloc[i]
                        #print(f'失败日期{failDate}')

        if signalCount != 0:
            SignalProb = round(success / signalCount,3)

            return SignalProb
        else:
            return 99


    def EMA5up10Strategy(self, stock):
        code = stock.code
        success = 0
        signalCount = 0
        accumlateReturn = 0
        buyFlag = 0
        buyPrice = 0
        sellPrice = 0
        initCash = 10000
        cash = 10000
        dayPriceData = stock.dayPriceData
        ema = stock.EMAData
        pos = 0
        for i in range(1, dayPriceData.shape[0]):
            if ema['EMA5'].iloc[i - 1] <= ema['EMA10'].iloc[i - 1] and ema['EMA5'].iloc[i] > ema['EMA10'].iloc[i]:
                # 上穿，买入
                buyFlag = 1
                signalCount = signalCount + 1
                # 记录买点位置
                pos = i
                buyDate = dayPriceData['DATE'].iloc[i]
                # buyPrice=dayPriceData['CLOSE'].iloc[i]
                # 按照上下穿均线价格买入卖出
                buyPrice = ema['EMA5'].iloc[i]
                print(f'{code}买入日期：{buyDate},买入价格{buyPrice}')
            elif ema['EMA5'].iloc[i - 1] >= ema['EMA10'].iloc[i - 1] and ema['EMA5'].iloc[i] < ema['EMA10'].iloc[i]:
                # 下穿，卖出
                if buyFlag == 1:
                    buyFlag = 0
                    sellDate = dayPriceData['DATE'].iloc[i]
                    # sellPrice=dayPriceData['CLOSE'].iloc[i]
                    # 按照上下穿均线价格买入卖出
                    sellPrice = ema['EMA5'].iloc[i]
                    gain = (sellPrice - buyPrice) / buyPrice
                    # 按复利来算
                    cash = cash * (1 + gain)
                    accumlateReturn = accumlateReturn + gain

                    if gain > 0:
                        success = success + 1
                    print(f'{code}卖出日期：{sellDate}，本次收益率{gain},卖出价格{sellPrice}')

            else:
                pass
        if signalCount != 0:
            SignalProb = success / signalCount
        cashReturn = cash / initCash
        return SignalProb, accumlateReturn, cashReturn


    def BBIStrategy(self, stock):
        code = stock.code
        success = 0
        signalCount = 0
        accumlateReturn = 0
        buyFlag = 0
        buyPrice = 0
        sellPrice = 0
        initCash = 10000
        cash = 10000
        fee = 0.0002
        dayPriceData = stock.dayPriceData
        bbi = stock.BBIData
        pos = 0
        for i in range(1, dayPriceData.shape[0]):

            if bbi['BBI'].iloc[i - 1] >= dayPriceData['CLOSE'].iloc[i - 1] and bbi['BBI'].iloc[i] < \
                    dayPriceData['CLOSE'].iloc[i]:
                # 上穿，买入
                buyFlag = 1
                signalCount = signalCount + 1
                # 记录买点位置
                pos = i
                buyDate = dayPriceData['DATE'].iloc[i]
                #buyPrice=dayPriceData['CLOSE'].iloc[i]
                # 按照上下穿均线价格买入卖出
                buyPrice = bbi['BBI'].iloc[i]
                print(f'{code}买入日期：{buyDate},买入价格{buyPrice}')
            elif bbi['BBI'].iloc[i - 1] <= dayPriceData['CLOSE'].iloc[i - 1] and bbi['BBI'].iloc[i] > \
                    dayPriceData['CLOSE'].iloc[i]:
                # 下穿，卖出
                if buyFlag == 1:
                    buyFlag = 0
                    sellDate = dayPriceData['DATE'].iloc[i]
                    #sellPrice=dayPriceData['CLOSE'].iloc[i]
                    # 按照上下穿均线价格买入卖出
                    sellPrice = bbi['BBI'].iloc[i]
                    gain = ((sellPrice - buyPrice) / buyPrice) - 1*fee
                    # 按复利来算
                    cash = cash * (1 + gain)
                    accumlateReturn = accumlateReturn + gain

                    if gain > 0:
                        success = success + 1
                    print(f'{code}卖出日期：{sellDate},卖出价格{sellPrice}',end='')
                    print('本次收益率为{:.2%}'.format(gain))

            else:
                pass
        if signalCount != 0:
            SignalProb = success / signalCount
        cashReturn = cash / initCash
        return SignalProb, accumlateReturn, cashReturn

'''
sql = f'select * from daypricedata where CODE = "NVDA.O" AND DATE between "2020-2-1" and "2024-3-17"'
outData = pd.DataFrame()
outData = pd.read_sql(text(sql), con=database.conUS)
outDataClose = outData['CLOSE'].tolist()
output=np.zeros(outData.shape[0])
outData = outData.sort_values(by="DATE", ascending=True)
#for i in range(0,outData.shape[0]):
#outputArray = talib.CDLHIGHWAVE(outData['OPEN'], outData['HIGH'], outData['LOW'], outData['CLOSE'])
#outData['RESULT']=outputArray
stg=Strategy()
kln=KLineSignal()
outputArray=kln.LongDownLeg(outData['OPEN'], outData['HIGH'], outData['LOW'], outData['CLOSE'])


result=stg.CalSignalProbabilityOnlyData(outputArray,outData,'up')
outData['result']=outputArray
pass
'''