import pandas as pd
import numpy as np

##############################################进行遍历的信号判断###############################################

class Strategy():
    def SignalProcess(self,stocklist):
        for stock in stocklist:
            signalTable=pd.DataFrame()
            signalProb=pd.DataFrame(index=[0],columns=['RSIOverBuy','RSIOverSell','KDJOverBuy','KDJOverSell','KDJTopArcSignal','KDJBottomArcSignal','EMA5TopArcSignal','EMA5BottomArcSignal','MACDTopArcSignal','MACDBottomArcSignal'])
            resultArry=[]
            dateSeries=stock.dayPriceData['DATE']
            signalTable['DATE']=dateSeries
            #依次搜索信号并计算概率
            #resultArry=self.RSIOverBuySellSignal(stock, 'buy')
            #signalTable['RSIOverBuy'] =resultArry
            #signalProb['RSIOverBuy']=self.CalSignalProbability(resultArry, stock,'down')

            #resultArry = self.RSIOverBuySellSignal(stock, 'sell')
            #signalTable['RSIOverSell'] = resultArry
            #signalProb['RSIOverSell'] = self.CalSignalProbability(resultArry, stock, 'up')

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
            arr = np.zeros(5)
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
            if resultarry[i] == 1:
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