from zfutu import *
import pandas as pd
import numpy as np
import basic
import talib

class BackTest():
    def __int__(self):
        pass

    def GetMinuteData(self,code,start,end):
        statrTimeStr = start.strftime("%Y-%m-%d")
        endTimeStr = end.strftime("%Y-%m-%d")
        ret,data,page_req_key=quote_ctx.request_history_kline(code, start=statrTimeStr, end=endTimeStr, ktype=KLType.K_1M, autype=AuType.QFQ, max_count=1000, extended_time=False)
        resultDataFrame=pd.DataFrame(columns=['code','name','time_key','open','close','high','low','volume'])
        pageNum=1
        if ret == RET_OK:
            print('获取到数据')
            resultDataFrame = pd.concat([resultDataFrame, data])
            pageNum=pageNum+1
        else:
            print('error:', data)
        while page_req_key != None:  # 请求后面的所有结果
            ret, data, page_req_key = quote_ctx.request_history_kline(code, start=statrTimeStr, end=endTimeStr, ktype=KLType.K_1M, autype=AuType.QFQ, max_count=1000, page_req_key=page_req_key) # 请求翻页后的数据
            if ret == RET_OK:
                print(f'获取第{pageNum}页数据')
                resultDataFrame = pd.concat([resultDataFrame, data])
                pageNum = pageNum + 1

            else:
                print('error:', data)
        print('All pages are finished!')
        return resultDataFrame
        pass

    def CalMinuteEMA(self,data):
        print('计算 EMA')
        emaPreValueDict = {}  # 用来存放各个周期的第一个值的字典
        data = data.sort_values(by="time_key", ascending=True)
        closeDataSeries = data['close']
        # 这里其实应该放在for里面更新，否则在像计算RSI这样周期不相等的数据时就会出问题
        EMACalResult = pd.DataFrame()
        concatDataframe = pd.DataFrame(columns=["EXPMA", "DATE", "CODE", "PERIOD"])
        for period in basic.emaPeriod:
            print(f"获取{period}ema数据")

            # 这里求出的ema6和ema12在数据够多的情况下才准确，前部不准
            EMABuff = closeDataSeries.ewm(span=period, min_periods=0, adjust=False, ignore_na=False).mean()
            EMACalResultList = EMABuff.tolist()
            EMACalResult['time_key'] = data['time_key']
            EMACalResult['code'] = data['code']
            EMACalResult['PERIOD'] = period
            EMACalResult['EXPMA'] = EMACalResultList
            concatDataframe = pd.concat([concatDataframe, EMACalResult])
            # 计算后连接到大的dataframe
        return concatDataframe

    def CalMinuteMA(self, data):
        print('计算 MA')
        data = data.sort_values(by="time_key", ascending=True)
        closeDataSeries = data['close']
        # 这里其实应该放在for里面更新，否则在像计算RSI这样周期不相等的数据时就会出问题
        MACalResult = pd.DataFrame()
        concatDataframe = pd.DataFrame(columns=["MA", "DATE", "CODE", "PERIOD"])
        for period in basic.maPeriod:
            print(f"计算{period}MA数据")
            MABuff = talib.MA(closeDataSeries,period)
            #需要前部的NAN值替换掉
            MABuff=MABuff.fillna(closeDataSeries.iloc[period-1])
            MACalResultList = MABuff.tolist()
            MACalResult['DATE'] = data['time_key']
            MACalResult['code'] = data['code']
            MACalResult['PERIOD'] = period
            MACalResult['MA'] = MACalResultList
            concatDataframe = pd.concat([concatDataframe, MACalResult])
            # 计算后连接到大的dataframe
        # 写入数据库,并更新index
        return concatDataframe

    def CalMinuteBBI(self, data):
        pass

def BBIStrategyMinute( data,bbi):
    code = data['code'].iloc[0]
    success = 0
    signalCount = 0
    accumlateReturn = 0
    buyFlag = 0
    buyPrice = 0
    sellPrice = 0
    initCash = 10000
    cash = 10000
    fee = 0.0002
    minutePriceData = data
    bbi = bbi
    tradeTimes=0
    #这里时间轴一定要对齐，内涵逻辑是有分钟必有日线，
    data['bbi_day']=0
    #先将BBI日线数据赋值给分钟数据
    for i in range(0, minutePriceData.shape[0]-1):
        #将时间进行转化
        print(f'bbi对齐第{i+1}条数据')
        dateThisMinuteStr=data['time_key'].iloc[i]
        dateThisMinute = datetime.strptime(dateThisMinuteStr, '%Y-%m-%d %H:%M:%S')
        dateThisDay=dateThisMinute.date()
        dateThisDayStr=str(dateThisDay)
        #查找到对应的当天的bbi
        BBITThisDay=bbi.loc[bbi['DATE'] == dateThisDay].copy()
        data['bbi_day'].iloc[i]= BBITThisDay['BBI'].iloc[0]

    for i in range(1, minutePriceData.shape[0]):
        if data['bbi_day'].iloc[i - 1] > data['close'].iloc[i - 1] and data['bbi_day'].iloc[i] < data['close'].iloc[i]:
            # 上穿，买入
            buyFlag = 1
            signalCount = signalCount + 1
            # 记录买点位置
            pos = i
            buyDateTime = data['time_key'].iloc[i]
            #buyPrice=dayPriceData['CLOSE'].iloc[i]
            # 按照上下穿均线价格买入卖出
            buyPrice = data['close'].iloc[i]
            print(f'{code}买入时间：{buyDateTime},买入价格{buyPrice}')
        elif data['bbi_day'].iloc[i - 1] < data['close'].iloc[i - 1] and data['bbi_day'].iloc[i] > data['close'].iloc[i]:

            # 下穿，卖出
            if buyFlag == 1:
                buyFlag = 0
                tradeTimes=tradeTimes+1
                sellDateTime = data['time_key'].iloc[i]
                #sellPrice=dayPriceData['CLOSE'].iloc[i]
                # 按照上下穿均线价格买入卖出
                sellPrice = data['close'].iloc[i]
                gain = ((sellPrice - buyPrice) / buyPrice) - 2*fee
                # 按复利来算
                cash = cash * (1 + gain)
                accumlateReturn = accumlateReturn + gain

                if gain > 0:
                    success = success + 1
                print(f'{code}卖出日期：{sellDateTime},卖出价格{sellPrice}',end='')
                print('本次收益率为{:.2%}'.format(gain))

        else:
            pass
    if signalCount != 0:
        SignalProb = success / signalCount
    cashReturn = cash / initCash
    return SignalProb, accumlateReturn, cashReturn,tradeTimes