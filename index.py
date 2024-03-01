#用于获取指数，特别是A股指数数据，以及进行处理的类
import database
import crawler
import basic
import recognition
from WindPy import *
import myexception
import sqlalchemy
import pandas as pd
import numpy as np
from sqlalchemy import text,create_engine, Table, Column, Integer, String, Float,Date, MetaData, ForeignKey, desc, inspect
import futu as ft
import zfutu
from futu import *


#趋势类
class Trend:
    def __init__(self, start, end, len, direction):
        self.StartTime = start
        self.EndTime = end
        self.Length = len
        self.Direction = direction

# 适用于wind数据的candle
class IndexClass:
     #不接受输入的初始化函数
    def __init__(self, code, dayPriceDataFrame,emaData,macdeData,rsiData,kdjData,market):  # 从数据库中取出的是一个dataframe
        self.code = code
        self.trendList = [Trend]
        # 直接在这里完成stock的转置和处理
        self.dayPriceData = dayPriceDataFrame
        self.EMAData = emaData
        self.MACDData=macdeData
        self.RSIData=rsiData
        self.KDJData=kdjData
        #############################获取时间########################
        self.startDate = self.dayPriceData['DATE'].iloc[0]
        self.endDate = self.dayPriceData['DATE'].iloc[-1]
        self.market=market
        self.totalCashFlowIn = 0
        self.superCashFlowIn = 0
        self.bigCashFlowIn = 0

    @classmethod
    def USIndexInit(self, code, dayPriceDataFrame, market):
        return self(code=code,dayPriceDataFrame=dayPriceDataFrame,market=market,emaData=None,macdeData=None,rsiData=None,kdjData=None)
        '''
        self.code = code
        self.dayPriceData = dayPriceDataFrame
        self.startDate = self.dayPriceData['DATE'].iloc[0]
        self.endDate = self.dayPriceData['DATE'].iloc[-1]
        self.market = market
        pass
        '''
    '''
    def __init__(self, code, dayPriceDataFrame,market):  # 从数据库中取出的是一个dataframe
        self.code = code
        self.trendList = [Trend]
        # 直接在这里完成stock的转置和处理
        self.dayPriceData = dayPriceDataFrame
        #############################获取时间########################
        self.startDate = self.dayPriceData['DATE'].iloc[0]
        self.endDate = self.dayPriceData['DATE'].iloc[-1]
        self.market=market
        self.totalCashFlowIn = 0
        self.superCashFlowIn = 0
        self.bigCashFlowIn = 0
    '''
#专门对A股指数进行处理
class Aindex:
    indexCodes=['SH.000001','SZ.399001']
    def __init__(self, con, engine, session):
        self.con = con
        self.engine = engine
        self.session = session


    def SyncIndexData(self):
        indexDB=database.GetWindDaTA(self.con,self.engine,self.session)
        pass
  #获取给定单一code的数据，成功返回1，失败返回0,修改为从futu获取数据
    def UpdateTimePeriodIndexDataSingle(self, code, startDate, endDate, tableName):
        statrTimeStr = startDate.strftime("%Y-%m-%d")
        endTimeStr = endDate.strftime("%Y-%m-%d")
        ret, data, page_req_key = zfutu.quote_ctx.request_history_kline(code, start=statrTimeStr, end=endTimeStr,max_count=1000, page_req_key=None)
        if ret == RET_OK:
            if data.empty:
                # 抛出无数据的异常
                # raise myexception.ExceptionFutuNoData('Futu: No data.')
                return 0, 0, 0
            buffData = pd.DataFrame()
            timeStrSeries = data['time_key']
            timeDateList = []
            for timeStr in timeStrSeries:
                timeDatetime = datetime.strptime(timeStr, '%Y-%m-%d %H:%M:%S')  # strptime()内参数必须为string格式
                timeDate = datetime.date(timeDatetime)
                timeDateList.append(timeDate)

            buffData['OPEN'] = data['open']
            buffData['CLOSE'] = data['close']
            buffData['HIGH'] = data['high']
            buffData['LOW'] = data['low']
            buffData['VOLUME'] = data['volume']
            buffData['DATE'] = timeDateList
            buffData['CODE'] = code
        else:
            print('error:', data)
        print(f"获取{code}的日线数据")

        # 检查数据是否有nan，防止nan进入数据库
        if self.CheckDataHasNan(buffData):
            print('数据中含有nan，无法使用')
            return 0
        buffData.to_sql(name=tableName, con=self.engine, if_exists="append", index=False)
        self.con.commit()
        # 30秒内最多请求60次，睡眠0.5S
        time.sleep(0.5)

        # 获取data的最后一个日期，作为codedateindex日期
        lastDate = buffData['DATE'].iloc[-1]
        firstDate = buffData['DATE'].iloc[0]
        return 1, firstDate, lastDate

    def UpdateTimePeriodIndexDataEMA(self, code, startDate, endDate,tableName):
        concatDataframe = pd.DataFrame(columns=["EXPMA","DATE","CODE","PERIOD"])
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()

        for period in basic.emaPeriod:
            buffData = w.wsd(code, "EXPMA", statrTimeStr, endTimeStr, f"EXPMA_N={period};PriceAdj=F")
            # 这里取出的是没有时间的。
            print(f"获取{code}的{period}ema数据")
            data = pd.DataFrame(buffData.Data, index=buffData.Fields)
            data = data.T
            # 检查数据是否有nan，防止nan进入数据库
            if self.CheckDataHasNan(data):
                print('数据中含有nan，无法使用')
                continue
            data['DATE'] = buffData.Times
            data['CODE'] = code
            data['PERIOD'] = period
            concatDataframe=pd.concat([concatDataframe,data])

        #全部一次性写入
        print("开始写入数据库")
        concatDataframe.to_sql(name=tableName, con=self.engine,if_exists="append",index=False)
        self.con.commit()
        return 1

    #直接从wind获取kdj指标
    def UpdateTimePeriodIndexDataKDJ(self, code, startDate, endDate, tableName):
        # windkdj分开取，用参数123
        KDJType={'1':'K','2':'D','3':'J'}
        resultDataFrame = pd.DataFrame(columns=["DATE", "CODE", "K","D","J"])
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()

        for key in KDJType:
            buffData = w.wsd(code, "KDJ", statrTimeStr, endTimeStr,
                             f"KDJ_N=9;KDJ_M1=3;KDJ_M2=3;KDJ_IO={key};PriceAdj=F")
            # 这里取出的是没有时间的。
            print(f"获取{code}的KDJ的{key}数据")
            data = pd.DataFrame(buffData.Data, index=buffData.Fields)
            data = data.T
            # 检查数据是否有nan，防止nan进入数据库
            if self.CheckDataHasNan(data):
                print('数据中含有nan，无法使用')
                continue
            resultDataFrame['DATE'] = buffData.Times
            resultDataFrame['CODE'] = code
            resultDataFrame[KDJType[key]] = data

        # 全部一次性写入
        print("开始写入数据库")
        resultDataFrame.to_sql(name=tableName, con=self.engine, if_exists="append", index=False)
        self.con.commit()
        return 1

    #从wind获取rsi并写入数据库
    def UpdateTimePeriodIndexDataSingleRSI(self, code, startDate, endDate,tableName):
        concatDataframe = pd.DataFrame(columns=["DATE", "CODE", 'RSI',"PERIOD"])
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()

        for period in basic.rsiPeriod:
            buffData = w.wsd(code, "RSI", statrTimeStr, endTimeStr,f"RSI_N={period};PriceAdj=F")
            # 这里取出的是没有时间的。
            print(f"获取{code}的{period}RSI数据")
            data = pd.DataFrame(buffData.Data, index=buffData.Fields)
            data = data.T
            # 检查数据是否有nan，防止nan进入数据库
            if self.CheckDataHasNan(data):
                print('数据中含有nan，无法使用')
                return 0
            data['DATE'] = buffData.Times
            data['CODE'] = code
            data['PERIOD'] = period
            concatDataframe = pd.concat([concatDataframe, data])

        # 返回到dataframe中
        #全部一次性写入
        print("开始写入数据库")
        concatDataframe.to_sql(name=tableName, con=self.engine,if_exists="append",index=False)
        self.con.commit()
        return 1

    # 从wind获取macd并写入数据库
    def UpdateTimePeriodIndexDataSingleMACD(self, code, startDate, endDate, tableName):
        concatDataframe = pd.DataFrame(columns=["DATE", "CODE", 'MACD', "DIFF","DEA"])
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()
        macdType={'DIFF':'1','DEA':'2','MACD':'3'}
        outData=pd.DataFrame()
        for key in macdType:
            buffData = w.wsd(code, "MACD", statrTimeStr, endTimeStr,
                  f"MACD_L=26;MACD_S=12;MACD_N=9;MACD_IO={macdType[key]};PriceAdj=F")

            # 这里取出的是没有时间的。
            print(f"获取{code} MACD的{macdType[key]}数据")
            data = pd.DataFrame(buffData.Data, index=buffData.Fields)
            data = data.T
            # 检查数据是否有nan，防止nan进入数据库
            if self.CheckDataHasNan(data):
                print('数据中含有nan，无法使用')
                return 0
            #循环三次获取三个值
            outData[key] = data['MACD']

        outData['DATE'] = buffData.Times
        outData['CODE'] = code

        # 返回到dataframe中
        # 全部一次性写入
        print("开始写入数据库")
        outData.to_sql(name=tableName, con=self.engine, if_exists="append", index=False)
        self.con.commit()
        return 1


    #同步数据，这里通过传入func回调函数的方法调用上面的函数，针对wind的改动，该函数暂时弃用
    def SyncDataBase(self, codeList, startdate, enddate,func,type):
        #通过type确定是哪个表和哪个指标,这里需要对type的类型进行确定
        oneDay = timedelta(days=1)
        # 需要将单个的code转化为codelist，满足update函数的运行条件
        for code in codeList:
            #分辨是日线还是技术指标
            if type=='daypricedata':
                codeResult = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).all()
            else:
                codeResult = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE == type).all()

            if len(codeResult) == 0:
                print(f'表中无该{code}数据,开始获取新数据')
                try:
                    result = func(code=code, startDate=startdate, endDate=enddate,tableName=type)
                except myexception.ExceptionWindNoData as e:
                    print("非交易日，无数据")
                    continue
                if result:
                    if type == 'daypricedata':
                        self.UpdateCodeIndex(code, startdate, enddate, self.session)
                    else:
                        self.UpdateTechIndex(code, startdate, enddate, self.session, techIndexType=type)
                continue

            # code在表中存在的情况下，在表中获取startdate和enddate
            if type == 'daypricedata':
                index = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
            else:
                index = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE == type).first()

            dbStartDate = index.STARTDATE
            dbEndDate = index.ENDDATE
            # 还有一种情况是需要获取的数据范围包裹住了数据库内的数据范围
            if startdate < dbStartDate and enddate > dbEndDate:
                print('前后方数据都需要补充')
                # 补充前方数据并更新前方index
                startdatebuff = startdate
                endDatebuff = dbStartDate - oneDay
                result = func(code, startdatebuff, endDatebuff, type)
                if result:
                    if type == 'daypricedata':
                        self.UpdateCodeIndex(code, startdate, dbEndDate, self.session)
                    else:
                        self.UpdateTechIndex(code, startdate, dbEndDate, self.session, techIndexType=type)
                # 补充后方数据并更新后方index
                startdatebuff = dbEndDate + oneDay
                endDatebuff = enddate
                result = func(code, startdatebuff, endDatebuff, type)
                # 这里边界以startdate和enddate为准
                if result:
                    if type == 'daypricedata':
                        self.UpdateCodeIndex(code, startdate, enddate, self.session)
                    else:
                        self.UpdateTechIndex(code, startdate, enddate, self.session, techIndexType=type)
                continue

            if (startdate >= dbEndDate or enddate <= dbStartDate) and startdate != enddate:
                # 说明数据库中的时间范围和目标时间范围不重叠，中间的空隙需要补上，否则整个逻辑会失效
                if startdate >= dbEndDate:
                    print('数据在后方有空隙，进行填补')
                    startdatebuff = dbEndDate + oneDay
                    endDatebuff = enddate
                    dbStartDateBuff = dbStartDate
                    dbEndDateBuff = endDatebuff
                else:
                    print('数据在前方有空隙，进行填补')
                    startdatebuff = startdate
                    endDatebuff = dbStartDate - oneDay
                    dbStartDateBuff = startdatebuff
                    dbEndDateBuff = dbEndDate
            else:
                if startdate >= dbStartDate and enddate <= dbEndDate:
                    # 说明日期在包裹之内，不用重新获取
                    print('已有数据，不用获取')
                    continue
                else:
                    if startdate >= dbStartDate and enddate > dbEndDate:
                        print('补充后方数据')
                        # 说明只需要获取后面一部分的数据
                        startdatebuff = dbEndDate + oneDay
                        endDatebuff = enddate
                        dbStartDateBuff = dbStartDate
                        dbEndDateBuff = endDatebuff
                    else:
                        if startdate < dbStartDate and enddate <= dbEndDate:
                            print('补充前方数据')
                            startdatebuff = startdate
                            endDatebuff = dbStartDate - oneDay
                            dbStartDateBuff = startdatebuff
                            dbEndDateBuff = dbEndDate
            # 根据日期情况更新数据
            try:
                result = func(code, startdatebuff, endDatebuff, type)
            except myexception.ExceptionWindNoData as e:
                print("非交易日，无数据")
                continue
            if result:
                if result:
                    if type == 'daypricedata':
                        self.UpdateCodeIndex(code, dbStartDateBuff, dbEndDateBuff, self.session)
                    else:
                        self.UpdateTechIndex(code, dbStartDateBuff, dbEndDateBuff, self.session, techIndexType=type)

    def SyncDataBaseDayPirceDataUpdate(self, codeList, startdate, enddate, tablename):
        oneDay = timedelta(days=1)
        # 需要将单个的code转化为codelist，满足update函数的运行条件
        for code in codeList:
            codeResult = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).all()
            if len(codeResult) == 0:
                print(f'表中无该{code}数据,开始获取新数据')
                try:
                    result,firstDate,lastDate = self.UpdateTimePeriodIndexDataSingle(code=code, startDate=startdate, endDate=enddate,tableName='daypricedata')
                except myexception.ExceptionFutuNoData as e:
                    print("无数据")
                    continue
                if result: self.UpdateCodeIndex(code, firstDate, lastDate, self.session)
                continue

            # code在表中存在的情况下，在表中获取startdate和enddate
            index = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
            dbStartDate = index.STARTDATE
            dbEndDate = index.ENDDATE
            # 还有一种情况是需要获取的数据范围包裹住了数据库内的数据范围
            if startdate < dbStartDate and enddate > dbEndDate:
                print('前后方数据都需要补充')
                # 补充前方数据并更新前方index
                startdatebuff = startdate
                endDatebuff = dbStartDate - oneDay
                result, firstDateFront, lastDateFront = self.UpdateTimePeriodIndexDataSingle(code, startdatebuff, endDatebuff, 'daypricedata')
                if result: self.UpdateCodeIndex(code, firstDateFront, dbEndDate, self.session)

                # 补充后方数据并更新后方index
                startdatebuff = dbEndDate + oneDay
                endDatebuff = enddate
                result, firstDateBack, lastDateBack  = self.UpdateTimePeriodIndexDataSingle(code, startdatebuff, endDatebuff, 'daypricedata')
                # 这里边界以startdate和enddate为准
                if result: self.UpdateCodeIndex(code, firstDateFront, lastDateBack, self.session)
                continue

            if (startdate >= dbEndDate or enddate <= dbStartDate) and startdate != enddate:
                # 说明数据库中的时间范围和目标时间范围不重叠，中间的空隙需要补上，否则整个逻辑会失效
                if startdate >= dbEndDate:
                    print('数据在后方有空隙，进行填补')
                    startdatebuff = dbEndDate + oneDay
                    endDatebuff = enddate
                    dbStartDateBuff = dbStartDate
                    dbEndDateBuff = endDatebuff
                    result, firstDate, lastDate = self.UpdateTimePeriodIndexDataSingle(code, startdatebuff,endDatebuff, 'daypricedata')
                    # 这里边界以startdate和enddate为准
                    if result: self.UpdateCodeIndex(code, dbStartDate, lastDate, self.session)
                    continue
                else:
                    print('数据在前方有空隙，进行填补')
                    startdatebuff = startdate
                    endDatebuff = dbStartDate - oneDay
                    dbStartDateBuff = startdatebuff
                    dbEndDateBuff = dbEndDate
                    result, firstDate, lastDate = self.UpdateTimePeriodIndexDataSingle(code, startdatebuff, endDatebuff,'daypricedata')
                    # 这里边界以startdate和enddate为准
                    if result: self.UpdateCodeIndex(code, firstDate, dbEndDate, self.session)
                    continue
            else:
                if startdate >= dbStartDate and enddate <= dbEndDate:
                    # 说明日期在包裹之内，不用重新获取
                    print('已有数据，不用获取')
                    continue
                else:
                    if startdate >= dbStartDate and enddate > dbEndDate:
                        print('补充后方数据')
                        # 说明只需要获取后面一部分的数据
                        startdatebuff = dbEndDate + oneDay
                        endDatebuff = enddate
                        dbStartDateBuff = dbStartDate
                        dbEndDateBuff = endDatebuff
                        result, firstDate, lastDate = self.UpdateTimePeriodIndexDataSingle(code, startdatebuff, endDatebuff,'daypricedata')
                        # 这里边界以startdate和enddate为准
                        if result: self.UpdateCodeIndex(code, dbStartDate, lastDate, self.session)
                        continue
                    else:
                        if startdate < dbStartDate and enddate <= dbEndDate:
                            print('补充前方数据')
                            startdatebuff = startdate
                            endDatebuff = dbStartDate - oneDay
                            dbStartDateBuff = startdatebuff
                            dbEndDateBuff = dbEndDate
                            result, firstDate, lastDate = self.UpdateTimePeriodIndexDataSingle(code, startdatebuff,endDatebuff, 'daypricedata')
                            # 这里边界以startdate和enddate为准
                            if result: self.UpdateCodeIndex(code, firstDate, dbEndDate, self.session)
                            continue

    def GetDataBase(self, codeList, startdate, enddate,type):
        codeListStr = ""
        for code in codeList:
            codeListStr = codeListStr + "'" + code + "'" + ','
        codeListStr = codeListStr.rstrip(',')

        startDateStr = startdate.strftime('%Y-%m-%d')
        endDateStr = enddate.strftime('%Y-%m-%d')

        sql = f'select * from {type} where CODE in ({codeListStr}) AND DATE between "{startDateStr}" and "{endDateStr}"'
        # sql = "select * from daypricedata"
        # left join进行筛选带有expma的数据
        # sql = ‘select * from DayPriceData LEFT JOIN expma ON (DayPriceData.Date=expma.Date AND DayPriceData.Code = expma.Code_ID) where DayPriceData.Code in('1024.HK','3690.HK','0700.HK','0001.HK') AND DayPriceData.Date between "2023-06-01" and "2023-06-05"
        outData = pd.DataFrame()
        # 和tosql不一樣，一個用con用，egine，一個用con，
        outData = pd.read_sql(text(sql), con=self.con)
        outData = outData.sort_values(by="DATE", ascending=True)
        return outData

    def DataPreWindDB(self,startdate,endate):
        #根据日期，代码获取
        print('同步数据库')
        self.SyncDataBaseDayPirceDataUpdate(self.indexCodes, startdate, endate,'daypricedata')
        #计算指标,更新到数据库中
        tix=recognition.TechIndex(self.con, self.engine, self.session,tradingcalendar='HKEX')
        tix.CalAllEMAUpdate(self.indexCodes)
        tix.CalcKDJ(self.indexCodes)
        tix.CalAllRSI(self.indexCodes)
        tix.CalAllMACD(self.indexCodes)

        #gwd.SyncDataBaseCapitalFLow(codelist, startdate, endate, 'capitalflow')
        #计算ema数据和kdj数据
        #tix=recognition.TechIndex(self.con,self.engine,self.session,self.tradingCalendar)
        #print('计算技术指标')
        #tix.CalcKDJ(codelist)
        #tix.CalAllEMA(codelist)

        #################################测试将数据存进去数据库###########################
        complexData = self.GetDataBase(self.indexCodes, startdate, endate,'daypricedata')
        complexDataEMA = self.GetDataBase(self.indexCodes, startdate, endate,'expma')
        complexDataMACD= self.GetDataBase(self.indexCodes, startdate, endate,'macd')
        complexDataKDJ= self.GetDataBase(self.indexCodes, startdate, endate,'kdj')
        complexDataRSI= self.GetDataBase(self.indexCodes, startdate, endate,'rsi')

        # 声明一个stock类的数组
        indexList = [IndexClass for i in range(len(self.indexCodes))]
        indexlistIndex = 0
        for code in self.indexCodes:
            print(f'准备{code}的数据')
            buffDayPriceData = pd.DataFrame()  # 重新声明，避免浅拷贝
            buftMACD=pd.DataFrame()
            buffEMA = pd.DataFrame()
            buffKDJ = pd.DataFrame()
            buffRSI = pd.DataFrame()
            #rsi和ema需要将数据进行重新排列，其他的不需要
            buffDayPriceData = complexData[complexData['CODE'] == code]
            # 索引重置
            buffDayPriceData = buffDayPriceData.reset_index(drop=True)
            #排序顺序
            buffDayPriceData = buffDayPriceData.sort_values(by="DATE",ascending=True)

            buftMACD = complexDataMACD[complexDataMACD['CODE'] == code]
            buftMACD = buftMACD.reset_index(drop=True)
            buftMACD = buftMACD.sort_values(by="DATE", ascending=True)

            buffKDJ = complexDataKDJ[complexDataKDJ['CODE'] == code]
            buffKDJ = buffKDJ.reset_index(drop=True)
            buffKDJ = buffKDJ.sort_values(by="DATE", ascending=True)



            periods = basic.emaPeriod  # 获取到设定好的ema值
            rsiPeriods = basic.rsiPeriod
            #整理ema数据
            for period in periods:
                buffdataEMA = pd.DataFrame()
                buffdataEMA = complexDataEMA[(complexDataEMA['CODE'] == code) & (complexDataEMA['PERIOD'] == period)]
                buffdataEMA = buffdataEMA.sort_values(by="DATE", ascending=True)
                # 对EMA数据的格式进行操作
                buffEMA['DATE'] = buffdataEMA['DATE'].values
                buffEMA[f'EMA{period}'] = buffdataEMA['EXPMA'].values
            #整理rsi数据
            for period in rsiPeriods:
                buffdataRSI = pd.DataFrame()
                buffdataRSI = complexDataRSI[(complexDataRSI['CODE'] == code) & (complexDataRSI['PERIOD'] == period)]
                buffdataRSI = buffdataRSI.sort_values(by="DATE", ascending=True)
                # 对EMA数据的格式进行操作
                buffRSI['DATE'] = buffdataRSI['DATE'].values
                buffRSI[f'RSI{period}'] = buffdataRSI['RSI'].values

            # 这里对buffdata要进行排序
            IndexClassBuff = IndexClass(code,dayPriceDataFrame=buffDayPriceData, emaData=buffEMA,macdeData=buftMACD,
                                        rsiData=buffRSI,kdjData=buffKDJ,market='SZSE')
            indexList[indexlistIndex] = IndexClassBuff
            indexlistIndex = indexlistIndex + 1

        return indexList

    # 用来同步数据库中的数据时间的函数
    def UpdateCodeIndex(self, code, startDate, endDate, session):
        # 更新codeindex表的数据
        queryResult = session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).all()
        if len(queryResult) == 0:
            codeDateIndexbuff = database.CodeDateIndex(CODE=code, STARTDATE=startDate, ENDDATE=endDate)
            session.add(codeDateIndexbuff)
        else:
            session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).update(
                {"STARTDATE": startDate, "ENDDATE": endDate})
        session.commit()

    def CheckDataHasNan(self, dataframe):
        check_for_nan = dataframe.isnull().values.any()
        if check_for_nan == True:
            return 1
        else:
            return 0

    # 用来进行code的函数
    def CodeTransferWind2FUTU(self, codelist):
        codelistNew = list()
        for code in codelist:
            codebuff = code.split('.')
            if self.tradingCalendar == 'HKEX':
                codeNew = codebuff[1] + '.' + '0' + codebuff[0]
            elif self.tradingCalendar == 'NYSE':
                codeNew = 'US' + '.' + codebuff[0]
            codelistNew.append(codeNew)
        return codelistNew

    # 升级techindex的函数
    def UpdateTechIndex(self, code, startDate, endDate, session, techIndexType):
        queryResult = session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,
                                                                   database.TechDateIndex.TECHINDEXTYPE == techIndexType).all()
        if len(queryResult) == 0:
            codeDateIndexbuff = database.TechDateIndex(CODE=code, STARTDATE=startDate, ENDDATE=endDate,
                                                       TECHINDEXTYPE=techIndexType)
            session.add(codeDateIndexbuff)
        else:
            session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,
                                                         database.TechDateIndex.TECHINDEXTYPE == techIndexType).update(
                {"STARTDATE": startDate, "ENDDATE": endDate, "TECHINDEXTYPE": techIndexType})
        session.commit()

    ################################识别具体的技术指标的函数##########################################
    #rsi超买超卖判断
    def RSIOverBuySell(self,index):
        rsi=index.RSIData
        lastRSI6=rsi['RSI6'].iloc[-1]
        if lastRSI6<20:
            return 'rsi_over_sell'
        if lastRSI6>80:
            return 'rsi_over_buy'



    def KDJArc(self,index):
        kdj=index.KDJData
        arr = np.zeros(3)
        for i in range(0, 3):
            arr[i] = kdj['J'].iloc[i - 3]
        #识别圆弧顶部
        posMax = np.argmax(arr)
        if posMax == 1:
            #判断kdj的曲线关系
            if kdj['J'].iloc[-2]>kdj['K'].iloc[-2] and kdj['K'].iloc[-2]>kdj['D'].iloc[-2]:
                print("顶部弧")
                return 'kdj_top_arc'

        posMin = np.argmin(arr)
        if posMin == 1:
            #判断kdj的曲线关系
            if kdj['J'].iloc[-2]<kdj['K'].iloc[-2] and kdj['K'].iloc[-2]<kdj['D'].iloc[-2]:
                print("底部弧")
                return 'kdj_bottom_arc'

    def MACDArc(self,index):
        macd=index.MACDData
        if type == '5':
            # 5个值
            data = macd['DIFF']
            arr = np.zeros(5)
            for i in range(0, 5):
                # 取最后5个
                arr[i] = data.iloc[i - 5]
            posMax = np.argmax(arr)
            if posMax == 2:
                if arr[1] > arr[0] and arr[3] > arr[4]:
                    print("MACD_top_arc")
                    return "MACD_top_arc"
            if posMax == 3:
                if arr[1] > arr[0] and arr[2] > arr[1]:
                    print("MACD_top_arc")
                    return "MACD_top_arc"

            posMin = np.argmin(arr)
            if posMin == 2:
                if arr[1] < arr[0] and arr[3] < arr[4]:
                    print("MACD_bottom_arc")
                    return "MACD_bottom_arc"
            if posMin == 3:
                if arr[1] < arr[0] and arr[2] < arr[1]:
                    print("MACD_bottom_arc")
                    return "MACD_bottom_arc"

    def MoneyFLow(self, index):
        if index.code=='000001.SH':
            moneyFlow=crawler.GetEastMoneyData(crawler.urlSH)
        elif index.code=='399001.SZ':
            moneyFlow=crawler.GetEastMoneyData(crawler.urlSZ)
        return moneyFlow

    def MoneyFLowNorth(self):
        moneyFlow = crawler.GetNorthMoneyData(crawler.urlNorthMoney)
        return moneyFlow
    ##############################################进行遍历的信号判断###############################################
    def RSIOverBuySellSignal(self,index):
        rsi=index.RSIData
        ResultArry=np.zeros(rsi.shape[0])
        for i in range(rsi.shape[0]):
            RSI6=rsi['RSI6'].iloc[i]
            if RSI6<15:
                ResultArry[i]=1

        return ResultArry

    def KDJUpcrossSignal(self,index):
        kdj = index.KDJData
        ResultArry=np.zeros(kdj.shape[0])
        for i in range(kdj.shape[0]):
            #判断上穿
            if kdj['J'].iloc[i-1] <= kdj['K'].iloc[i-1] and kdj['J'].iloc[i] > kdj['K'].iloc[i]:
                ResultArry[i] = 1
        return ResultArry

        # EMA均线的圆形底

    def EMA5BottomArcSignal(self, index):
        ema = index.EMAData
        ResultArry = np.zeros(ema.shape[0])
        #需要考虑两边的情况，后续是看3日后的值，所以边界-3
        for i in range(2,ema.shape[0]-3):
            '''
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
            arr = np.zeros(5)
            for j in range(0, 5):
                # 取最后5个
                arr[j] = ema['EMA5'].iloc[i - 2 +j]
            posMin = np.argmin(arr)
            if posMin == 3:
                if arr[1] < arr[0] and arr[2] < arr[1]:
                    ResultArry[i+2] = 1


        return ResultArry


    def CalSignalProbability(self,resultarry,index):
        success=0
        signalCount=0
        dayPriceData=index.dayPriceData
        for i in range(len(resultarry)-3):
            if resultarry[i]==1:
                #取三天后的价格判断
                signalCount=signalCount+1
                pricePre=dayPriceData['CLOSE'].iloc[i]
                priceAfter=dayPriceData['CLOSE'].iloc[i+3]
                if priceAfter>pricePre:
                    success=success+1

        if signalCount!=0:
            SignalProb=success/signalCount

        return SignalProb

    def EMA5up10Strategy(self,index):
        code=index.code
        success = 0
        signalCount = 0
        accumlateReturn=0
        buyFlag=0
        buyPrice=0
        sellPrice=0
        dayPriceData = index.dayPriceData
        ema = index.EMAData
        pos=0
        for i in range(1,dayPriceData.shape[0]):
            if ema['EMA5'].iloc[i - 1] <= ema['EMA10'].iloc[i - 1] and ema['EMA5'].iloc[i] > ema['EMA10'].iloc[i]:
                #上穿，买入
                buyFlag=1
                signalCount=signalCount+1
                #记录买点位置
                pos=i
                buyDate=dayPriceData['DATE'].iloc[i]
                #buyPrice=dayPriceData['CLOSE'].iloc[i]
                #按照上下穿均线价格买入卖出
                buyPrice=ema['EMA5'].iloc[i]
                print(f'{code}买入日期：{buyDate},买入价格{buyPrice}')
            elif ema['EMA5'].iloc[i - 1] >= ema['EMA10'].iloc[i - 1] and ema['EMA5'].iloc[i] < ema['EMA10'].iloc[i]:
                # 下穿，卖出
                if buyFlag==1:
                    buyFlag = 0
                    sellDate = dayPriceData['DATE'].iloc[i]
                    # sellPrice=dayPriceData['CLOSE'].iloc[i]
                    # 按照上下穿均线价格买入卖出
                    sellPrice = ema['EMA5'].iloc[i]
                    gain = (sellPrice - buyPrice) / buyPrice
                    accumlateReturn = accumlateReturn + gain

                    if gain>0:
                        success=success+1



                    print(f'{code}卖出日期：{sellDate}，本次收益率{gain},卖出价格{sellPrice}')

            else:
                pass
        if signalCount!=0:
            SignalProb=success/signalCount
        return SignalProb,accumlateReturn


    def ProbabilityProc(self,index):
        UpProbability=0
        DownProbability=0
        ResultRSIOverBuySell=self.RSIOverBuySell(index)
        ResultKDJArc=self.KDJArc(index)
        ResultMACDArc=self.MACDArc(index)
        ResultMoneyFLow=self.MoneyFLowNorth()

        #判断rsi超买超卖的概率
        if ResultRSIOverBuySell=='rsi_over_sell':
            UpProbability+=20
        elif ResultRSIOverBuySell=='rsi_over_buy':
            DownProbability+=20

        if ResultKDJArc=='kdj_bottom_arc':
            UpProbability+=20
        elif ResultKDJArc=='kdj_top_arc':
            DownProbability+=20

        if ResultMACDArc=='MACD_bottom_arc':
            UpProbability+=20
        elif ResultMACDArc=='MACD_top_arc':
            DownProbability+=20

        if ResultMoneyFLow > 0:
            UpProbability += 20
        elif ResultMoneyFLow < 0:
            DownProbability += 20

        print(f'{index.code}分析上涨概率{UpProbability}%')
        print(f'{index.code}分析下跌概率{DownProbability}%')

class USindex:
    #indexCodes=["IXIC.GI,DJI.GI,SPX.GI"]

    def __init__(self,code, con, engine, session):
        self.con = con
        self.engine = engine
        self.session = session
        self.code=code


  #获取给定单一code的数据，成功返回1，失败返回0
    def UpdateTimePeriodIndexDataSingle(self,code, startDate, endDate, tableName):
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()
        #美股日历默认用NYSE
        buffData = w.wsd(code, "open,high,low,close,volume", statrTimeStr, endTimeStr,"TradingCalendar=NYSE;PriceAdj=F")
        print(f"获取{code}的日线数据")
        if buffData.Data[0][0] == 'CWSDService: No data.':
            # 抛出无数据的异常
            raise myexception.ExceptionWindNoData('CWSDService: No data.')
        data = pd.DataFrame(buffData.Data, index=buffData.Fields)
        data = data.T
        # 检查数据是否有nan，防止nan进入数据库
        if self.CheckDataHasNan(data):
            print('数据中含有nan，无法使用')
            return 0
        data['DATE'] = buffData.Times
        data['CODE'] = code
        data.to_sql(name=tableName, con=self.engine, if_exists="append", index=False)
        self.con.commit()
        return 1



    #同步数据，这里通过传入func回调函数的方法调用上面的函数
    def SyncDataBase(self, startdate, enddate,func,type):
        #通过type确定是哪个表和哪个指标,这里需要对type的类型进行确定
        oneDay = timedelta(days=1)
        # 需要将单个的code转化为codelist，满足update函数的运行条件
        code=self.code
        codeList=[]
        codeList.append(code)
        #这里实际上循环只执行一次，这样写是为了不改动之前的逻辑
        for code in codeList:
            #分辨是日线还是技术指标
            if type=='daypricedata':
                codeResult = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).all()
            else:
                codeResult = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE == type).all()

            if len(codeResult) == 0:
                print(f'表中无该{code}数据,开始获取新数据')
                try:
                    result = func(code=code, startDate=startdate, endDate=enddate,tableName=type)
                except myexception.ExceptionWindNoData as e:
                    print("非交易日，无数据")
                    continue
                if result:
                    if type == 'daypricedata':
                        self.UpdateCodeIndex(startdate, enddate, self.session)
                    else:
                        self.UpdateTechIndex(startdate, enddate, self.session, techIndexType=type)
                continue

            # code在表中存在的情况下，在表中获取startdate和enddate
            if type == 'daypricedata':
                index = self.session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).first()
            else:
                index = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE == type).first()

            dbStartDate = index.STARTDATE
            dbEndDate = index.ENDDATE
            # 还有一种情况是需要获取的数据范围包裹住了数据库内的数据范围
            if startdate < dbStartDate and enddate > dbEndDate:
                print('前后方数据都需要补充')
                # 补充前方数据并更新前方index
                startdatebuff = startdate
                endDatebuff = dbStartDate - oneDay
                result = func(code, startdatebuff, endDatebuff, type)
                if result:
                    if type == 'daypricedata':
                        self.UpdateCodeIndex(startdate, dbEndDate, self.session)
                    else:
                        self.UpdateTechIndex(startdate, dbEndDate, self.session, techIndexType=type)
                # 补充后方数据并更新后方index
                startdatebuff = dbEndDate + oneDay
                endDatebuff = enddate
                result = func(code, startdatebuff, endDatebuff, type)
                # 这里边界以startdate和enddate为准
                if result:
                    if type == 'daypricedata':
                        self.UpdateCodeIndex(startdate, enddate, self.session)
                    else:
                        self.UpdateTechIndex(startdate, enddate, self.session, techIndexType=type)
                continue

            if (startdate >= dbEndDate or enddate <= dbStartDate) and startdate != enddate:
                # 说明数据库中的时间范围和目标时间范围不重叠，中间的空隙需要补上，否则整个逻辑会失效
                if startdate >= dbEndDate:
                    print('数据在后方有空隙，进行填补')
                    startdatebuff = dbEndDate + oneDay
                    endDatebuff = enddate
                    dbStartDateBuff = dbStartDate
                    dbEndDateBuff = endDatebuff
                else:
                    print('数据在前方有空隙，进行填补')
                    startdatebuff = startdate
                    endDatebuff = dbStartDate - oneDay
                    dbStartDateBuff = startdatebuff
                    dbEndDateBuff = dbEndDate
            else:
                if startdate >= dbStartDate and enddate <= dbEndDate:
                    # 说明日期在包裹之内，不用重新获取
                    print('已有数据，不用获取')
                    continue
                else:
                    if startdate >= dbStartDate and enddate > dbEndDate:
                        print('补充后方数据')
                        # 说明只需要获取后面一部分的数据
                        startdatebuff = dbEndDate + oneDay
                        endDatebuff = enddate
                        dbStartDateBuff = dbStartDate
                        dbEndDateBuff = endDatebuff
                    else:
                        if startdate < dbStartDate and enddate <= dbEndDate:
                            print('补充前方数据')
                            startdatebuff = startdate
                            endDatebuff = dbStartDate - oneDay
                            dbStartDateBuff = startdatebuff
                            dbEndDateBuff = dbEndDate
            # 根据日期情况更新数据
            try:
                result = func(code, startdatebuff, endDatebuff, type)
            except myexception.ExceptionWindNoData as e:
                print("非交易日，无数据")
                continue
            if result:
                if result:
                    if type == 'daypricedata':
                        self.UpdateCodeIndex(dbStartDateBuff, dbEndDateBuff, self.session)
                    else:
                        self.UpdateTechIndex(dbStartDateBuff, dbEndDateBuff, self.session, techIndexType=type)

    def GetDataBase(self, startdate, enddate,type):
        codeListStr = ""
        code = self.code
        codeListStr = codeListStr + "'" + code + "'" + ','
        codeListStr = codeListStr.rstrip(',')

        startDateStr = startdate.strftime('%Y-%m-%d')
        endDateStr = enddate.strftime('%Y-%m-%d')

        sql = f'select * from {type} where CODE in ({codeListStr}) AND DATE between "{startDateStr}" and "{endDateStr}"'
        # sql = "select * from daypricedata"
        # left join进行筛选带有expma的数据
        # sql = ‘select * from DayPriceData LEFT JOIN expma ON (DayPriceData.Date=expma.Date AND DayPriceData.Code = expma.Code_ID) where DayPriceData.Code in('1024.HK','3690.HK','0700.HK','0001.HK') AND DayPriceData.Date between "2023-06-01" and "2023-06-05"
        outData = pd.DataFrame()
        # 和tosql不一樣，一個用con用，egine，一個用con，
        outData = pd.read_sql(text(sql), con=self.con)
        outData = outData.sort_values(by="DATE", ascending=True)
        return outData

    def DataPreWindDB(self,startdate,endate):
        #根据日期，代码获取
        print('同步数据库')
        self.SyncDataBase( startdate, endate, self.UpdateTimePeriodIndexDataSingle,'daypricedata')

        #################################测试将数据存进去数据库###########################
        complexData = self.GetDataBase(startdate, endate,'daypricedata')

        # 声明一个index类的数组
        print(f'准备{self.code}的数据')
        buffDayPriceData = pd.DataFrame()  # 重新声明，避免浅拷贝
        #rsi和ema需要将数据进行重新排列，其他的不需要
        buffDayPriceData = complexData[complexData['CODE'] == self.code]
        # 索引重置
        buffDayPriceData = buffDayPriceData.reset_index(drop=True)
        #排序顺序
        buffDayPriceData = buffDayPriceData.sort_values(by="DATE",ascending=True)

        # 这里对buffdata要进行排序
        IndexClassBuff = IndexClass.USIndexInit(self.code, dayPriceDataFrame=buffDayPriceData,market='NYSE')

        self.IndexClass=IndexClassBuff
        #return IndexClassBuff

    # 用来同步数据库中的数据时间的函数
    def UpdateCodeIndex(self, startDate, endDate, session):
        # 更新codeindex表的数据
        code=self.code
        queryResult = session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).all()
        if len(queryResult) == 0:
            codeDateIndexbuff = database.CodeDateIndex(CODE=code, STARTDATE=startDate, ENDDATE=endDate)
            session.add(codeDateIndexbuff)
        else:
            session.query(database.CodeDateIndex).filter(database.CodeDateIndex.CODE == code).update(
                {"STARTDATE": startDate, "ENDDATE": endDate})
        session.commit()

    def CheckDataHasNan(self, dataframe):
        check_for_nan = dataframe.isnull().values.any()
        if check_for_nan == True:
            return 1
        else:
            return 0

    # 用来进行code的函数
    def CodeTransferWind2FUTU(self, codelist):
        codelistNew = list()
        for code in codelist:
            codebuff = code.split('.')
            if self.tradingCalendar == 'HKEX':
                codeNew = codebuff[1] + '.' + '0' + codebuff[0]
            elif self.tradingCalendar == 'NYSE':
                codeNew = 'US' + '.' + codebuff[0]
            codelistNew.append(codeNew)
        return codelistNew

    # 升级techindex的函数
    def UpdateTechIndex(self, startDate, endDate, session, techIndexType):
        code=self.code
        queryResult = session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,
                                                                   database.TechDateIndex.TECHINDEXTYPE == techIndexType).all()
        if len(queryResult) == 0:
            codeDateIndexbuff = database.TechDateIndex(CODE=code, STARTDATE=startDate, ENDDATE=endDate,
                                                       TECHINDEXTYPE=techIndexType)
            session.add(codeDateIndexbuff)
        else:
            session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,
                                                         database.TechDateIndex.TECHINDEXTYPE == techIndexType).update(
                {"STARTDATE": startDate, "ENDDATE": endDate, "TECHINDEXTYPE": techIndexType})
        session.commit()


###########################################################beta分析类的#####################################################################
def CalCorrBeta(index,stock):
    #这里根据定义直接获取到日线数据的close值
    index_close = index.IndexClass.dayPriceData['CLOSE']
    index_pct = index_close.pct_change()

    stock_close=stock.dayPriceData['CLOSE']
    stock_pct = stock_close.pct_change()

    #判断是否相等
    if len(index_close)!=len(stock_close):
        print(f'{stock.code}数据不完整')
        return 0,0
    r_Matrix = np.corrcoef(index_close, stock_close)
    r=r_Matrix[0][1]
    covr_Matrix = np.cov(stock_pct[1:],index_pct[1:])
    covr = covr_Matrix[0][1]
    index_var = np.var(index_pct[1:])
    beta = covr / index_var
    return r,beta

def BetaAnalyze(startdate,enddate,stocklist):
    #indexCodes=["IXIC.GI,DJI.GI,SPX.GI"]
    #纳斯达克，道琼斯，标普，三个指数类
    IXIC = USindex('IXIC.GI', database.conUS, database.engineUS, database.sessionUS)
    #DJI=USindex('DJI.GI', database.conUS, database.engineUS, database.sessionUS)
    SPX=USindex('SPX.GI', database.conUS, database.engineUS, database.sessionUS)
    IXIC.DataPreWindDB(startdate, enddate)
    #DJI.DataPreWindDB(startdate, enddate)
    SPX.DataPreWindDB(startdate, enddate)


    #存放结果的dataframe
    resultTable=pd.DataFrame(columns=['code','IXIC_R','IXIC_Beta','DJI_R','DJI_Beta','SPX_R','SPX_Beta'])
    for stock in stocklist:
        print(f'计算{stock.code}beta值')
        dicBuff = {'code':'','IXIC_R':0,'IXIC_Beta':0,'DJI_R':0,'DJI_Beta':0,'SPX_R':0,'SPX_Beta':0}
        dicBuff['code'] = stock.code
        dicBuff['IXIC_R'],dicBuff['IXIC_Beta'] = CalCorrBeta(IXIC,stock)
        #dicBuff['DJI_R'],dicBuff['DJI_Beta'] = CalCorrBeta(DJI,stock)
        dicBuff['SPX_R'],dicBuff['SPX_Beta'] = CalCorrBeta(SPX,stock)

        # 将结果添加到resultable
        resultTable.loc[len(resultTable)] = dicBuff

    return resultTable


'''
startDate = date(2023, 8, 1)
endDate = date(2023, 12, 7)
IXIC=USindex('IXIC.GI', database.conUS,database.engineUS,database.sessionUS)
BABA=USindex('AAPL.O', database.conUS,database.engineUS,database.sessionUS)
IXIC.DataPreWindDB(startDate,endDate)
BABA.DataPreWindDB(startDate,endDate)
IXIC_close=IXIC.IndexClass.dayPriceData['CLOSE']
BABA_close=BABA.IndexClass.dayPriceData['CLOSE']
IXIC_close_pct=IXIC_close.pct_change()
BABA_close_pct=BABA_close.pct_change()


r = np.corrcoef(IXIC_close, BABA_close)
covr_MATRIX=np.cov(BABA_close_pct[1:],IXIC_close_pct[1:])
covr = covr_MATRIX[0][1]
#计算沪深300指数数据的方差
var = np.var(IXIC_close_pct[1:])
var2 = np.var(BABA_close_pct[1:])

R2=covr/pow((var*var2),0.5)
#计算贝塔值
beta = covr/var
cov= np.cov(IXIC_close, BABA_close)
pass
'''