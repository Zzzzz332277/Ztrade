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
    def __init__(self):
        pass

    def __init__(self, code, dayPriceDataFrame,emaData,macdeData,rsiData,kdjData,market):  # 从数据库中取出的是一个dataframe
        self.code = code
        self.trendList = [Trend]
        self.EMAData1 = emaData
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


#专门对A股指数进行处理
class Aindex:
    indexCodes=['000001.SH','399001.SZ']
    def __init__(self, con, engine, session):
        self.con = con
        self.engine = engine
        self.session = session


    def SyncIndexData(self):
        indexDB=database.GetWindDaTA(self.con,self.engine,self.session)
        pass
  #获取给定单一code的数据，成功返回1，失败返回0
    def UpdateTimePeriodIndexDataSingle(self, code, startDate, endDate, tableName):
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()

        buffData = w.wsd(code, "open,high,low,close,volume", statrTimeStr, endTimeStr,"PriceAdj=F")
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


    #同步数据，这里通过传入func回调函数的方法调用上面的函数
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
            # 根据日期情况鞥新数据
            try:
                result = self.func(code, startdatebuff, endDatebuff, type)
            except myexception.ExceptionWindNoData as e:
                print("非交易日，无数据")
                continue
            if result:
                if result:
                    if type == 'daypricedata':
                        self.UpdateCodeIndex(code, dbStartDateBuff, dbEndDateBuff, self.session)
                    else:
                        self.UpdateTechIndex(code, dbStartDateBuff, dbEndDateBuff, self.session, techIndexType=type)

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
        self.SyncDataBase(self.indexCodes, startdate, endate, self.UpdateTimePeriodIndexDataSingle,'daypricedata')
        self.SyncDataBase(self.indexCodes, startdate, endate, self.UpdateTimePeriodIndexDataEMA,'expma')
        self.SyncDataBase(self.indexCodes, startdate, endate, self.UpdateTimePeriodIndexDataKDJ,'kdj')
        self.SyncDataBase(self.indexCodes, startdate, endate, self.UpdateTimePeriodIndexDataSingleRSI,'rsi')
        self.SyncDataBase(self.indexCodes, startdate, endate, self.UpdateTimePeriodIndexDataSingleMACD,'macd')

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
                buffRSI[f'EMA{period}'] = buffdataRSI['RSI'].values

            # 这里对buffdata要进行排序
            IndexClassBuff = IndexClass(code, dayPriceDataFrame=buffDayPriceData, emaData=buffEMA,macdeData=buftMACD,
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

startDate = date(2023, 8, 1)
endDate = date(2023, 11, 30)
IC=Aindex(database.con,database.engine,database.session)
IC.DataPreWindDB(startDate,endDate)