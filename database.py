import datetime
from WindPy import *
import sqlalchemy
#from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text,create_engine, Table, Column, Integer, String, Float,Date, MetaData, ForeignKey, desc, inspect
import pymysql
from sqlalchemy.orm import sessionmaker,declarative_base
import pandas as pd
import numpy as np
import stockclass
import myexception
import time

# 数据库的地址
DataBaseAddr = {'hostNAME': '127.0.0.1',
                'PORT': '3306',
                'database': 'ztrade',
                'username': 'root',
                'password': 'mysqlzph'}

'''
    datas = pd.DataFrame({'id':[111233,12324,3525,1231],'name':['zzz','adsfa','dfsf','derer'],'deptid':['2000','232323','145145','234234'],'salary':[343434,234234,542525,2342343]})
    engine=sqlalchemy.create_engine("mysql+pymysql://root:xinxikemysql@localhost:3306/testz")

con = engine.connect()
datas.to_sql(name="testz_table",con=con,if_exists="append")

con.close()
'''
#####################################数据库的连接######################################
engine = sqlalchemy.create_engine(f"mysql+pymysql://{DataBaseAddr['username']}:{DataBaseAddr['password']}@localhost:3306/{DataBaseAddr['database']}")
con = engine.connect()
#这里要分析下con和engine的区别

Session = sessionmaker(bind=engine)
session=Session()
#metadata = MetaData(bind=engine)
#用于获取到标的字段数据
insp = inspect(engine)
Base = declarative_base()

#####################################数据库的连接######################################



'''
目前需要捋顺几个点，
'''

##########################定义sqlalchemy映射的类####################################
#不同表对应的查询wind用的关键字字符串
tableKeyWordForWind={'daypricedata':'open,high,low,close,volume',
                     'ExpMA':'"EXPMA_N=5,"EXPMA_N=10,"EXPMA_N=20,"EXPMA_N=30,"EXPMA_N=60'}

class StockCode(Base):
    __tablename__ = 'StockCode'
    Id = Column(Integer, primary_key=True)
    Code=Column(String(60))

class DayPriceData(Base):
    __tablename__ = 'DayPriceData'
    ID = Column(Integer, primary_key=True)
    '''
    CODE=Column(String(60))
    #Code_ID=Column(Integer, ForeignKey('StockCode.Id'))
    DATE= Column(Date)
    OPEN= Column(Float)
    CLOSE= Column(Float)
    HIGH= Column(Float)
    LOW= Column(Float)
    VOLUME=Column(Float)
    '''
    CODE = Column(String(60))
    # Code_ID=Column(Integer, ForeignKey('StockCode.Id'))
    DATE = Column(Date)
    OPEN = Column(Float)
    CLOSE = Column(Float)
    HIGH = Column(Float)
    LOW = Column(Float)
    VOLUME = Column(Float)

class MoneyFlowData(Base):
    __tablename__ = 'MoneyFlow'
    ID = Column(Integer, primary_key=True)
    #Code_ID=Column(Integer, ForeignKey('StockCode.Id'))
    DATE= Column(Date)
    #futu‘s moneyflow
    LARGEORDER = Column(Float)
    BIGORDER = Column(Float)
    MIDORDER = Column(Float)
    SMALLORDER = Column(Float)

class ExpMA(Base):
    __tablename__ = 'ExpMA'
    ID = Column(Integer, primary_key=True)
    CODE=Column(String(60))

    #Code_ID=Column(Integer, ForeignKey('StockCode.Id'))
    DATE= Column(Date)
    EXPMA= Column(Float)
    PERIOD= Column(Integer)

class CodeDateIndex(Base):
    __tablename__ = 'CodeDateIndex'
    ID = Column(Integer, primary_key=True)
    CODE=Column(String(60))
    STARTDATE= Column(Date)
    ENDDATE= Column(Date)

####################################创建表格###############################################################
Base.metadata.create_all(engine)
#######################################################################################################################
# 向数据库中写数据
class DBOperation:

    def __int__(self):
        pass

    def DBConnection(self, databaseaddr):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{databaseaddr['username']}:{databaseaddr['password']}@localhost:3306/{databaseaddr['database']}")
        con = engine.connect()
        Session = sessionmaker(bind=engine)

        return con

    def DBWrite(self, con, table, data):
        data.to_sql(name=table, con=con, if_exists="append")

    # 读取对应代码下的所有数据库数据
    def DBRead(self, con, outdata, stockcode):
        outdata.read_sql('select * from testz_table', con)
        return outdata


# 获取wind数据的类
class GetWindDaTA:
    # 这里是一个dataframe对象的数组，用来存放获取到的wind数据集
    dataSet = []
    dataSetUpdate = []
    #EMA的周期数,这里注意，存到数据库里变成数字，和直接从wind拿到的不同
    #emaPeriod = ['5', '10', '20', '30', '60', '120', '250']
    emaPeriod = [5, 10, 20, 30, 60, 120, 250]


    def __init__(self):
        pass

    def GetCodeHK(self):
        pass
    '''
    def GetDataHK(self):
        for i in range(0, len(codeHKtest) - 1):
            w.start()
            print(f'获取{codeHKtest[i]}的行情数据')
            buffData = w.wsd(codeHKtest[i],
                             "open,low,close,volume,amt,pct_chg,turn,mfd_buyamt_a,mfd_sellamt_a,mfd_buyvol_a,mfd_sellvol_a,mfd_netbuyamt,mfd_netbuyvol,mfd_buyamt_d,mfd_sellamt_d,mfn_sn_inflowdays,mfn_sn_outflowdays,mfd_sn_buyamt,mfd_sn_sellamt",
                             "2023-01-01", "2023-04-26", "unit=1;traderType=1;TradingCalendar=HKEX;PriceAdj=F")
            data = pd.DataFrame(buffData.Data, index=buffData.Fields)
            # 引入时间
            data = data.T  # 转置
            data['TIMES'] = buffData.Times
            self.dataSet.append(data)
    '''
    # 传入证券代码数组,时间,表名，更新数据到数据库
    def UpdateTimePeriodData(self, codelist, startDate, endDate,tableName):
        # 合并字符串
        windinputstr = ','.join(codelist)
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()
        for code in codelist:
            buffData = w.wsd(code,tableKeyWordForWind[tableName],statrTimeStr, endTimeStr, "unit=1;traderType=1;TradingCalendar=HKEX;PriceAdj=F")
            print(f"获取{code}的日线数据")
            if buffData.Data[0][0]=='CWSDService: No data.':
                #抛出无数据的异常
                raise myexception.ExceptionWindNoData('CWSDService: No data.')
            data = pd.DataFrame(buffData.Data, index=buffData.Fields)
            data = data.T
            data['DATE'] = buffData.Times
            data['CODE'] = code
            data.to_sql(name=tableName, con=engine, schema='ztrade',if_exists="append",index=False)



    #获取EMA数据并写入数据库
    def UpdateTimePeriodDataEMA(self, codelist, startDate, endDate,tableName):
        # 合并字符串
        windinputstr = ','.join(codelist)
        concatDataframe = pd.DataFrame(columns=["EXPMA","DATE","CODE","PERIOD"])
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()


        for code in codelist:
            for period in self.emaPeriod:
                start_time = time.time()
                buffData = w.wsd(code, "EXPMA", statrTimeStr, endTimeStr, f"EXPMA_N={period};TradingCalendar=HKEX;PriceAdj=F")
                # 这里取出的是没有时间的。
                print(f"获取{code}的{period}ema数据")
                data = pd.DataFrame(buffData.Data, index=buffData.Fields)
                data = data.T
                data['DATE'] = buffData.Times
                data['CODE'] = code
                data['PERIOD'] = period
                concatDataframe=pd.concat([concatDataframe,data])
                end_time = time.time()
                print("耗时: {:.2f}秒".format(end_time - start_time))

        #全部一次性写入
        print("开始写入数据库")
        concatDataframe.to_sql(name=tableName, con=engine,schema='ztrade', if_exists="append",index=False)

###################################这两个程序是只返回dataframe，不存储##########################################
    def GetTimePeriodData(self, codelist, startDate, endDate):
        # 合并字符串
        #用于合并的dataframe
        concatDataframe = pd.DataFrame(columns=["OPEN","HIGH","LOW","CLOSE","VOLUME","DATE","CODE"])

        windinputstr = ','.join(codelist)
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()
        for code in codelist:
            buffData = w.wsd(code,'open,high,low,close,volume',statrTimeStr, endTimeStr, "unit=1;traderType=1;TradingCalendar=HKEX;PriceAdj=F")
            print(f"获取{code}的日线数据")
            # 这里取出的是没有时间的。
            data = pd.DataFrame(buffData.Data, index=buffData.Fields)
            data = data.T
            data['DATE'] = buffData.Times
            data['CODE'] = code
            concatDataframe = pd.concat([concatDataframe,data])
            #Id = np.linspace(1,len(data),len(data))
            #data['ID'] = Id
        return concatDataframe

    #写一个只获取数据，不存储的程序
    def GetTimePeriodDataEMA(self, codelist, startDate, endDate):
        # 合并字符串
        windinputstr = ','.join(codelist)
        concatDataframe = pd.DataFrame(columns=["EXPMA","DATE","CODE","PERIOD"])
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()
        for code in codelist:
            for period in self.emaPeriod:
                start_time = time.time()
                buffData = w.wsd(code, "EXPMA", statrTimeStr, endTimeStr, f"EXPMA_N={period};TradingCalendar=HKEX;PriceAdj=F")
                # 这里取出的是没有时间的。
                print(f"获取{code}的{period}ema数据")
                data = pd.DataFrame(buffData.Data, index=buffData.Fields)
                data = data.T
                data['DATE'] = buffData.Times
                data['CODE'] = code
                data['PERIOD'] = period
                concatDataframe=pd.concat([concatDataframe,data])
                end_time = time.time()
                print("耗时: {:.2f}秒".format(end_time - start_time))
        #返回到dataframe中
        return concatDataframe
#####################################################################################

    # 根据证券代码更新当天
    def UpdateTodayData(self, codelist):
        # 合并字符串
        windinputstr = ','.join(codelist)
        w.start()
        today = date.today()
        todaystr = today.strftime("%Y%m%d")
        buffData = w.wss(windinputstr, "pre_close,open,high,low,volume,amount_btin,amount_aht",
                         f"tradeDate={todaystr};priceAdj=U;cycle=D;unit=1")
        # 这里取出的是没有时间的。
        data = pd.DataFrame(buffData.Data, index=buffData.Fields)
        data = data.T
        for i in range(0, len(codelist) - 1):
            buffRow = data.iloc[[i]]
            buffRow['DATE'] = today
            self.dataSetUpdate.append(buffRow)
        return self.dataSetUpdate

    # 根据code更新到今天的值
    def UpdateDataToNewest(self, codelist,tableName):
        #根据输入的表的名字，去查询字段
        #columnDict= insp.get_columns(tableName, schema=DataBaseAddr['database'])

        # 合并字符串
        windinputstr = ','.join(codelist)
        w.start()
        today = date.today()
        todayStr = today.strftime("%Y%m%d")
        #据codelist去查询每个code在数据库中的时间情况，确定起始时间
        for code in codelist:
            dayResult = session.query(DayPriceData).filter(DayPriceData.Code==code).order_by(desc(DayPriceData.Date)).first()
            #更新至最新的值，判断一下是不是5点，不然更新前一天
            #判断 if 时间是五点之前更新前一天，否则后一探
            if dayResult.Date==today:
                print("Already Newest!")
            else:
                #从wind进行升级
                startDate=dayResult.Date+timedelta(days=1)
                startDateStr=startDate.strftime("%Y%m%d")
                #从tablename对应的字典来查找数据
                print(f"获取{code}的日线数据")
                buffData = w.wsd(code, tableKeyWordForWind[tableName], startDateStr, todayStr,"unit=1;traderType=1;TradingCalendar=HKEX;PriceAdj=F")
                data = pd.DataFrame(buffData.Data, index=buffData.Fields)
                data = data.T
                data['DATE'] = buffData.Times
                data['CODE'] = code
                data.to_sql(name=tableName,con=engine,schema='ztrade',if_exists="append",index=False)

    # 根据code更新到今天的值
    def UpdateDataToNewestEMA(self, codelist, tableName):
        # 根据输入的表的名字，去查询字段
        # columnDict= insp.get_columns(tableName, schema=DataBaseAddr['database'])
        concatDataframe = pd.DataFrame(columns=["EXPMA","DATE","CODE","PERIOD"])
        # 合并字符串
        windinputstr = ','.join(codelist)
        w.start()
        today = date.today()
        todayStr = today.strftime("%Y%m%d")
        # 据codelist去查询每个code在数据库中的时间情况，确定起始时间
        for code in codelist:
            dayResult = session.query(ExpMA).filter(ExpMA.Code == code).order_by(desc(ExpMA.Date)).first()
            # 更新至最新的值，判断一下是不是5点，不然更新前一天
            # 判断 if 时间是五点之前更新前一天，否则后一探
            if dayResult.Date == today:
                print("Already Newest!")
            else:
                # 从wind进行升级
                startDate = dayResult.Date + timedelta(days=1)
                startDateStr = startDate.strftime("%Y%m%d")
                # 从tablename对应的字典来查找数据
                for period in self.emaPeriod:
                    buffData = w.wsd(code, "EXPMA", startDateStr, todayStr,f"EXPMA_N={period};TradingCalendar=HKEX;PriceAdj=F")
                    print(f"获取{code}的{period}ema数据")
                    # 这里取出的是没有时间的。
                    data = pd.DataFrame(buffData.Data, index=buffData.Fields)
                    data = data.T
                    data['DATE'] = buffData.Times
                    data['CODE'] = code
                    data['PERIOD'] = period
                    concatDataframe = pd.concat([concatDataframe, data])
                    # 全部一次性写入
        print("开始写入数据库")
        concatDataframe.to_sql(name=tableName, con=engine, schema='ztrade', if_exists="append", index=False)


    #将数据库的数据查询出来存储在一个dataframe里，以供后续进行筛选，处理。需要细化传入的是date类型还是字符串
    def GetDataBase(self, codeList, startdate, enddate):
        codeListStr=""
        for code in codeList:
            codeListStr = codeListStr+"'"+code+"'"+','
        codeListStr=codeListStr.rstrip(',')

        startDateStr = startdate.strftime('%Y-%m-%d')
        endDateStr = enddate.strftime('%Y-%m-%d')

        sql = f'select * from daypricedata where CODE in ({codeListStr}) AND DATE between "{startDateStr}" and "{endDateStr}"'
        #sql = "select * from daypricedata"
        #left join进行筛选带有expma的数据
        #sql = ‘select * from DayPriceData LEFT JOIN expma ON (DayPriceData.Date=expma.Date AND DayPriceData.Code = expma.Code_ID) where DayPriceData.Code in('1024.HK','3690.HK','0700.HK','0001.HK') AND DayPriceData.Date between "2023-06-01" and "2023-06-05"
        outData = pd.DataFrame()
        outData=pd.read_sql(text(sql), con=con)
        return outData


    def GetDataBaseEMA(self, codeList, startdate, enddate):
        codeListStr = ""
        for code in codeList:
            codeListStr = codeListStr + "'" + code + "'" + ','
        codeListStr = codeListStr.rstrip(',')

        startDateStr = startdate.strftime('%Y-%m-%d')
        endDateStr = enddate.strftime('%Y-%m-%d')

        sql = f'select * from expma where CODE in ({codeListStr}) AND DATE between "{startDateStr}" and "{endDateStr}"'
        # sql = "select * from daypricedata"
        # left join进行筛选带有expma的数据
        # sql = ‘select * from DayPriceData LEFT JOIN expma ON (DayPriceData.Date=expma.Date AND DayPriceData.Code = expma.Code_ID) where DayPriceData.Code in('1024.HK','3690.HK','0700.HK','0001.HK') AND DayPriceData.Date between "2023-06-01" and "2023-06-05"
        outData = pd.DataFrame()
        outData = pd.read_sql(text(sql), con=con)

        return outData

    def SyncDateBase(self, codeList, startdate, enddate,tablename):
        oneDay=timedelta(days=1)
        #需要将单个的code转化为codelist，满足update函数的运行条件
        for code in codeList:
            codeListBuff = []
            codeListBuff.append(code)
            codeResult = session.query(CodeDateIndex).filter(CodeDateIndex.CODE == code).all()
            if len(codeResult) == 0:
                print(f'表中无该{code}数据,开始获取新数据')
                try:
                    self.UpdateTimePeriodData(codelist=codeListBuff, startDate=startdate, endDate=enddate, tableName='daypricedata')
                except myexception.ExceptionWindNoData as e:
                    print("非交易日，无数据")
                    continue
                self.UpdateTimePeriodDataEMA(codelist=codeListBuff, startDate=startdate, endDate=enddate, tableName='expma')
                self.UpdateCodeIndex(code,startdate, enddate)
                continue

            # code在表中存在的情况下，在表中获取startdate和enddate
            index = session.query(CodeDateIndex).filter(CodeDateIndex.CODE == code).first()
            dbStartDate = index.STARTDATE
            dbEndDate = index.ENDDATE
            # 还有一种情况是需要获取的数据范围包裹住了数据库内的数据范围
            if startdate < dbStartDate and enddate > dbEndDate:
                print('前后方数据都需要补充')
                # 补充前方数据
                startdatebuff = startdate
                endDatebuff = dbStartDate - oneDay
                self.UpdateTimePeriodData(codeListBuff, startdatebuff, endDatebuff, 'daypricedata')
                self.UpdateTimePeriodDataEMA(codeListBuff, startdatebuff, endDatebuff, tableName='expma')
                # 补充后方数据
                startdatebuff = dbEndDate + oneDay
                endDatebuff = enddate
                self.UpdateTimePeriodData(codeListBuff, startdatebuff, endDatebuff, 'daypricedata')
                self.UpdateTimePeriodDataEMA(codeListBuff, startdatebuff, endDatebuff, tableName='expma')

                # 这里边界以startdate和enddate为准
                self.UpdateCodeIndex(code, startdate, enddate)
                continue


            if (startdate>=dbEndDate or enddate<=dbStartDate) and startdate!=enddate:
                #说明数据库中的时间范围和目标时间范围不重叠，中间的空隙需要补上，否则整个逻辑会失效
                if startdate>=dbEndDate:
                    print('数据在后方有空隙，进行填补')
                    startdatebuff = dbEndDate+oneDay
                    endDatebuff = enddate
                    dbStartDateBuff=dbStartDate
                    dbEndDateBuff=endDatebuff
                else:
                    print('数据在前方有空隙，进行填补')
                    startdatebuff = startdate
                    endDatebuff = dbStartDate-oneDay
                    dbStartDateBuff = startdatebuff
                    dbEndDateBuff = dbEndDate
            else:
                if startdate>=dbStartDate and enddate<=dbEndDate:
                    #说明日期在包裹之内，不用重新获取
                    print('已有数据，不用获取')
                    continue
                else:
                    if startdate>=dbStartDate and enddate>dbEndDate:
                        print('补充后方数据')
                        #说明只需要获取后面一部分的数据
                        startdatebuff = dbEndDate+oneDay
                        endDatebuff = enddate
                        dbStartDateBuff = dbStartDate
                        dbEndDateBuff = endDatebuff
                    else:
                        if startdate<dbStartDate and enddate<=dbEndDate:
                            print('补充前方数据')
                            startdatebuff = startdate
                            endDatebuff=dbStartDate-oneDay
                            dbStartDateBuff = startdatebuff
                            dbEndDateBuff = dbEndDate
            #根据日期情况鞥新数据
            try:
                self.UpdateTimePeriodData(codeListBuff, startdatebuff, endDatebuff, 'daypricedata')
            except myexception.ExceptionWindNoData as e:
                print("非交易日，无数据")
                continue
            self.UpdateTimePeriodDataEMA(codeListBuff, startdatebuff, endDatebuff, tableName='expma')
            self.UpdateCodeIndex(code, dbStartDateBuff, dbEndDateBuff)

    #用来同步数据库中的数据时间的函数
    def UpdateCodeIndex(self,code,startDate,endDate):
        # 更新codeindex表的数据
        queryResult = session.query(CodeDateIndex).filter(CodeDateIndex.CODE == code).all()
        if len(queryResult) == 0:
            codeDateIndexbuff = CodeDateIndex(CODE=code, STARTDATE=startDate, ENDDATE=endDate)
            session.add(codeDateIndexbuff)
        else:
            session.query(CodeDateIndex).filter(CodeDateIndex.CODE == code).update({"STARTDATE": startDate, "ENDDATE": endDate})
        session.commit()

class DataPrepare():
    def __int__(self):
        pass

    def DataPreWindDB(self,codelist,startdate,endate):
        #根据日期，代码获取
        gwd = GetWindDaTA()
        gwd.SyncDateBase(codelist, startdate, endate, 'codedateindex')

        #################################测试将数据存进去数据库###########################
        # complexData = gwd.GetTimePeriodData(codelist,startDate,endDate)
        # complexDataEMA =gwd.GetTimePeriodDataEMA(codelist,startDate,endDate)
        complexData = gwd.GetDataBase(codelist, startdate, endate)
        complexDataEMA = gwd.GetDataBaseEMA(codelist, startdate, endate)

        # 声明一个stock类的数组
        stocklist = [stockclass.StockClass for i in range(len(codelist))]
        stocklistIndex = 0
        for code in codelist:
            buffdata = pd.DataFrame()  # 重新声明，避免浅拷贝
            buffEMA = pd.DataFrame()
            buffdata = complexData[complexData['CODE'] == code]
            # 索引重置
            buffdata = buffdata.reset_index(drop=True)

            periods = gwd.emaPeriod  # 获取到设定好的ema值
            for period in periods:
                buffdataEMA = pd.DataFrame()
                buffdataEMA = complexDataEMA[(complexDataEMA['CODE'] == code) & (complexDataEMA['PERIOD'] == period)]
                # 对EMA数据的格式进行操作
                buffEMA['DATE'] = buffdataEMA['DATE'].values
                buffEMA[f'EMA{period}'] = buffdataEMA['EXPMA'].values

            # 这里对buffdata要进行排序
            stockClassBuff = stockclass.StockClass(code, buffdata, buffEMA)
            stocklist[stocklistIndex] = stockClassBuff
            stocklistIndex = stocklistIndex + 1

        return stocklist
