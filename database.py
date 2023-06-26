import datetime
from WindPy import *
import sqlalchemy
import sqlalchemy
#from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Table, Column, Integer, String, Float,Date, MetaData, ForeignKey, desc, inspect
import pymysql
from sqlalchemy.orm import sessionmaker,declarative_base
import pandas as pd
import numpy as np
import stock

# 数据库的地址
DataBaseAddr = {'hostNAME': '127.0.0.1',
                'PORT': '3306',
                'database': 'testz',
                'username': 'root',
                'password': 'xinxikemysql'}

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
tableKeyWordForWind={'DayPriceData':'open,high,low,close,volume',
                     'ExpMA':'"EXPMA_N=5,"EXPMA_N=10,"EXPMA_N=20,"EXPMA_N=30,"EXPMA_N=60'}

class StockCode(Base):
    __tablename__ = 'StockCode'
    Id = Column(Integer, primary_key=True)
    Code=Column(String(60))

class DayPriceData(Base):
    __tablename__ = 'DayPriceData'
    Id = Column(Integer, primary_key=True)
    Code=Column(String(60))
    #Code_ID=Column(Integer, ForeignKey('StockCode.Id'))
    Date= Column(Date)
    Open= Column(Float)
    Close= Column(Float)
    High= Column(Float)
    Low= Column(Float)
    Volume=Column(Float)

class MoneyFlowData(Base):
    __tablename__ = 'MoneyFlow'
    Id = Column(Integer, primary_key=True)
    #Code_ID=Column(Integer, ForeignKey('StockCode.Id'))
    Date= Column(Date)
    #futu‘s moneyflow
    LargeOrder = Column(Float)
    BigOrder = Column(Float)
    MidOrder = Column(Float)
    SmallOrder = Column(Float)

class ExpMA(Base):
    __tablename__ = 'ExpMA'
    Id = Column(Integer, primary_key=True)
    #Code_ID=Column(Integer, ForeignKey('StockCode.Id'))
    Date= Column(Date)
    EMA5= Column(Float)
    EMA10= Column(Float)
    EMA20= Column(Float)
    EMA30= Column(Float)
    EMA60=Column(Float)
    EMA120=Column(Float)

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
            # 这里取出的是没有时间的。
            data = pd.DataFrame(buffData.Data, index=buffData.Fields)
            data = data.T
            data['Date'] = buffData.Times
            data['Code'] = code
            data.to_sql(name=tableName, con=con, if_exists="append",index=False)

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
            buffRow['Date'] = today
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
                buffData=w.wsd(code,tableKeyWordForWind[tableName],startDateStr, todayStr, "unit=1;traderType=1;TradingCalendar=HKEX;PriceAdj=F")
                data = pd.DataFrame(buffData.Data, index=buffData.Fields)
                data = data.T
                data['Date'] = buffData.Times
                data['Code'] = code
                data.to_sql(name=tableName,con=con,if_exists="append",index=False)

#准备数据的类
class DataPreparation:
    def __init__(self):
        pass

    #将数据库的数据查询出来存储在一个dataframe里，以供后续进行筛选，处理。需要细化传入的是date类型还是字符串
    def GetDataBase(self, codeList, startdate, enddate):
        codeListStr=""
        for code in codeList:
            codeListStr = codeListStr+"'"+code+"'"+','
        codeListStr=codeListStr.rstrip(',')

        startDateStr = startdate.strftime('%Y-%m-%d')
        endDateStr = enddate.strftime('%Y-%m-%d')

        sql = f'select * from DayPriceData where Code in({codeListStr}) AND Date between "{startDateStr}" and "{endDateStr}"'
        #left join进行筛选带有expma的数据
        #sql = ‘select * from DayPriceData LEFT JOIN expma ON (DayPriceData.Date=expma.Date AND DayPriceData.Code = expma.Code_ID) where DayPriceData.Code in('1024.HK','3690.HK','0700.HK','0001.HK') AND DayPriceData.Date between "2023-06-01" and "2023-06-05"
        outData = pd.DataFrame()
        outData=pd.read_sql(sql, con)

        result= []
        testLocals=[]
        testDict={}

        for code in codeList:
            codeData=outData[outData['Code']==code]
            result.append(codeData)

            testDict['stock'+code] = stock.StockClass(code=code,dayPriceDataFrame=codeData)



        print(testDict)

        pass
        #return outData
