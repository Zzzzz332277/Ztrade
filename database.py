import datetime
from WindPy import *
import sqlalchemy
#from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text,create_engine, Table, Column, Integer, String, Float,Date, MetaData, ForeignKey, desc, inspect
import pymysql
from sqlalchemy.orm import sessionmaker,declarative_base
import pandas as pd
import numpy as np

import recognition
import stockclass
import myexception
import time
import basic
import database
from futu import *
import futu as ft
import zfutu


# 数据库的地址:HK市場
DataBaseAddr = {'hostNAME': '127.0.0.1',
                'PORT': '3306',
                'database': 'ztrade',
                'username': 'root',
                'password': 'mysqlzph'}
#US市場
DataBaseAddrUS = {'hostNAME': '127.0.0.1',
                'PORT': '3306',
                'database': 'ztradeUS',
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

engineUS = sqlalchemy.create_engine(f"mysql+pymysql://{DataBaseAddrUS['username']}:{DataBaseAddrUS['password']}@localhost:3306/{DataBaseAddrUS['database']}")
conUS = engineUS.connect()

#这里要分析下con和engine的区别
Session = sessionmaker(bind=engine)
session=Session()

SessionUS = sessionmaker(bind=engineUS)
sessionUS=SessionUS()

#metadata = MetaData(bind=engine)
#用于获取到标的字段数据
insp = inspect(engine)
Base = declarative_base()

#####################################数据库的连接######################################

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

class KDJ(Base):
    __tablename__ = 'KDJ'
    ID = Column(Integer, primary_key=True)
    CODE=Column(String(60))

    #Code_ID=Column(Integer, ForeignKey('StockCode.Id'))
    DATE= Column(Date)
    K = Column(Float)
    D = Column(Float)
    J = Column(Float)

#rsi的处理比较特殊，因为和EMA一样涉及到过往数据的均线计算，所以将中间过程UP/DOWN SMA存储下来
class RSI(Base):
    __tablename__ = 'RSI'
    ID = Column(Integer, primary_key=True)
    CODE=Column(String(60))

    #Code_ID=Column(Integer, ForeignKey('StockCode.Id'))
    DATE= Column(Date)
    UPSMA= Column(Float)
    DOWNSMA= Column(Float)
    PERIOD= Column(Integer)

class CodeDateIndex(Base):
    __tablename__ = 'CodeDateIndex'
    ID = Column(Integer, primary_key=True)
    CODE=Column(String(60))
    STARTDATE= Column(Date)
    ENDDATE= Column(Date)

#用于存放技术指标的起始时间的数据
class TechDateIndex(Base):
    __tablename__ = 'TechDateIndex'
    ID = Column(Integer, primary_key=True)
    CODE = Column(String(60))
    STARTDATE = Column(Date)
    ENDDATE = Column(Date)
    TECHINDEXTYPE = Column(String(60))

class CapitalFlow(Base):
    __tablename__ = 'CapitalFlow'
    ID = Column(Integer, primary_key=True)
    CODE=Column(String(60))

    #Code_ID=Column(Integer, ForeignKey('StockCode.Id'))
    DATE= Column(Date)
    Super_In_FLow= Column(Float)
    Big_In_FLow= Column(Float)
    Mid_In_FLow= Column(Float)
    Sml_In_FLow= Column(Float)

####################################创建表格###############################################################
Base.metadata.create_all(engine)
Base.metadata.create_all(engineUS)
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

    def __init__(self,con,engine,session,tradingcalendar):
        #初始化时配置数据库连接
        self.con=con
        self.engine=engine
        self.session=session
        self.tradingCalendar=tradingcalendar
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

        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()
        for code in codelist:
            buffData = w.wsd(code,tableKeyWordForWind[tableName],statrTimeStr, endTimeStr, f"TradingCalendar={self.tradingCalendar};PriceAdj=F")
            print(f"获取{code}的日线数据")
            if buffData.Data[0][0]=='CWSDService: No data.':
                #抛出无数据的异常
                raise myexception.ExceptionWindNoData('CWSDService: No data.')
            data = pd.DataFrame(buffData.Data, index=buffData.Fields)
            data = data.T
            #检查数据是否有nan，防止nan进入数据库
            if self.CheckDataHasNan(data):
                print('数据中含有nan，无法使用')
                continue
            data['DATE'] = buffData.Times
            data['CODE'] = code
            data.to_sql(name=tableName, con=self.engine,if_exists="append",index=False)
            self.con.commit()

    #获取给定单一code的数据，成功返回1，失败返回0
    def UpdateTimePeriodDataSingle(self, code, startDate, endDate, tableName):
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()

        buffData = w.wsd(code, tableKeyWordForWind[tableName], statrTimeStr, endTimeStr,
                         f"TradingCalendar={self.tradingCalendar};PriceAdj=F")
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

    #获取EMA数据并写入数据库
    def UpdateTimePeriodDataEMA(self, codelist, startDate, endDate,tableName):
        concatDataframe = pd.DataFrame(columns=["EXPMA","DATE","CODE","PERIOD"])
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()

        for code in codelist:
            for period in basic.emaPeriod:
                start_time = time.time()
                buffData = w.wsd(code, "EXPMA", statrTimeStr, endTimeStr, f"EXPMA_N={period};TradingCalendar={self.tradingCalendar};PriceAdj=F")
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
                end_time = time.time()
                print("耗时: {:.2f}秒".format(end_time - start_time))

        #全部一次性写入
        print("开始写入数据库")
        concatDataframe.to_sql(name=tableName, con=self.engine,if_exists="append",index=False)
        self.con.commit()

    #直接从wind获取kdj指标
    def UpdateTimePeriodDataKDJ(self, codelist, startDate, endDate, tableName):
        # 合并字符串
        KDJType=['1','2','3']
        concatDataframe = pd.DataFrame(columns=["KDJ", "DATE", "CODE", "TYPE"])
        statrTimeStr = startDate.strftime("%Y%m%d")
        endTimeStr = endDate.strftime("%Y%m%d")
        w.start()

        for code in codelist:
            for type in KDJType:
                start_time = time.time()
                buffData = w.wsd(code, "KDJ", statrTimeStr, endTimeStr,
                                 f"KDJ_N=9;KDJ_M1=3;KDJ_M2=3;KDJ_IO={type};TradingCalendar={self.tradingCalendar};PriceAdj=F")
                # 这里取出的是没有时间的。
                print(f"获取{code}的KDJ的{type}数据")
                data = pd.DataFrame(buffData.Data, index=buffData.Fields)
                data = data.T
                # 检查数据是否有nan，防止nan进入数据库
                if self.CheckDataHasNan(data):
                    print('数据中含有nan，无法使用')
                    continue
                data['DATE'] = buffData.Times
                data['CODE'] = code
                data['TYPE'] = type
                concatDataframe = pd.concat([concatDataframe, data])
                end_time = time.time()
                print("耗时: {:.2f}秒".format(end_time - start_time))

        # 全部一次性写入
        print("开始写入数据库")
        concatDataframe.to_sql(name=tableName, con=self.engine, if_exists="append", index=False)
        self.con.commit()

    # 获取给定单一code的CapitialFLow数据，保存入数据库,适用于futu平台
    def UpdateTimePeriodCapitalSingle(self, code, startDate, endDate, tableName):
        codeList=[]
        codeList.append(code)
        codeListNew=self.CodeTransferWind2FUTU(codeList)
        codeNew=codeListNew[0]
        statrTimeStr = startDate.strftime("%Y-%m-%d")
        endTimeStr = endDate.strftime("%Y-%m-%d")
        #从富途获取数据
        ret, data = zfutu.quote_ctx.get_capital_flow(codeNew, period_type = PeriodType.DAY,start=statrTimeStr,end=endTimeStr)
        #休息1s，避免超出30S30次的限制
        time.sleep(1)
        if ret == RET_OK:
            print(f"获取{code}的资金流数据")
        else:
            print('error:', data)
            return 0

        buffData=data.loc[:,['super_in_flow','big_in_flow','mid_in_flow','sml_in_flow']]
        buffData['CODE']=code
        buffData['DATE']=data['capital_flow_item_time']

        # 检查数据是否有nan，防止nan进入数据库
        if self.CheckDataHasNan(buffData):
            print('数据中含有nan，无法使用')
            return 0
        buffData.to_sql(name=tableName, con=self.engine, if_exists="append", index=False)
        self.con.commit()
        return 1

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
            buffData = w.wsd(code,'open,high,low,close,volume',statrTimeStr, endTimeStr, f"TradingCalendar={self.tradingCalendar};PriceAdj=F")
            print(f"获取{code}的日线数据")
            # 这里取出的是没有时间的。
            data = pd.DataFrame(buffData.Data, index=buffData.Fields)
            data = data.T
            # 检查数据是否有nan，防止nan进入数据库
            if self.CheckDataHasNan(data):
                print('数据中含有nan，无法使用')
                continue
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
            for period in basic.emaPeriod:
                start_time = time.time()
                buffData = w.wsd(code, "EXPMA", statrTimeStr, endTimeStr, f"EXPMA_N={period};TradingCalendar={self.tradingCalendar};PriceAdj=F")
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
        # 检查数据是否有nan，防止nan进入数据库
        if self.CheckDataHasNan(data):
            print('数据中含有nan，无法使用')
            return
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
            dayResult = self.session.query(DayPriceData).filter(DayPriceData.Code==code).order_by(desc(DayPriceData.Date)).first()
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
                buffData = w.wsd(code, tableKeyWordForWind[tableName], startDateStr, todayStr,f"TradingCalendar={self.tradingCalendar};PriceAdj=F")
                data = pd.DataFrame(buffData.Data, index=buffData.Fields)
                data = data.T
                # 检查数据是否有nan，防止nan进入数据库
                if self.CheckDataHasNan(data):
                    print('数据中含有nan，无法使用')
                    continue
                data['DATE'] = buffData.Times
                data['CODE'] = code
                data.to_sql(name=tableName,con=self.engine,if_exists="append",index=False)
                self.con.commit()

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
            dayResult = self.session.query(ExpMA).filter(ExpMA.Code == code).order_by(desc(ExpMA.Date)).first()
            # 更新至最新的值，判断一下是不是5点，不然更新前一天
            # 判断 if 时间是五点之前更新前一天，否则后一探
            if dayResult.Date == today:
                print("Already Newest!")
            else:
                # 从wind进行升级
                startDate = dayResult.Date + timedelta(days=1)
                startDateStr = startDate.strftime("%Y%m%d")
                # 从tablename对应的字典来查找数据
                for period in basic.emaPeriod:
                    buffData = w.wsd(code, "EXPMA", startDateStr, todayStr,f"EXPMA_N={period};TradingCalendar={self.tradingCalendar};PriceAdj=F")
                    print(f"获取{code}的{period}ema数据")
                    # 这里取出的是没有时间的。
                    data = pd.DataFrame(buffData.Data, index=buffData.Fields)
                    data = data.T
                    # 检查数据是否有nan，防止nan进入数据库
                    if self.CheckDataHasNan(data):
                        print('数据中含有nan，无法使用')
                        continue
                    data['DATE'] = buffData.Times
                    data['CODE'] = code
                    data['PERIOD'] = period
                    concatDataframe = pd.concat([concatDataframe, data])
                    # 全部一次性写入
        print("开始写入数据库")
        concatDataframe.to_sql(name=tableName, con=self.engine, if_exists="append", index=False)
        self.con.commit()

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
        #和tosql不一樣，一個用con用，egine，一個用con，
        outData=pd.read_sql(text(sql), con=self.con)
        outData = outData.sort_values(by="DATE", ascending=True)
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
        outData = pd.read_sql(text(sql), con=self.con)
        outData = outData.sort_values(by="DATE", ascending=True)
        return outData

    #用来更新数据库时间轴
    def SyncDateBase(self, codeList, startdate, enddate,tablename):
        oneDay=timedelta(days=1)
        #需要将单个的code转化为codelist，满足update函数的运行条件
        for code in codeList:
            codeListBuff = []
            codeListBuff.append(code)
            codeResult = self.session.query(CodeDateIndex).filter(CodeDateIndex.CODE == code).all()
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
            index = self.session.query(CodeDateIndex).filter(CodeDateIndex.CODE == code).first()
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
                self.UpdateCodeIndex(code, startdate, enddate,self.session)
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
            self.UpdateCodeIndex(code, dbStartDateBuff, dbEndDateBuff,self.session)

    #这里只更新日线数据，去除ema的更新，所有技术指标的计算放在计算模块中进行
    def SyncDateBaseDayPirceData(self, codeList, startdate, enddate,tablename):
        oneDay=timedelta(days=1)
        #需要将单个的code转化为codelist，满足update函数的运行条件
        for code in codeList:

            codeResult = self.session.query(CodeDateIndex).filter(CodeDateIndex.CODE == code).all()
            if len(codeResult) == 0:
                print(f'表中无该{code}数据,开始获取新数据')
                try:
                    result=self.UpdateTimePeriodDataSingle(code=code, startDate=startdate, endDate=enddate, tableName='daypricedata')
                except myexception.ExceptionWindNoData as e:
                    print("非交易日，无数据")
                    continue
                if result: self.UpdateCodeIndex(code,startdate, enddate,self.session)
                continue

            # code在表中存在的情况下，在表中获取startdate和enddate
            index = self.session.query(CodeDateIndex).filter(CodeDateIndex.CODE == code).first()
            dbStartDate = index.STARTDATE
            dbEndDate = index.ENDDATE
            # 还有一种情况是需要获取的数据范围包裹住了数据库内的数据范围
            if startdate < dbStartDate and enddate > dbEndDate:
                print('前后方数据都需要补充')
                # 补充前方数据并更新前方index
                startdatebuff = startdate
                endDatebuff = dbStartDate - oneDay
                result=self.UpdateTimePeriodDataSingle(code, startdatebuff, endDatebuff, 'daypricedata')
                if result: self.UpdateCodeIndex(code,startdate, dbEndDate,self.session)

                # 补充后方数据并更新后方index
                startdatebuff = dbEndDate + oneDay
                endDatebuff = enddate
                result=self.UpdateTimePeriodDataSingle(code, startdatebuff, endDatebuff, 'daypricedata')
                # 这里边界以startdate和enddate为准
                if result:self.UpdateCodeIndex(code, startdate, enddate,self.session)
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
                result=self.UpdateTimePeriodDataSingle(code, startdatebuff, endDatebuff, 'daypricedata')
            except myexception.ExceptionWindNoData as e:
                print("非交易日，无数据")
                continue
            if result:self.UpdateCodeIndex(code, dbStartDateBuff, dbEndDateBuff,self.session)

    def SyncDataBaseCapitalFLow(self, codeList, startdate, enddate,tablename):
        oneDay=timedelta(days=1)
        #需要将单个的code转化为codelist，满足update函数的运行条件
        for code in codeList:

            CapFlowIndex = self.session.query(database.TechDateIndex).filter(database.TechDateIndex.CODE == code,database.TechDateIndex.TECHINDEXTYPE == 'CapitalFlow').first()
            if CapFlowIndex == None:
                print(f'表中无该{code}数据,开始获取新数据')
                try:
                    result=self.UpdateTimePeriodCapitalSingle(code=code, startDate=startdate, endDate=enddate, tableName='capitalflow')
                except myexception.ExceptionWindNoData as e:
                    print("非交易日，无数据")
                    continue
                if result: self.UpdateTechIndex(code,startdate, enddate,self.session,'capitalflow')
                continue

            # code在表中存在的情况下，在表中获取startdate和enddate
            dbStartDate = CapFlowIndex.STARTDATE
            dbEndDate = CapFlowIndex.ENDDATE
            # 还有一种情况是需要获取的数据范围包裹住了数据库内的数据范围
            if startdate < dbStartDate and enddate > dbEndDate:
                print('前后方数据都需要补充')
                # 补充前方数据并更新前方index
                startdatebuff = startdate
                endDatebuff = dbStartDate - oneDay
                result=self.UpdateTimePeriodCapitalSingle(code, startdatebuff, endDatebuff, 'capitalflow')
                if result: self.UpdateTechIndex(code,startdate, dbEndDate,self.session, 'capitalflow')

                # 补充后方数据并更新后方index
                startdatebuff = dbEndDate + oneDay
                endDatebuff = enddate
                result=self.UpdateTimePeriodCapitalSingle(code, startdatebuff, endDatebuff, 'capitalflow')
                # 这里边界以startdate和enddate为准
                if result:self.UpdateTechIndex(code, startdate, enddate,self.session,'capitalflow')
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
                result=self.UpdateTimePeriodCapitalSingle(code, startdatebuff, endDatebuff, 'capitalflow')
            except myexception.ExceptionWindNoData as e:
                print("非交易日，无数据")
                continue
            if result:self.UpdateTechIndex(code, dbStartDateBuff, dbEndDateBuff,self.session,'capitalflow')

    #用来同步数据库中的数据时间的函数
    def UpdateCodeIndex(self,code,startDate,endDate,session):
        # 更新codeindex表的数据
        queryResult = session.query(CodeDateIndex).filter(CodeDateIndex.CODE == code).all()
        if len(queryResult) == 0:
            codeDateIndexbuff = CodeDateIndex(CODE=code, STARTDATE=startDate, ENDDATE=endDate)
            session.add(codeDateIndexbuff)
        else:
            session.query(CodeDateIndex).filter(CodeDateIndex.CODE == code).update({"STARTDATE": startDate, "ENDDATE": endDate})
        session.commit()

    def CheckDataHasNan(self,dataframe):
        check_for_nan = dataframe.isnull().values.any()
        if check_for_nan==True:
            return 1
        else:
            return 0

    #用来进行code的函数
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

    #升级techindex的函数
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

    #用于对数据库进行维护的函数，确保无重复，无nan和0数据
    def DatabaseMainten(self):
        pass

class DataPrepare():

    def __init__(self, con, engine, session, tradingcalendar):
        # 初始化时配置数据库连接
        self.con = con
        self.engine = engine
        self.session = session
        self.tradingCalendar = tradingcalendar
        pass

    def DataPreWindDB(self,codelist,startdate,endate):
        #根据日期，代码获取
        gwd = GetWindDaTA(self.con,self.engine,self.session,self.tradingCalendar)
        print('同步数据库')
        gwd.SyncDateBaseDayPirceData(codelist, startdate, endate, 'codedateindex')
        #gwd.SyncDataBaseCapitalFLow(codelist, startdate, endate, 'capitalflow')
        #计算ema数据和kdj数据
        tix=recognition.TechIndex(self.con,self.engine,self.session,self.tradingCalendar)
        print('计算技术指标')
        tix.CalcKDJ(codelist)
        tix.CalAllEMA(codelist)

        #################################测试将数据存进去数据库###########################
        # complexData = gwd.GetTimePeriodData(codelist,startDate,endDate)
        # complexDataEMA =gwd.GetTimePeriodDataEMA(codelist,startDate,endDate)
        complexData = gwd.GetDataBase(codelist, startdate, endate)
        complexDataEMA = gwd.GetDataBaseEMA(codelist, startdate, endate)

        # 声明一个stock类的数组
        stocklist = [stockclass.StockClass for i in range(len(codelist))]
        stocklistIndex = 0
        for code in codelist:
            print(f'准备{code}的数据')
            buffdata = pd.DataFrame()  # 重新声明，避免浅拷贝
            buffEMA = pd.DataFrame()
            buffdata = complexData[complexData['CODE'] == code]
            # 索引重置
            buffdata = buffdata.reset_index(drop=True)
            #排序顺序
            buffdata = buffdata.sort_values(by="DATE",ascending=True)

            periods = basic.emaPeriod  # 获取到设定好的ema值

            for period in periods:
                buffdataEMA = pd.DataFrame()
                buffdataEMA = complexDataEMA[(complexDataEMA['CODE'] == code) & (complexDataEMA['PERIOD'] == period)]
                buffdataEMA = buffdataEMA.sort_values(by="DATE", ascending=True)
                # 对EMA数据的格式进行操作
                buffEMA['DATE'] = buffdataEMA['DATE'].values
                buffEMA[f'EMA{period}'] = buffdataEMA['EXPMA'].values

            # 这里对buffdata要进行排序
            stockClassBuff = stockclass.StockClass(code, buffdata, buffEMA,self.tradingCalendar)
            stocklist[stocklistIndex] = stockClassBuff
            stocklistIndex = stocklistIndex + 1

        return stocklist

#根据输入的市场配置数据库和wind字符
def MarketSetting(market):
    match market:
        case 'HK':
            con=database.con
            engine=database.engine
            session=database.session
            tradingCalendar='HKEX'
        case 'US':
            con = database.conUS
            engine = database.engineUS
            session=database.sessionUS
            tradingCalendar = 'NYSE'
        case _:
            print('市场输入错误')
            return 0

    return con,engine,session,tradingCalendar