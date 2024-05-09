import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
import  traceback
import re
import ast
import time
from datetime import date, timedelta

import crawler
from selenium import webdriver
from bs4 import BeautifulSoup
#处理字典转换的安全性问题
false = False
true = True
null = ''

EastmoneyCodeDic={
        '外盘':"f49",
        '内盘':"f161",
        "code":"f57",
        "name":"f58",
        'eps':"f92",
        '换手率':"f168"
}
EastmoneyCodeDicIndex={
        '外盘':"f49",
        '内盘':"f161",
        "code":"f57",
        "name":"f58",
        'eps':"f92",
        '换手率':"f168"
}

'''
databuff=pd.DataFrame(columns=['code','sellBid','buyBid','turnOverRate'])
codeDF = pd.read_csv("D:\ztrade\codes.csv")
# codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
codeList = codeDF['WindCodes'].tolist()
databuff['code']=codeList
databuff.set_index('code',inplace=True)
'''

#用于从东财接口查询的url
urlHead='https://push2.eastmoney.com/api/qt/stock/get?invt=2&fltt=1&fields=f58%2Cf107%2Cf57%2Cf43%2Cf59%2Cf169%2Cf170%2Cf152%2Cf46%2Cf60%2Cf44%2Cf45%2Cf47%2Cf48%2Cf19%2Cf532%2Cf39%2Cf161%2Cf49%2Cf171%2Cf50%2Cf86%2Cf600%2Cf601%2Cf154%2Cf84%2Cf85%2Cf168%2Cf108%2Cf116%2Cf167%2Cf164%2Cf92%2Cf71%2Cf117%2Cf177%2Cf123%2Cf124%2Cf125%2Cf174%2Cf175%2Cf126%2Cf257%2Cf256%2Cf258%2Cf251%2Cf255%2Cf252%2Cf254%2Cf253%2Cf198%2Cf292%2Cf301%2Cf752%2Cf751&secid='
urlTail='&ut=fa5fd1943c7b386f172d6893dbfba10b&wbp2u=%7C0%7C0%7C0%7Cweb&_=1699431497060'
eastmoneyCode='116.'+'01385'
headers={'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'}

#期指接口url
futureurl='https://data.eastmoney.com/IF/Chart/VS.html?&va=IF&ct=IF2312&me=10102950'
#pagesize是1，只获取一个值
futureapiurlhead='https://datacenter-web.eastmoney.com/api/data/v1/get?&reportName=RPT_FUTU_NET_POSITION&columns=SECURITY_CODE%2CSECURITY_INNER_CODE%2CSECURITY_NAME%2CTYSECURITY_INNER_CODE%2CTRADE_DATE%2CSETTLE_PRICE%2CLP_CHANGE_TOTAL%2CTOTAL_LONG_POSITION%2CTOTAL_SHORT_POSITION%2CSP_CHANGE_TOTAL%2CNET_POSITION%2CCLOSE_PRICE%2CCLOSE_PRICE_CHANGE%2CBASIS&filter=(SECURITY_CODE%3D%22'
futureapiurltail='%22)&pageNumber=1&pageSize=1&sortTypes=-1&sortColumns=TRADE_DATE&source=WEB&client=WEB&_=1700631215427'
##############################################################################################################################################################

#沪市：
urlSH='https://push2.eastmoney.com/api/qt/ulist.np/get?&secids=1.000001&fields=f62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf64%2Cf65%2Cf70%2Cf71%2Cf76%2Cf77%2Cf82%2Cf83%2Cf164%2Cf166%2Cf168%2Cf170%2Cf172%2Cf252%2Cf253%2Cf254%2Cf255%2Cf256%2Cf124%2Cf6%2Cf278%2Cf279%2Cf280%2Cf281%2Cf282&ut=b2884a393a59ad64002292a3e90d46a5&_=1700727803637'
urlSZ='https://push2.eastmoney.com/api/qt/ulist.np/get?&fltt=2&secids=0.399001&fields=f62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf64%2Cf65%2Cf70%2Cf71%2Cf76%2Cf77%2Cf82%2Cf83%2Cf164%2Cf166%2Cf168%2Cf170%2Cf172%2Cf252%2Cf253%2Cf254%2Cf255%2Cf256%2Cf124%2Cf6%2Cf278%2Cf279%2Cf280%2Cf281%2Cf282&ut=b2884a393a59ad64002292a3e90d46a5&_=1700727959888'

#北向资金：
urlNorthMoney='https://datacenter-web.eastmoney.com/api/data/v1/get?&reportName=RPT_MUTUAL_QUOTA&columns=TRADE_DATE%2CMUTUAL_TYPE%2CBOARD_TYPE%2CMUTUAL_TYPE_NAME%2CFUNDS_DIRECTION%2CINDEX_CODE%2CINDEX_NAME%2CBOARD_CODE&quoteColumns=status~07~BOARD_CODE%2CdayNetAmtIn~07~BOARD_CODE%2CdayAmtRemain~07~BOARD_CODE%2CdayAmtThreshold~07~BOARD_CODE%2Cf104~07~BOARD_CODE%2Cf105~07~BOARD_CODE%2Cf106~07~BOARD_CODE%2Cf3~03~INDEX_CODE~INDEX_f3%2CnetBuyAmt~07~BOARD_CODE&quoteType=0&pageNumber=1&pageSize=200&sortTypes=1&sortColumns=MUTUAL_TYPE&source=WEB&client=WEB&_=1701937642492'

#从yahoo Finance获取财经日历数据
urlYahooEarningsCalenderHead='https://finance.yahoo.com/calendar/earnings?'
tail='from=2024-04-14&to=2024-04-20&day=2024-04-16'


def getHTMLText(url):
    code = 'UTF-8'
    try:
        r = requests.get(url=url,headers=headers)
        r.raise_for_status()
        #r.encoding = r.apparent_encoding
        r.encoding = code
        return r.text
    except:
        return ""


def GetEastMoneyData(code):
    codebuff = code.split('.')
    url=urlHead+'116.0'+codebuff[0]+urlTail
    print(f'获取{code}数据')
    html=getHTMLText(url)
    htmlJson=ast.literal_eval(html)
    #内外盘及换手率数据
    eastMoneyData=htmlJson['data']
    sellBid=eastMoneyData['f161']
    buyBid=eastMoneyData['f49']
    turnOverRate=eastMoneyData['f168']/10000
    return sellBid,buyBid,turnOverRate

#获取到今日的指定的指数期货的持仓信息并返回
def GetEastMoneyIndexFutureData():
    today=datetime.datetime.today()
    year=today.year-2000
    month=today.month
    day=today.day

    securityCode='IF'+str(year)+str(month)
    url=futureapiurlhead+securityCode+futureapiurltail
    html = getHTMLText(url)
    htmlJson = eval(html)
   #多级取值后的比较难看的写法
    result=htmlJson.get('result').get('data')[0]
    return result

def GetEastMoneyIndexData(url):
    html = getHTMLText(url)
    htmlJson = eval(html)
   #多级取值后的比较难看的写法
    data=htmlJson.get('data').get('diff')[0]
    result={}
    result['sup_flow_in']=data['f64']
    result['sup_flow_out']=data['f65']

    result['big_flow_in']=data['f70']
    result['big_flow_out']=data['f71']

    result['mid_flow_in']=data['f76']
    result['mid_flow_out']=data['f77']

    result['smal_flow_in']=data['f82']
    result['smal_flow_out']=data['f83']

    return result

def GetNorthMoneyData(url):
    html = getHTMLText(url)
    htmlJson = eval(html)
   #多级取值后的比较难看的写法
    dataSH=htmlJson.get('result').get('data')[0]
    dataSZ=htmlJson.get('result').get('data')[2]

    moneyNorth2SH=dataSH['dayNetAmtIn']
    moneyNorth2SZ=dataSZ['dayNetAmtIn']

    totalMoney=moneyNorth2SH+moneyNorth2SZ
    return totalMoney

#GetNorthMoneyData(urlNorthMoney)

def GetYahooEarningsCalenderData(date):
    urlYahooEarningsCalenderHead = 'https://finance.yahoo.com/calendar/earnings?'
    dateStr = date.strftime("%Y-%m-%d")
    tail = 'day='+dateStr
    url=urlYahooEarningsCalenderHead+tail
    #print(f'获取{code}数据')

    html=getHTMLText(url)
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.find_all('tr')
    simbols=soup.find_all(name='a',attrs={'data-test':"quoteLink"})
    nameList=list()
    for simbol in simbols:
        print(simbol.text)
        nameList.append(simbol.text)
    nameList=pd.Series(nameList)
    nameList=nameList.drop_duplicates()

    return nameList


#date = date(2024, 4, 18)
#GetYahooEarningsCalenderData(date)