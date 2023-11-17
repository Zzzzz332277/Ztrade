import pandas as pd
import requests
from bs4 import BeautifulSoup
import  traceback
import re
import ast

import crawler
from selenium import webdriver
from bs4 import BeautifulSoup

EastmoneyCodeDic={
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

