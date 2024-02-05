import pandas as pd
from futu import *
#codeDF = pd.read_csv("D:\ztrade\heatChartUSbuff.csv", encoding="gb2312")
#codeDFHeat = pd.read_csv("D:\ztrade\heatChart.csv", encoding="gb2312")
# codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
#codeDFHeat.rename(columns={'代码':'TRADE_CODE'},inplace=True)
#set_diff_df = pd.concat([codeDFHeat, codeDF, codeDF]).drop_duplicates(keep=False)

#intersected_df = pd.merge(codeDF, codeDFHeat, on=['TRADE_CODE'], how='inner')

#.to_csv("D:\ztrade\heatChartUS.csv", index_label="index_label")

#codeDFHeat['WINDCODE']=''


#codeList = codeDF['WindCodes'].tolist()
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

ret, data, page_req_key = quote_ctx.request_history_kline('SH.000001', start='2019-09-11', end='2019-09-18', max_count=1000,page_req_key=None)  # 每页5个，请求第一页
if ret == RET_OK:
    print(data)
    buffData=pd.DataFrame()
    timeStrSeries=data['time_key']
    timeDateList=[]
    for timeStr in timeStrSeries:
        timeDatetime = datetime.strptime(timeStr, '%Y-%m-%d %H:%M:%S')  # strptime()内参数必须为string格式
        timeDate=datetime.date(timeDatetime)
        timeDateList.append(timeDate)

    buffData['OPEN']=data['open']
    buffData['CLOSE']=data['close']
    buffData['HIGH']=data['high']
    buffData['LOW']=data['low']
    buffData['VOLUME']=data['volume']
    buffData['DATE']=timeDateList

    pass
    #print(data['code'][0])    # 取第一条的
    # 股票代码
    #print(data['close'].values.tolist())   # 第一页收盘价转为 list
else:
    print('error:', data)
while page_req_key != None:  # 请求后面的所有结果
    print('*************************************')
    ret, data, page_req_key = quote_ctx.request_history_kline('HK.00700', start='2019-09-11', end='2019-09-18', max_count=5, page_req_key=page_req_key) # 请求翻页后的数据
    if ret == RET_OK:
        print(data)
    else:
        print('error:', data)
print('All pages are finished!')
quote_ctx.close() # 结束后记得关闭当条连接，防止连接条数用尽