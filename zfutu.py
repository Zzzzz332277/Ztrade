import time

import futu as ft
from futu import *

# 实例化行情上下文对象
quote_ctx = ft.OpenQuoteContext(host="127.0.0.1", port=11111)
# 上下文控制
quote_ctx.start()  # 开启异步数据接收
quote_ctx.set_handler(ft.TickerHandlerBase())  # 设置用于异步处理数据的回调对象(可派生支持自定义)
#处理数据并与富途进行通信的类

############测试自选股功能#####################
'''
codeList=['HK.03678','HK.02291']
ret, data = quote_ctx.modify_user_security('ztrade', ModifyUserSecurityOp.MOVE_OUT, codeList)
if ret == RET_OK:
    print(data)  # 返回 success
else:
    print('error:', data)
'''

#用于转化单个code的函数
def CodeTransWind2FUTU(code):
    codebuff = code.split('.')
    codeNew = codebuff[1] + '.' + '0' + codebuff[0]
    return codeNew


class Zfutu():
    def __init__(self,market):
        self.market=market
        #ema diffusion先不做
        self.recogList = ['backstepema', 'EmaDiffusion','EMAUpCross','MoneyFlow','EMA5BottomArc','EMA5TOPArc','EMADownCross']
        if market=='HK':
            self.listNameList=['backstepemaHK', 'EmaDiffusionHK','EMAUpCrossHK', 'MoneyFlowHK', 'EMA5BottomArcHK','EMA5TOPArcHK','EMADownCrossHK']
        elif market=='US':
            self.listNameList= ['backstepemaUS', 'EmaDiffusionUS','EMAUpCrossUS', 'MoneyFlowUS', 'EMA5BottomArcUS','EMA5TOPArcUS','EMADownCrossUS']
        else:
            print('市场输入错误')
            return

    def FutuDisConnect(self):
        quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽

    def ModifyFutuStockList(self,resultTable):
        codelist=list()
        #########################这里需要注意，对每日关注的列表进行保留操作，以免删除ztrade时候也删除了自选股###########################################

        if self.market=='HK':
            watchList='每日关注'
        elif self.market=='US':
            watchList='美股关注'
        else:
            print('市场输入错误')
            return

        ret, everyDayWatchData = quote_ctx.get_user_security(watchList)
        if ret == RET_OK:
            pass
            # print(data)  # 返回 success
        else:
            print('error:', everyDayWatchData)
        codeListEveryDayWatch = everyDayWatchData['code'].tolist()

        ##################################################先讲原先的list清除########################
        self.CleanOutFUTUList(self.listNameList)
        print('等待30S，防止调用futu接口过于频繁')
        time.sleep(30)

        #将resulttable中的结果按识别内容分类，并清除为0的行
        for index,recogName in enumerate(self.recogList):
            resultTableSliced=resultTable[['code',recogName]]
            resultTableSliced=resultTableSliced.loc[~((resultTableSliced[recogName] == 0) )]
            codeList=resultTableSliced['code'].tolist()
            codelistNew=self.CodeTransferWind2FUTU(codeList)
            self.AddFutuList(listname=self.listNameList[index],list=codelistNew)
            time.sleep(1)
            #这里加入等待避免超出接口限制
        ###################################再恢复每日关注的股票#####################################
        self.AddFutuList(listname=watchList, list=codeListEveryDayWatch)

    #将wind的代码与富途进行转化
    def CodeTransferWind2FUTU(self,codelist):
        codelistNew = list()
        for code in codelist:
            codebuff=code.split('.')
            if self.market == 'HK':
                codeNew=codebuff[1]+'.'+'0'+codebuff[0]
            elif self.market == 'US':
                codeNew = 'US'+'.'+codebuff[0]
            codelistNew.append(codeNew)
        return codelistNew

    def CleanOutFUTUList(self,listnamelist):
        codeListMoveOut=[]
        #需要将代码在wind和futu间转换
        for list in listnamelist:
            ret, data = quote_ctx.get_user_security(list)
            if ret == RET_OK:
                codeListMoveOut=data['code'].tolist()
                # print(data)  # 返回 success
            else:
                print('error:', data)

            # 清空所有自选
            ret, data = quote_ctx.modify_user_security(list, ModifyUserSecurityOp.DEL, codeListMoveOut)
            if ret == RET_OK:
                print(data)  # 返回 success
            else:
                print('error:', data)

    def AddFutuList(self,listname,list):
        # 再加入新的自选
        ret, data = quote_ctx.modify_user_security(listname, ModifyUserSecurityOp.ADD, list)
        if ret == RET_OK:
            print(data)  # 返回 success
        else:
            print('error:', data)

