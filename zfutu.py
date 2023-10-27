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
        self.recogList = ['backstepema', 'EMAUpCross', 'MoneyFlow', 'EMA5BottomArc']
        if market=='HK':
            self.listNameList=['backstepemaHK', 'EMAUpCrossHK', 'MoneyFlowHK', 'EMA5BottomArcHK']
        elif market=='US':
            self.listNameList= ['backstepemaUS', 'EMAUpCrossUS', 'MoneyFlowUS', 'EMA5BottomArcUS']
        else:
            print('市场输入错误')
            return

    def FutuDisConnect(self):
        quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽

    def ModifyFutuStockList(self,resultTable):
        codelist=list()
        #listNameList= ['backstepema', 'EmaDiffusion', 'EMAUpCross','MoneyFlow','EMA5BottomArc']
        #去除emaDiffusion，减少接口调用次数
        #########################这里需要注意，对每日关注的列表进行保留操作，以免删除ztrade时候也删除了自选股###########################################
        ret, everyDayWatchData = quote_ctx.get_user_security('每日关注')
        if ret == RET_OK:
            pass
            # print(data)  # 返回 success
        else:
            print('error:', everyDayWatchData)
        codeListEveryDayWatch = everyDayWatchData['code'].tolist()

        ##################################################先讲原先的list清除########################
        self.CleanOutFUTUList(self.listNameList)
        #将resulttable中的结果按识别内容分类，并清除为0的行
        for index,recogName in enumerate(self.recogList):
            resultTableSliced=resultTable[['code',recogName]]
            resultTableSliced=resultTableSliced.loc[~((resultTableSliced[recogName] == 0) )]
            codeList=resultTableSliced['code'].tolist()
            codelistNew=self.CodeTransferWind2FUTU(codeList)
            self.AddFutuList(listname=self.listNameList[index],list=codelistNew)
            time.sleep(0.1)
        ###################################再恢复每日关注的股票#####################################
        time.sleep(1)
        self.AddFutuList(listname='每日关注', list=codeListEveryDayWatch)

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
        #需要将代码在wind和futu间转换
        for list in listnamelist:
            ret, data = quote_ctx.get_user_security(list)
            if ret == RET_OK:
                pass
                # print(data)  # 返回 success
            else:
                print('error:', data)
            codeListMoveOut = data['code'].tolist()

            # 挨个清空自选
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

