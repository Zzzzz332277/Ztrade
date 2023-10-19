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


class Zfutu():
    def __int__(self):
        pass

    def FutuDisConnect(self):
        quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽

    def ModifyFutuStockList(self,resultTable,listname):
        codelist=list()
        for i in range(len(resultTable)):
            listbuff=resultTable.loc[i]
            #通过设置1和0的flag来判断是否是识别到了
            if (listbuff['backstepema']+listbuff['EmaDiffusion']+listbuff['EMAUpCross']+listbuff['MoneyFlow'])>0:
                codelist.append(listbuff['code'])
        codelistNew=self.CodeTransferWind2FUTU(codelist)
        ret, data = quote_ctx.get_user_security(listname)
        if ret == RET_OK:
            pass
            #print(data)  # 返回 success
        else:
            print('error:', data)
        codeListMoveOut=data['code'].tolist()
        #########################这里需要注意，对每日关注的列表进行保留操作，以免删除ztrade时候也删除了自选股###########################################
        ret, everyDayWatchData = quote_ctx.get_user_security('每日关注')
        if ret == RET_OK:
            pass
            # print(data)  # 返回 success
        else:
            print('error:', data)
        codeListEveryDayWatch = everyDayWatchData['code'].tolist()

        #先清空ztrade自选
        ret, data = quote_ctx.modify_user_security(listname, ModifyUserSecurityOp.DEL,codeListMoveOut)
        if ret == RET_OK:
            print(data)  # 返回 success
        else:
            print('error:', data)
        #再加入新的自选
        ret, data = quote_ctx.modify_user_security(listname, ModifyUserSecurityOp.ADD,codelistNew)
        if ret == RET_OK:
            print(data)  # 返回 success
        else:
            print('error:', data)
        ###################################再恢复每日关注的股票#####################################
        ret, data = quote_ctx.modify_user_security('每日关注', ModifyUserSecurityOp.ADD, codeListEveryDayWatch)
        if ret == RET_OK:
            print(data)  # 返回 success
        else:
            print('error:', data)


    #将wind的代码与富途进行转化
    def CodeTransferWind2FUTU(self,codelist):
        codelistNew=list()
        for code in codelist:
            codebuff=code.split('.')
            codeNew=codebuff[1]+'.'+'0'+codebuff[0]
            codelistNew.append(codeNew)
        return codelistNew
