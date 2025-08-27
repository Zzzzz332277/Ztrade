import csv

import pandas as pd
import numpy as np
import talib
from sqlalchemy import text
import numpy as np
import database
##############################################进行遍历的信号判断###############################################



class KLineSignal():
    def __init__(self):
        pass

    #参考talib直接传入数组
    def BigBaldRed(self,openseries,highseries,lowseries,closeseries):
        reultArray=np.zeros(len(openseries))
        for i in range(len(openseries)):
            open=openseries[i]
            high = highseries[i]
            close=closeseries[i]
            low=lowseries[i]

            bodyLen=abs(open-close)
            #阳线
            if close>=open:
                upShadowLen=abs(high-close)
                downShadowLen=abs(low-open)
                rise = bodyLen/open
            #阴线
            else:
                upShadowLen = abs(high - open)
                downShadowLen = abs(low - close)
                reultArray[i]=0
                continue
            #判断涨幅
            if rise>=0.05 and upShadowLen==0 and downShadowLen==0:
                reultArray[i]=1
            else:
                reultArray[i]=0

        return reultArray
    #底部长腿
    def LongDownLeg(self,openseries,highseries,lowseries,closeseries):
        reultArray=np.zeros(len(openseries))
        for i in range(len(openseries)):
            open=openseries[i]
            high = highseries[i]
            close=closeseries[i]
            low=lowseries[i]

            #阳线
            if close>=open:
                downShadowLen=abs(low-open)
                bodyLen=abs(close-open)
                upShadowLen=abs(high-close)
            #阴线
            else:
                downShadowLen = abs(low - close)
                bodyLen = abs(close - open)
                upShadowLen = abs(high - open)

            #判断涨幅
            if downShadowLen>=(bodyLen+upShadowLen):
                reultArray[i]=1
            else:
                reultArray[i]=0
        return reultArray

class Strategy():
    def __init__(self):
        self.signalProbTableALL = pd.DataFrame(columns=['code', 'RSIOverBuy', 'RSIOverSell', 'KDJOverBuy', 'KDJOverSell',
                                                   'KDJTopArcSignal', 'KDJBottomArcSignal', 'EMA5TopArcSignal',
                                                   'EMA5BottomArcSignal', 'MACDTopArcSignal', 'MACDBottomArcSignal'])

    def SignalProcess(self,stocklist):
        for stock in stocklist:
            print(f'开始计算{stock.code}信号成功率')
            signalTable=pd.DataFrame()
            signalProb=pd.DataFrame(index=[0],columns=['RSIOverBuy','RSIOverSell','KDJOverBuy','KDJOverSell','KDJTopArcSignal','KDJBottomArcSignal','EMA5TopArcSignal','EMA5BottomArcSignal','MACDTopArcSignal','MACDBottomArcSignal'])
            resultArry=[]
            dateSeries=stock.dayPriceData['DATE']
            signalTable['DATE']=dateSeries
            #依次搜索信号并计算概率
            resultArry=self.RSIOverBuySellSignal(stock, 'buy')
            signalTable['RSIOverBuy'] =resultArry
            signalProb['RSIOverBuy']=self.CalSignalProbability(resultArry, stock,'down')

            resultArry = self.RSIOverBuySellSignal(stock, 'sell')
            signalTable['RSIOverSell'] = resultArry
            signalProb['RSIOverSell'] = self.CalSignalProbability(resultArry, stock, 'up')

            resultArry = self.KDJOverBuySellSignal(stock, 'buy')
            signalTable['KDJOverBuy'] = resultArry
            signalProb['KDJOverBuy'] = self.CalSignalProbability(resultArry, stock, 'down')

            resultArry = self.KDJOverBuySellSignal(stock,'sell')
            signalTable['KDJOverSell'] = resultArry
            signalProb['KDJOverSell'] = self.CalSignalProbability(resultArry, stock, 'up')

            resultArry = self.KDJArcSignal(stock, 'top')
            signalTable['KDJTopArcSignal'] = resultArry
            signalProb['KDJTopArcSignal'] = self.CalSignalProbability(resultArry, stock, 'down')

            resultArry = self.KDJArcSignal(stock, 'bottom')
            signalTable['KDJBottomArcSignal'] = resultArry
            signalProb['KDJBottomArcSignal'] = self.CalSignalProbability(resultArry, stock, 'up')

            resultArry = self.EMA5ArcSignal(stock, 'top')
            signalTable['EMA5TopArcSignal'] = resultArry
            signalProb['EMA5TopArcSignal'] = self.CalSignalProbability(resultArry, stock, 'down')

            resultArry = self.EMA5ArcSignal(stock, 'bottom')
            signalTable['EMA5BottomArcSignal'] = resultArry
            signalProb['EMA5BottomArcSignal'] = self.CalSignalProbability(resultArry, stock, 'up')

            resultArry = self.MACDArcSignal(stock, 'top')
            signalTable['MACDTopArcSignal'] = resultArry
            signalProb['MACDTopArcSignal'] = self.CalSignalProbability(resultArry, stock, 'down')

            resultArry = self.MACDArcSignal(stock, 'bottom')
            signalTable['MACDBottomArcSignal'] = resultArry
            signalProb['MACDBottomArcSignal'] = self.CalSignalProbability(resultArry, stock, 'up')

            #统计各个信号的成功率
            stock.signalTable=signalTable
            stock.signalSucessProb=signalProb
            signalProb.insert(0,'code',stock.code)
            #结果写入总的表中
            self.signalProbTableALL = pd.concat([self.signalProbTableALL,signalProb])
            pass

    def WriteToExcel(self):
        #writer = pd.ExcelWriter('test.xlsx')
        print('开始写入数据')
        self.signalProbTableALL.to_excel('Analysis.xlsx', sheet_name="data", index=False, na_rep=0, inf_rep=0)

        #self.signalProbTableALL.to_excel(writer, sheet_name='data')
        # 最后保存
        #writer.save()
        #writer.close()

    def LoadExcel(self,stocklist):
        ProbDF = pd.read_csv("D:\ztrade\Analysis2020.1.1-2024.3.15_us.csv")
        # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
        #将读取到的table分配给各个stock
        pass
        for stock in stocklist:
            stock.signalSucessProb=ProbDF[ProbDF['code']==stock.code]


 #计算60日波动率
    def CalVol60(self,stocklist):
        for stock in stocklist:
            dayPriceDate = stock.dayPriceData
            dayPriceDate = dayPriceDate.sort_values(by="DATE", ascending=True)
            len=dayPriceDate.shape[0]
            if len>-60:
                closeData60=dayPriceDate['CLOSE'].iloc[-60:]
            #不满60天直接取全部
            else:
                closeData60=dayPriceDate['CLOSE']
            logreturns = np.diff(np.log(closeData60))
            # print(logreturns)
            # 股票波动率：是对价格变动的一种衡量。
            # 年股票波动率：对数收益率的标准差除以对数收益率的平均值，然后再除以252个工作日的倒数的平方根。
            Volatility = np.std(logreturns) * np.sqrt(252)
            #保留两位小数
            stock.vol60='{:.2f}'.format(Volatility)


    #参考deepseek给出的计算平滑性的函数
    def calculate_kline_smoothness(self,stocklist):
        """
        计算K线形态平滑度评分
        评估K线的连续性和一致性，避免大幅跳空和剧烈震荡

        参数:
        kline_data: DataFrame，包含OHLCV数据，需要有'open','high','low','close'列

        返回:
        smoothness_score: 平滑度评分(0-100分)
        """
        #测试用数据记录



        for stock in stocklist:
            dayPriceData = stock.dayPriceData
            dayPriceData = dayPriceData.sort_values(by="DATE", ascending=True)
            lenData = dayPriceData.shape[0]
            if lenData < 90:
                #raise ValueError("数据不足90日")
                #不足90日跳过计算
                continue

            # 使用最近90日数据
            data = dayPriceData.iloc[-90:].copy()
            opens = data['OPEN'].values
            highs = data['HIGH'].values
            lows = data['LOW'].values
            closes = data['CLOSE'].values

            # 1. 计算开盘跳空幅度（与前一日收盘比较）
            gap_up = opens[1:] - closes[:-1]  # 向上跳空
            gap_down = closes[:-1] - opens[1:]  # 向下跳空
            gap_ratio = np.abs(opens[1:] - closes[:-1]) / closes[:-1]
            avg_gap_ratio = np.mean(gap_ratio) * 100  # 平均跳空百分比

            # 2. 计算K线实体大小（衡量价格变动幅度）
            body_size = np.abs(closes - opens) / opens  # 实体大小占开盘价比例
            avg_body_size = np.mean(body_size) * 100  # 平均实体大小百分比

            # 3. 计算上下影线比例（衡量价格震荡程度）
            upper_shadow = np.where(closes > opens,
                                    (highs - closes) / (highs - lows + 1e-10),
                                    (highs - opens) / (highs - lows + 1e-10))
            lower_shadow = np.where(closes > opens,
                                    (opens - lows) / (highs - lows + 1e-10),
                                    (closes - lows) / (highs - lows + 1e-10))
            avg_upper_shadow = np.mean(upper_shadow) * 100  # 平均上影线比例
            avg_lower_shadow = np.mean(lower_shadow) * 100  # 平均下影线比例

            # 4. 计算趋势一致性（连续同向K线数量）
            direction_changes = 0
            for i in range(1, len(closes)):
                # 判断趋势是否改变（考虑小幅波动）
                prev_trend = 1 if closes[i - 1] > opens[i - 1] else (-1 if closes[i - 1] < opens[i - 1] else 0)
                curr_trend = 1 if closes[i] > opens[i] else (-1 if closes[i] < opens[i] else 0)

                if prev_trend != 0 and curr_trend != 0 and prev_trend != curr_trend:
                    # 只有当趋势明确改变时才计数
                    if abs(closes[i] - opens[i]) / opens[i] > 0.002:  # 忽略极小实体
                        direction_changes += 1

            trend_consistency = 100 * (1 - direction_changes / 89)  # 趋势一致性评分

            # 5. 计算价格变化的连续性（相邻K线重叠程度）
            overlap_scores = []
            for i in range(1, len(opens)):
                # 计算相邻K线的价格区间重叠程度
                prev_min = min(opens[i - 1], closes[i - 1])
                prev_max = max(opens[i - 1], closes[i - 1])
                curr_min = min(opens[i], closes[i])
                curr_max = max(opens[i], closes[i])

                # 计算重叠部分占前一根K线的比例
                overlap_min = max(prev_min, curr_min)
                overlap_max = min(prev_max, curr_max)

                if overlap_max > overlap_min:
                    overlap_ratio = (overlap_max - overlap_min) / (prev_max - prev_min + 1e-10)
                else:
                    overlap_ratio = 0

                overlap_scores.append(overlap_ratio)

            avg_overlap = np.mean(overlap_scores) * 100  # 平均重叠比例

            # 6. 计算日内振幅稳定性
            daily_ranges = (highs - lows) / opens  # 日内振幅占开盘价比例
            range_std = np.std(daily_ranges) * 100  # 振幅的标准差（稳定性指标）

            # 归一化各项指标并计算总分
            # 所有指标都转换为0-100分，越高越好

            # 跳空幅度（越小越好 → 反向指标）
            gap_score = 100 * max(0, 1 - min(avg_gap_ratio / 2, 1))  # 假设2%为最大可接受跳空

            # 实体大小（适中最好，太小无方向，太大太剧烈）
            if avg_body_size < 0.5:
                body_score = 100 * (avg_body_size / 0.5)
            elif avg_body_size > 2.0:
                body_score = 100 * (1 - (avg_body_size - 2.0) / 3.0)
            else:
                body_score = 100

            # 影线比例（越小越好 → 反向指标）
            shadow_score = 100 * max(0, 1 - (avg_upper_shadow + avg_lower_shadow) / 100)

            # 趋势一致性（越高越好）
            trend_score = trend_consistency

            # 重叠程度（越高越好）
            overlap_score = avg_overlap

            # 振幅稳定性（标准差越小越好 → 反向指标）
            range_stability = 100 * max(0, 1 - min(range_std / 1.0, 1))  # 假设1%为标准差阈值

            # 加权平均计算总分
            weights = {
                'gap': 0.25,  # 跳空幅度
                'body': 0.15,  # 实体大小
                'shadow': 0.15,  # 影线比例
                'trend': 0.2,  # 趋势一致性
                'overlap': 0.15,  # 重叠程度
                'stability': 0.1  # 振幅稳定性
            }

            smoothness_score = (
                    weights['gap'] * gap_score +
                    weights['body'] * body_score +
                    weights['shadow'] * shadow_score +
                    weights['trend'] * trend_score +
                    weights['overlap'] * overlap_score +
                    weights['stability'] * range_stability
            )

            # 确保分数在0-100之间
            smoothness_score = max(0, min(100, smoothness_score))

            # 返回详细评分信息（可选）
            detail_scores = {
                'total_score': round(smoothness_score, 2),
                'gap_score': round(gap_score, 2),
                'body_score': round(body_score, 2),
                'shadow_score': round(shadow_score, 2),
                'trend_score': round(trend_score, 2),
                'overlap_score': round(overlap_score, 2),
                'stability_score': round(range_stability, 2)
            }

            stock.smooth90=smoothness_score
            new_row = [stock.code,stock.smooth90]
            file_path = 'testSmooth.csv'
            try:
                # 打开文件并以追加模式写入
                with open(file_path, mode="a", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(new_row)
                    # 写入新的一行
            except FileNotFoundError:
                print(f"错误：文件 {file_path} 不存在！")
            except Exception as e:
                print(f"发生错误：{e}")


            print(f"成功向文件 {file_path} 添加一行数据：{new_row}")
            #return detail_scores


    #计算和指数的相关性：

    def CalCorrelation60(self,stocklist,indexlist):
        #准备指数
        #IndexList=['NDX', 'DJI', 'SPX']

        for stock in stocklist:
            print(f'计算{stock.code}')
            #用于判断最大值的list
            rList=[]
            #绝对值
            rAbsList= []
            keyList=[]
            for index in indexlist:
                dayPriceData = stock.dayPriceData
                dayPriceData = dayPriceData.sort_values(by="DATE", ascending=True)
                #通过merge来找出和指数相同日期的值
                dayPriceDataMerge=dayPriceData.merge(index.dayPriceData,on='DATE',how='inner')
                lenth=dayPriceDataMerge.shape[0]
                if lenth > 60:
                    #closeData60 = dayPriceData['CLOSE'].iloc[-60:]
                    index_close = index.dayPriceData['CLOSE'].iloc[-60:]
                    index_pct = index_close.pct_change()

                    stock_close = dayPriceDataMerge['CLOSE_x'].iloc[-60:]
                    stock_pct = stock_close.pct_change()
                    # 不满60天直接取全部
                else:
                    index_close = index.dayPriceData['CLOSE']
                    index_pct = index_close.pct_change()

                    stock_close = dayPriceDataMerge['CLOSE_x']
                    stock_pct = stock_close.pct_change()

                '''
                # 判断是否相等
                if len(index_close) != len(stock_close):
                    print(f'{stock.code}数据不完整')
                    return 0, 0
                '''
                r_Matrix = np.corrcoef(index_close, stock_close)
                r = r_Matrix[0][1]
                covr_Matrix = np.cov(stock_pct[1:], index_pct[1:])
                covr = covr_Matrix[0][1]
                index_var = np.var(index_pct[1:])
                beta = covr / index_var
                #根据定位定位到字典中的对应项
                #stock.CorrelationUS[index.dayPriceData['CODE'].iloc[0]]=r
                rList.append(r)
                rAbsList.append(abs(r))
                code=index.dayPriceData['CODE'].iloc[0]
                keyList.append(code)

            # 求列表最大值及索引
            max_value = max(rAbsList)  # 求列表最大值
            max_idx = rAbsList.index(max_value)  # 求最大值对应索引
            stock.CorrelationUS.append(keyList[max_idx])
            stock.CorrelationUS.append(rList[max_idx])


    def RSIOverBuySellSignal(self,stock,type):
        rsi = stock.RSIData
        resultArry = np.zeros(rsi.shape[0])
        for i in range(rsi.shape[0]):
            RSI6 = rsi['RSI6'].iloc[i]
            if type=='sell':
                if RSI6 < 15:
                    resultArry[i] = 1
            if type=='buy':
                if RSI6 > 85:
                    resultArry[i] = 1
        # 找出连续1的起始位置
        posArry = resultArry.copy()
        for i in range(1, resultArry.shape[0]):
            if resultArry[i] == 1 and resultArry[i - 1] == 1:
                posArry[i] = 0

        return posArry



    def KDJOverBuySellSignal(self,stock,type):
        kdj = stock.KDJData
        resultArry = np.zeros(kdj.shape[0])
        for i in range(kdj.shape[0]):
            J = kdj['J'].iloc[i]
            if type == 'sell':
                if J < 10:
                    resultArry[i] = 1
            if type=='buy':
                if J > 90:
                    resultArry[i] = 1
        #找出连续1的起始位置
        posArry = resultArry.copy()
        for i in range(1,resultArry.shape[0]):
            if resultArry[i]==1 and resultArry[i-1]==1:
                posArry[i]=0

        return posArry


    def KDJUpcrossSignal(self, stock):
        kdj = stock.KDJData
        ResultArry = np.zeros(kdj.shape[0])
        for i in range(kdj.shape[0]):
            # 判断上穿
            if kdj['J'].iloc[i - 1] <= kdj['K'].iloc[i - 1] and kdj['J'].iloc[i] > kdj['K'].iloc[i]:
                ResultArry[i] = 1
        return ResultArry

        # EMA均线的圆形底

    def KDJArcSignal(self,stock,type):
        kdj = stock.KDJData
        ResultArry = np.zeros(kdj.shape[0])
        for i in range(2,kdj.shape[0]):
            # 必须下半部分
            if type == 'bottom':
                if kdj['J'].iloc[i - 2] <50 and kdj['J'].iloc[i-1] <50 and kdj['J'].iloc[i] <50:
                    if kdj['J'].iloc[i - 2] > kdj['J'].iloc[i-1] and kdj['J'].iloc[i] > kdj['J'].iloc[i-1]:
                        ResultArry[i] = 1
            if type=='top':
                if kdj['J'].iloc[i - 2] > 50 and kdj['J'].iloc[i - 1] > 50 and kdj['J'].iloc[i] > 50:
                    if kdj['J'].iloc[i - 2] < kdj['J'].iloc[i - 1] and kdj['J'].iloc[i] < kdj['J'].iloc[i - 1]:
                        ResultArry[i] = 1
        return ResultArry


    def EMA5ArcSignal(self, stock,type):
        ema = stock.EMAData
        ResultArry = np.zeros(ema.shape[0])
        # 需要考虑两边的情况，后续是看3日后的值，所以边界-3
        for i in range(2, ema.shape[0] - 3):
            if type == 'bottom':
                arr = np.zeros(5)
                for j in range(0, 5):
                    # 取最后5个
                    arr[j] = ema['EMA5'].iloc[i - 2 + j]
                posMin = np.argmin(arr)
                if posMin == 3:
                    if arr[1] < arr[0] and arr[2] < arr[1]:
                        ResultArry[i + 2] = 1
            if type == 'top':
                arr = np.zeros(5)
                for j in range(0, 5):
                    # 取最后5个
                    arr[j] = ema['EMA5'].iloc[i - 2 + j]
                posMax = np.argmax(arr)
                if posMax == 3:
                    if arr[1] > arr[0] and arr[2] > arr[1]:
                        ResultArry[i + 2] = 1
        return ResultArry

    def MACDArcSignal(self, stock,type):
        macd = stock.MACDData
        resultArry = np.zeros(macd.shape[0])
        # 需要考虑两边的情况，后续是看3日后的值，所以边界-3
        for i in range(1, macd.shape[0] - 1):
            arr = np.zeros(3)
            for j in range(0, 3):
                # 取最后5个
                arr[j] = macd['BAR'].iloc[i - 1 + j]
            if type=='bottom':
                posMin = np.argmin(arr)
            #3个值都小于0
                if posMin == 1 and (np.array(arr)<0).any():
                        resultArry[i] = 1
            if type=='top':
                posMin = np.argmax(arr)
                # 3个值都>于0
                if posMin == 1 and (np.array(arr) > 0).any():
                    resultArry[i] = 1
        posArry = resultArry.copy()
        for i in range(1, resultArry.shape[0]):
            if resultArry[i] == 1 and resultArry[i - 1] == 1:
                posArry[i] = 0

        return posArry



    def CalSignalProbability(self, resultarry, stock,direction):
        success = 0
        signalCount = 0
        dayPriceData = stock.dayPriceData

        for i in range(len(resultarry) - 3):
            if resultarry[i] >= 1:
                # 取三天后的价格判断
                signalCount = signalCount + 1
                pricePre = dayPriceData['CLOSE'].iloc[i]
                priceAfter = dayPriceData['CLOSE'].iloc[i + 3]
                if direction=='up':
                    if priceAfter > pricePre:
                        success = success + 1
                if direction=='down':
                    if priceAfter < pricePre:
                        success = success + 1
                    else:
                        failDate=dayPriceData['DATE'].iloc[i]
                        #print(f'失败日期{failDate}')

        if signalCount != 0:
            SignalProb = round(success / signalCount,3)

            return SignalProb
        else:
            return 99

    #只用daypricedata
    def CalSignalProbabilityOnlyData(self, resultarry, daypricedata,direction):
        success = 0
        signalCount = 0
        dayPriceData = daypricedata

        for i in range(len(resultarry) - 3):
            #为了适配talib,加入大于1的判断
            if resultarry[i] >= 1:
                # 取三天后的价格判断
                signalCount = signalCount + 1
                pricePre = dayPriceData['CLOSE'].iloc[i]
                priceAfter = dayPriceData['CLOSE'].iloc[i + 3]
                if direction=='up':
                    if priceAfter > pricePre:
                        success = success + 1
                if direction=='down':
                    if priceAfter < pricePre:
                        success = success + 1
                    else:
                        failDate=dayPriceData['DATE'].iloc[i]
                        #print(f'失败日期{failDate}')

        if signalCount != 0:
            SignalProb = round(success / signalCount,3)

            return SignalProb
        else:
            return 99


    def EMA5up10Strategy(self, stock):
        code = stock.code
        success = 0
        signalCount = 0
        accumlateReturn = 0
        buyFlag = 0
        buyPrice = 0
        sellPrice = 0
        initCash = 10000
        cash = 10000
        dayPriceData = stock.dayPriceData
        ema = stock.EMAData
        pos = 0
        for i in range(1, dayPriceData.shape[0]):
            if ema['EMA5'].iloc[i - 1] <= ema['EMA10'].iloc[i - 1] and ema['EMA5'].iloc[i] > ema['EMA10'].iloc[i]:
                # 上穿，买入
                buyFlag = 1
                signalCount = signalCount + 1
                # 记录买点位置
                pos = i
                buyDate = dayPriceData['DATE'].iloc[i]
                # buyPrice=dayPriceData['CLOSE'].iloc[i]
                # 按照上下穿均线价格买入卖出
                buyPrice = ema['EMA5'].iloc[i]
                print(f'{code}买入日期：{buyDate},买入价格{buyPrice}')
            elif ema['EMA5'].iloc[i - 1] >= ema['EMA10'].iloc[i - 1] and ema['EMA5'].iloc[i] < ema['EMA10'].iloc[i]:
                # 下穿，卖出
                if buyFlag == 1:
                    buyFlag = 0
                    sellDate = dayPriceData['DATE'].iloc[i]
                    # sellPrice=dayPriceData['CLOSE'].iloc[i]
                    # 按照上下穿均线价格买入卖出
                    sellPrice = ema['EMA5'].iloc[i]
                    gain = (sellPrice - buyPrice) / buyPrice
                    # 按复利来算
                    cash = cash * (1 + gain)
                    accumlateReturn = accumlateReturn + gain

                    if gain > 0:
                        success = success + 1
                    print(f'{code}卖出日期：{sellDate}，本次收益率{gain},卖出价格{sellPrice}')

            else:
                pass
        if signalCount != 0:
            SignalProb = success / signalCount
        cashReturn = cash / initCash
        return SignalProb, accumlateReturn, cashReturn


    def BBIStrategy(self, stock):
        code = stock.code
        success = 0
        signalCount = 0
        accumlateReturn = 0
        buyFlag = 0
        buyPrice = 0
        sellPrice = 0
        initCash = 10000
        cash = 10000
        fee = 0.0002
        dayPriceData = stock.dayPriceData
        bbi = stock.BBIData
        pos = 0
        for i in range(1, dayPriceData.shape[0]):

            if bbi['BBI'].iloc[i - 1] >= dayPriceData['CLOSE'].iloc[i - 1] and bbi['BBI'].iloc[i] < \
                    dayPriceData['CLOSE'].iloc[i]:
                # 上穿，买入
                buyFlag = 1
                signalCount = signalCount + 1
                # 记录买点位置
                pos = i
                buyDate = dayPriceData['DATE'].iloc[i]
                #buyPrice=dayPriceData['CLOSE'].iloc[i]
                # 按照上下穿均线价格买入卖出
                buyPrice = bbi['BBI'].iloc[i]
                print(f'{code}买入日期：{buyDate},买入价格{buyPrice}')
            elif bbi['BBI'].iloc[i - 1] <= dayPriceData['CLOSE'].iloc[i - 1] and bbi['BBI'].iloc[i] > \
                    dayPriceData['CLOSE'].iloc[i]:
                # 下穿，卖出
                if buyFlag == 1:
                    buyFlag = 0
                    sellDate = dayPriceData['DATE'].iloc[i]
                    #sellPrice=dayPriceData['CLOSE'].iloc[i]
                    # 按照上下穿均线价格买入卖出
                    sellPrice = bbi['BBI'].iloc[i]
                    gain = ((sellPrice - buyPrice) / buyPrice) - 1*fee
                    # 按复利来算
                    cash = cash * (1 + gain)
                    accumlateReturn = accumlateReturn + gain

                    if gain > 0:
                        success = success + 1
                    print(f'{code}卖出日期：{sellDate},卖出价格{sellPrice}',end='')
                    print('本次收益率为{:.2%}'.format(gain))

            else:
                pass
        if signalCount != 0:
            SignalProb = success / signalCount
        cashReturn = cash / initCash
        return SignalProb, accumlateReturn, cashReturn

'''
sql = f'select * from daypricedata where CODE = "NVDA.O" AND DATE between "2020-2-1" and "2024-3-17"'
outData = pd.DataFrame()
outData = pd.read_sql(text(sql), con=database.conUS)
outDataClose = outData['CLOSE'].tolist()
output=np.zeros(outData.shape[0])
outData = outData.sort_values(by="DATE", ascending=True)
#for i in range(0,outData.shape[0]):
#outputArray = talib.CDLHIGHWAVE(outData['OPEN'], outData['HIGH'], outData['LOW'], outData['CLOSE'])
#outData['RESULT']=outputArray
stg=Strategy()
kln=KLineSignal()
outputArray=kln.LongDownLeg(outData['OPEN'], outData['HIGH'], outData['LOW'], outData['CLOSE'])


result=stg.CalSignalProbabilityOnlyData(outputArray,outData,'up')
outData['result']=outputArray
pass
'''