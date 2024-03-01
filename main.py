# -*- coding: utf-8 -*-
import functools
from datetime import date, timedelta

import matplotlib.pyplot as plt
from PyQt5.QtCore import QObject, pyqtSignal
from PyQt5.QtGui import QTextCursor
from PyQt5.QtWidgets import QApplication, QMainWindow, QTableWidget, QGraphicsWidget, QGraphicsView, QGraphicsScene, \
    QMenu
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas

# Form implementation generated from reading ui file 'qtwindow.ui'
#
# Created by: PyQt5 UI code generator 5.15.9
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.

import zmain
from PyQt5 import QtCore, QtGui, QtWidgets

import zplot
from qtwindow import Ui_MainWindow
import pandas as pd
import database
import recognition
import stockclass
from talib import EMA
import zfutu
from futu import *


class Signal(QObject):
    text_update = pyqtSignal(str)

    def write(self, text):
        self.text_update.emit(str(text))
        # loop = QEventLoop()
        # QTimer.singleShot(100, loop.quit)
        # loop.exec_()
        QApplication.processEvents()



class MyMainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self, MainWindow,parent=None):
        super(MyMainWindow, self).__init__(parent)
        self.setupUi(MainWindow)

        #图像显示，在graphicview中添加scene
        self.scene = QGraphicsScene(self)
        self.graphicsView.setScene(self.scene)

        #############################
        self.pushButton.clicked.connect(self.ZtradeUS)
        #显示K线的逻辑
        #self.showCandle.clicked.connect(self.ShowCandle)
        #传递table值进去
        self.tableWidget_backstepema.itemDoubleClicked.connect(functools.partial(self.ShowCandle, 0))
        self.tableWidget_EmaDiffusion.itemDoubleClicked.connect(functools.partial(self.ShowCandle, 1))
        self.tableWidget_EMAUpCross.itemDoubleClicked.connect(functools.partial(self.ShowCandle, 2))
        self.tableWidget_MoneyFlow.itemDoubleClicked.connect(functools.partial(self.ShowCandle, 3))
        self.tableWidget_EMA5BottomArc.itemDoubleClicked.connect(functools.partial(self.ShowCandle, 4))
        self.tableWidget_EMA5TOPArc.itemDoubleClicked.connect(functools.partial(self.ShowCandle, 5))
        self.tableWidget_EMADownCross.itemDoubleClicked.connect(functools.partial(self.ShowCandle, 6))

        self.tableWidget_backstepema.customContextMenuRequested.connect(functools.partial(self.GenerateMenu,0))
        self.tableWidget_EmaDiffusion.customContextMenuRequested.connect(functools.partial(self.GenerateMenu,1))
        self.tableWidget_EMAUpCross.customContextMenuRequested.connect(functools.partial(self.GenerateMenu,2))
        self.tableWidget_MoneyFlow.customContextMenuRequested.connect(functools.partial(self.GenerateMenu,3))
        self.tableWidget_EMA5BottomArc.customContextMenuRequested.connect(functools.partial(self.GenerateMenu,4))
        self.tableWidget_EMA5TOPArc.customContextMenuRequested.connect(functools.partial(self.GenerateMenu,5))
        self.tableWidget_EMADownCross.customContextMenuRequested.connect(functools.partial(self.GenerateMenu,6))


        #self.pushButton.clicked.connect(self.UpdateTable)
        sys.stdout = Signal()

        sys.stdout.text_update.connect(self.updatetext)

        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    #def StartAnalyze(self):
    #    zmain.ZtradeUS()

    def ZtradeUS(self):
        # f= open(, encoding="utf-8")
        TradeCalendar_US = 'NYSE'
        codeDF = pd.read_csv("D:\ztrade\heatChartUS.csv", encoding="gb2312")
        # codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
        codeList = codeDF['WindCodes'].tolist()

        startDate = date(2023, 8, 1)
        #endDate = date(2024, 2,16)
        endDate = date.today() - timedelta(days=1)

        # codeList=['AES.N']

        pass
        dtp_US = database.DataPrepare(database.conUS, database.engineUS, database.sessionUS, TradeCalendar_US)
        # 准备好待处理的stock类
        stocks_US = dtp_US.DataPreWindDB(codeList, startDate, endDate)
        # 使用stock列表进行beta分析
        # result= index.BetaAnalyze(startDate,endDate,stocks_US)
        pass
        # 识别的类

        recog_US = recognition.Recognition()
        recog_US.RecognitionProcess(stocks_US)

        zft_US = zfutu.Zfutu(market='US')
        #zft_US.ModifyFutuStockList(recog_US.resultTable)

        self.UpdateTab(stocks_US,recog_US)

    def updatetext(self, text):
        """
            更新textBrowser
        """
        cursor = self.textBrowser.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.textBrowser.append(text)
        self.textBrowser.setTextCursor(cursor)
        self.textBrowser.ensureCursorVisible()

    def UpdateTab(self,stocklist,recog_US):
        recogList = ['backstepema', 'EmaDiffusion','EMAUpCross','MoneyFlow','EMA5BottomArc','EMA5TOPArc','EMADownCross']

        resultTable=recog_US.resultTable
        for index,recogName in enumerate(recogList):
            resultTableSliced=resultTable[['code',recogName]]
            resultTableSliced=resultTableSliced.loc[~((resultTableSliced[recogName] == 0) )]
            codeList=resultTableSliced['code'].tolist()
            #使用filter来找出各个list股票名称
            stockFiltered=list(filter(lambda x:x.code in codeList,stocklist))
            #通过index获取到tab列表
            widget=self.tabWidget.widget(index)
            #获取到table
            tablelist=widget.findChildren(QTableWidget)
            table=tablelist[0]
            self.UpdateTable(table,stockFiltered)

            #codelistNew=self.CodeTransferWind2FUTU(codeList)


    #更新股票表格
    def UpdateTable(self,table,stocklist):
        # data = {'代码': ['app', 'Ja', 'St', 'Ri'], '名称': ['Tom', 'Jack', 'Steve', 'Ricky'],'价格': [19.1, 128.03, 3423.32,0.11],'涨跌幅': [19.1, 128.03, 3423.32,0.11],'成交量': [19.1, 128.03, 3423.32,0.11]}  # 两组列元素，并且个数需要相同
        # df = pd.DataFrame(data)  # 这里默认的 index 就是 range(n)，n 是列表的长度

        for stock in stocklist:
            dayPriceData=stock.dayPriceData
            close=dayPriceData['CLOSE'].iloc[-1]
            code=dayPriceData['CODE'].iloc[-1]
            open=dayPriceData['OPEN'].iloc[-1]
            volume=dayPriceData['VOLUME'].iloc[-1]
            changePct=format(100*(close-open)/open,'.2f')
            changePctStr=str(changePct)+'%'

            row_count = table.rowCount()  # 返回当前行数(尾部)
            table.insertRow(row_count)  # 尾部插入一行
            table.setItem(row_count-1, 0, QtWidgets.QTableWidgetItem(str(code)))
            table.setItem(row_count-1, 1, QtWidgets.QTableWidgetItem(str(code)))
            table.setItem(row_count - 1, 2, QtWidgets.QTableWidgetItem(str(close)))
            table.setItem(row_count - 1, 3, QtWidgets.QTableWidgetItem(changePctStr))
            table.setItem(row_count - 1, 4, QtWidgets.QTableWidgetItem(str(volume)))


        pass

    def ShowCandle(self,tabindex,Item=None):
        if Item is None:
            return
        else:
            row = Item.row()  # 获取行数
            col = Item.column()  # 获取列数 注意是column而不是col哦

            match tabindex:
                case 0:
                    code = self.tableWidget_backstepema.item(row, 1).text()
                case 1:
                    code = self.tableWidget_EmaDiffusion.item(row, 1).text()
                case 2:
                    code = self.tableWidget_EMAUpCross.item(row, 1).text()
                case 3:
                    code = self.tableWidget_MoneyFlow.item(row, 1).text()
                case 4:
                    code = self.tableWidget_EMA5BottomArc.item(row, 1).text()
                case 5:
                    code = self.tableWidget_EMA5TOPArc.item(row, 1).text()
                case 6:
                    code = self.tableWidget_EMADownCross.item(row, 1).text()
                case _:
                    return

            #调用画图函数
            fig,axlist=zplot.plotCandle(code)
            # 输出测试
            canvas=FigureCanvas(fig)
            self.scene.addWidget(canvas)

    def GenerateMenu(self,tabindex,pos):
        match tabindex:
            case 0:
                table = self.tableWidget_backstepema
            case 1:
                table = self.tableWidget_EmaDiffusion
            case 2:
                table = self.tableWidget_EMAUpCross
            case 3:
                table = self.tableWidget_MoneyFlow
            case 4:
                table = self.tableWidget_EMA5BottomArc
            case 5:
                table = self.tableWidget_EMA5TOPArc
            case 6:
                table = self.tableWidget_EMADownCross
            case _:
                return

        #print(pos)
        # 获得右键所点击的索引值
        for i in table.selectionModel().selection().indexes():
            # 获得当前的行数目
            rowIndex = i.row()
            # 如果选择的索引小于总行数, 弹出上下文菜单
            if rowIndex < table.rowCount():
                # 构造菜单
                menu = QMenu()
                # 添加菜单的选项
                item1 = menu.addAction("添加特别关注")
                #item2 = menu.addAction("菜单项2")
                #item3 = menu.addAction("菜单项3")
                # 获得相对屏幕的位置
                screenPos = table.mapToGlobal(pos)
                # 被阻塞, 执行菜单
                action = menu.exec(screenPos)
                if action == item1:
                    code= table.item(rowIndex, 0).text()
                    codelist = list()
                    codelist.append(code)
                    codelistNew=zfutu.CodeTransWind2FUTU_US(codelist)
                    #添加到特别关注
                    zfutu.AddFutuList('SpecialFocus',codelistNew)

                    #print("选择了第一个菜单项", table.item(rowIndex, 0).text(),
                    #      self.tableWidget_backstepema.item(rowIndex, 1).text(),
                    #      self.tableWidget_backstepema.item(rowIndex, 2).text())

            else:
                return


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)

    MainWindow = QtWidgets.QMainWindow()
    myWin = MyMainWindow(MainWindow)
    MainWindow.show()

    sys.exit(app.exec_())