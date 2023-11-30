import pandas as pd


#codeDF = pd.read_csv("D:\ztrade\heatChartUSbuff.csv", encoding="gb2312")
#codeDFHeat = pd.read_csv("D:\ztrade\heatChart.csv", encoding="gb2312")
# codeDF=pd.read_csv("D:\ztrade\codesShort.csv")
#codeDFHeat.rename(columns={'代码':'TRADE_CODE'},inplace=True)
#set_diff_df = pd.concat([codeDFHeat, codeDF, codeDF]).drop_duplicates(keep=False)

#intersected_df = pd.merge(codeDF, codeDFHeat, on=['TRADE_CODE'], how='inner')

#.to_csv("D:\ztrade\heatChartUS.csv", index_label="index_label")

#codeDFHeat['WINDCODE']=''


#codeList = codeDF['WindCodes'].tolist()