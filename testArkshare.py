import akshare as ak
import pandas as pd
stock_us_hist_df = ak.stock_us_hist(symbol='105.AAPL', period="daily", start_date="20230101", end_date="20250712", adjust="qfq")
#symbol= ak.stock_us_spot_em()
print(stock_us_hist_df)
#symbol.to_excel('symboldata.xlsx', index=False)
#df = pd.read_excel('symbolArkshare.xlsx')



