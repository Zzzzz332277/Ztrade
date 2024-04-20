import yahoo_fin.stock_info as si
import requests_html
aapl_earnings_hist = si.get_earnings_history("aapl")