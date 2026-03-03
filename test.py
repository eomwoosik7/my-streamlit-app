from pykrx import stock
tickers = stock.get_market_ticker_list('20260228', market='KOSPI')
print(len(tickers), tickers[:3])