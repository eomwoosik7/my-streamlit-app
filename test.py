<<<<<<< HEAD
import duckdb
con = duckdb.connect("data/meta/backtest.db", read_only=True)
df = con.execute("""
    SELECT symbol, market, market_cap 
    FROM backtest 
    WHERE market = 'KR' 
    LIMIT 5
""").fetchdf()
print(df)
con.close()
=======
from pykrx import stock
tickers = stock.get_market_ticker_list('20260228', market='KOSPI')
print(len(tickers), tickers[:3])
>>>>>>> e40495d3d647e32871b6a6b3afb030b7b636fd21
