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