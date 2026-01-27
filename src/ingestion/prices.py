import pandas as pd
import yfinance as yf
import os

def normalize_prices(raw_df, tickers):

    df = raw_df.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df = df.stack(level = "Ticker").reset_index()

        df = df.rename(columns = {
            "Date":"date",
            "Ticker": "ticker",
            "Open" : "open",
            "High" : "high",
            "Low" : "low",
            "Close" : "close",
            "Adj Close" : "adj_close",
            "Volume" : "volume",
            })
        
    else:
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()

        df = df.rename(columns = {
            "Date":"date",
            "Open" : "open",
            "High" : "high",
            "Low" : "low",
            "Close" : "close",
            "Adj Close" : "adj_close",
            "Volume" : "volume",
            })
    
        if isinstance(tickers, (list, tuple)):
            if len(tickers) != 1:
                raise ValueError("Single ticker path needs exactly one ticker.")
            ticker_val = tickers[0]
        else:
            ticker_val = tickers

        df["ticker"] = ticker_val

    df.columns.name = None
    df["date"] = pd.to_datetime(df['date'])

    cols = ["date", "ticker", "open", "high", "low", "close"]
    if "adj_close" in df.columns:
        cols.append("adj_close")
    cols.append("volume")
     
    df = df[cols].sort_values(["ticker", "date"]).reset_index(drop = True)
    return df


def fetch_prices(tickers, start, end, interval = "1d", cache_dir = "data/raw"):
    if isinstance(tickers, str):
        tickers_list = [tickers]
    else:
        tickers_list = list(tickers)

    tickers_key = "-".join(sorted(tickers_list))
    filename = f"prices{tickers_key}_{start}_{end}_{interval}.parquet"
    path = os.path.join(cache_dir, filename)

    os.makedirs(cache_dir, exist_ok = True)

    if os.path.exists(path):
        return pd.read_parquet(path)
    
    raw = yf.download(
        tickers_list,
        start = start,
        end = end,
        interval = interval,
        auto_adjust = False,
        progress = False,
        group_by = "column",
    )

    tidy = normalize_prices(raw, tickers_list)
    tidy.to_parquet(path, index = False)
    return tidy