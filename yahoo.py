import yfinance as yf
import pandas as pd


def add_yahoo_quarter_price_stats_batch(
    df,
    ticker_col="stock",
    quarter_col="quarter",
):
    df = df.copy()
    df["_period"] = pd.PeriodIndex(
        df[quarter_col].str.replace(r"(Q[1-4])\s*(\d{4})", r"\2\1", regex=True),
        freq="Q",
    )
    stats = []
    for ticker in df[ticker_col].unique():
        start_date = df["_period"].min().start_time
        end_date = df["_period"].max().end_time
        prices = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            progress=False,
            auto_adjust=True,
        )["Close"].dropna()
        if prices.empty:
            q_stats = pd.DataFrame(
                {
                    "period": df["_period"].unique(),
                    "price_p10": float("nan"),
                    "price_p90": float("nan"),
                    "price_p50": float("nan"),
                    ticker_col: ticker,
                }
            )
            stats.append(q_stats)
            continue
        q_prices = prices.to_frame("price").assign(period=lambda x: x.index.to_period("Q"))
        q_stats = (
            q_prices.groupby("period")["price"]
            .agg(
                price_p10=lambda x: x.quantile(0.10),
                price_p90=lambda x: x.quantile(0.90),
                price_p50=lambda x: x.quantile(0.50),
            )
            .reset_index()
        )
        q_stats[ticker_col] = ticker
        stats.append(q_stats)
    stats_df = pd.concat(stats, ignore_index=True)
    df = df.merge(
        stats_df,
        left_on=[ticker_col, "_period"],
        right_on=[ticker_col, "period"],
        how="left",
    )
    df.drop(columns=["_period", "period"], inplace=True)
    return df


def fetch_current_price(ticker):
    try:
        new_ticker = ticker.replace(".", "-")
        stock_yf = yf.Ticker(new_ticker)
        hist = stock_yf.history(period="5d")
        if not hist.empty:
            return hist["Close"].iloc[-1]
        return None
    except Exception:
        return None
