"""
Backtest copying an investor's trades. For each Buy/Add, checks if the stock
ever dropped X% below the investor's avg entry price AFTER the filing became
public (quarter_end + 45 days). If it did, you could have copied at a discount.

Usage:
    python -m copy_investor "Li Lu" --buy-below 20
    python -m copy_investor "Warren Buffett" --buy-below 10
    python -m copy_investor "Warren Buffett" --quarter "Q1 2025"
"""

import argparse
import warnings
from datetime import timedelta

import pandas as pd
import yfinance as yf

from dataroma import get_investor_activity
from yahoo import add_yahoo_quarter_price_stats_batch

warnings.filterwarnings("ignore")

BOLD  = "\033[1m"
DIM   = "\033[2m"
GREEN = "\033[32m"
RED   = "\033[31m"
CYAN  = "\033[36m"
RESET = "\033[0m"

FILING_DELAY_DAYS = 45


# ── Price helpers ────────────────────────────────────────────────────────

def quarter_end(quarter_str: str) -> pd.Timestamp:
    q_num = int(quarter_str[1])
    year = int(quarter_str.split()[1])
    return pd.Period(f"{year}Q{q_num}", freq="Q").end_time


def filing_date(quarter_str: str) -> pd.Timestamp:
    return quarter_end(quarter_str) + timedelta(days=FILING_DELAY_DAYS)


def download_history(ticker: str) -> pd.Series:
    clean = ticker.replace(".", "-").upper()
    try:
        prices = yf.download(clean, progress=False, auto_adjust=True)["Close"].dropna()
        if isinstance(prices, pd.DataFrame):
            prices = prices.iloc[:, 0]
        return prices
    except Exception:
        return pd.Series(dtype=float)


def prices_after(history: pd.Series, date: pd.Timestamp) -> pd.Series:
    """All closing prices from date onwards."""
    if history.empty:
        return history
    return history[history.index >= date]


# ── Step 1: Fetch + enrich activity ─────────────────────────────────────

def fetch_activity(investor_name: str) -> pd.DataFrame:
    df = get_investor_activity(investor_name)
    df["stock"] = df["stock"].str.replace(".", "-", regex=False).str.upper()

    print(f"  Enriching with Yahoo quarter prices...")
    df = add_yahoo_quarter_price_stats_batch(df)
    df = df.dropna(subset=["price_p50"])
    return df


# ── Step 2: Check if price dipped below target after filing ─────────────

def find_next_sell_date(df: pd.DataFrame, ticker: str, after_quarter: str) -> pd.Timestamp | None:
    """Find the earliest sell/reduce for this ticker after the given quarter."""
    after = quarter_end(after_quarter)
    sells = df[
        (df["stock"] == ticker)
        & (df["activity"].str.startswith("Sell") | df["activity"].str.contains("Reduce"))
    ]
    for _, row in sells.iterrows():
        sell_end = quarter_end(row["quarter"])
        if sell_end > after:
            return filing_date(row["quarter"])
    return None


def prices_between(history: pd.Series, start: pd.Timestamp, end: pd.Timestamp | None) -> pd.Series:
    """Prices from start up to (but not after) end. If end is None, all prices from start."""
    if history.empty:
        return history
    after_start = history[history.index >= start]
    if end is not None:
        return after_start[after_start.index <= end]
    return after_start


def analyze_dips(df: pd.DataFrame, buy_below: float) -> tuple[pd.DataFrame, dict]:
    tickers = df["stock"].unique()
    print(f"  Downloading histories for {len(tickers)} tickers...")

    histories = {}
    for t in tickers:
        histories[t] = download_history(t)

    rows = []
    for _, row in df.iterrows():
        ticker = row["stock"]
        quarter = row["quarter"]
        activity = row["activity"]
        shares = row["shares"]
        avg_price = row["price_p50"]
        floor_price = row.get("price_p10", 0)

        is_buy = activity.startswith(("Buy", "Add"))
        is_sell = activity.startswith("Sell") or "Reduce" in activity

        fdate = filing_date(quarter)
        history = histories.get(ticker, pd.Series(dtype=float))
        target_price = max(avg_price * (1 - buy_below / 100), floor_price)

        if is_buy:
            # window closes when the investor next sells/reduces this ticker
            next_sell = find_next_sell_date(df, ticker, quarter)
            window = prices_between(history, fdate, next_sell)
            window_low = float(window.min()) if not window.empty else None

            if window.empty:
                signal = "NO DATA"
                hit_date = None
                hit_price = None
            elif window_low <= target_price:
                hits = window[window <= target_price]
                hit_date = hits.index[0]
                hit_price = float(hits.iloc[0])
                signal = "BUY"
            else:
                signal = "SKIP"
                hit_date = None
                hit_price = None
        elif is_sell:
            next_sell = None
            window_low = None
            signal = "SELL"
            hit_date = None
            hit_price = None
        else:
            next_sell = None
            window_low = None
            signal = "—"
            hit_date = None
            hit_price = None

        rows.append({
            "quarter": quarter,
            "ticker": ticker,
            "activity": activity,
            "shares": shares,
            "avg_price": avg_price,
            "target_price": target_price,
            "filing_date": fdate,
            "window_end": next_sell,
            "low_in_window": window_low,
            "hit_date": hit_date,
            "hit_price": hit_price,
            "signal": signal,
        })

    result = pd.DataFrame(rows)

    # only SELL tickers we actually got a BUY on
    bought_tickers = set(result.loc[result["signal"] == "BUY", "ticker"])
    sell_mask = result["signal"] == "SELL"
    result.loc[sell_mask & ~result["ticker"].isin(bought_tickers), "signal"] = "—"

    return result, histories


# ── Step 3: Display ─────────────────────────────────────────────────────

SIGNAL_COLORS = {
    "BUY": GREEN,
    "SELL": CYAN,
    "SKIP": RED,
    "NO DATA": DIM,
    "—": DIM,
}


TK_W   = 8
ACT_W  = 18
PRC_W  = 10
DT_W   = 12
SIG_W  = 14


def fmt_price(p, w=PRC_W):
    if pd.notna(p):
        s = f"${p:,.2f}"
        return f"{s:>{w}s}"
    return f"{'n/a':>{w}s}"


def fmt_date(d, w=DT_W):
    if pd.notna(d):
        return f"{d.strftime('%Y-%m-%d'):<{w}s}"
    return f"{'':<{w}s}"


def display(df: pd.DataFrame, quarter_filter: str | None = None):
    if quarter_filter:
        df = df[df["quarter"] == quarter_filter]

    sep_w = TK_W + ACT_W + PRC_W * 4 + DT_W * 2 + SIG_W + 8 * 3
    quarters = df["quarter"].unique()
    for q in quarters:
        qdf = df[df["quarter"] == q]
        fdate = qdf["filing_date"].iloc[0].strftime("%Y-%m-%d") if len(qdf) else "?"
        print(f"\n  {BOLD}{q}{RESET}  {DIM}(filing ≈ {fdate}){RESET}")
        print(f"  {DIM}{'─' * sep_w}{RESET}")
        hdr = (f"{'Ticker':<{TK_W}s} │ {'Activity':<{ACT_W}s} │ "
               f"{'Avg':>{PRC_W}s} │ {'Target':>{PRC_W}s} │ "
               f"{'Window End':<{DT_W}s} │ {'Low':>{PRC_W}s} │ "
               f"{'Hit Date':<{DT_W}s} │ {'Hit Price':>{PRC_W}s} │ "
               f"{'Signal':<{SIG_W}s}")
        print(f"  {DIM}{hdr}{RESET}")
        print(f"  {DIM}{'─' * sep_w}{RESET}")

        for _, r in qdf.iterrows():
            col = SIGNAL_COLORS.get(r["signal"], "")
            wend = r.get("window_end")
            wend_str = fmt_date(wend) if pd.notna(wend) else f"{'now':<{DT_W}s}"
            ticker = r["ticker"][:TK_W]
            activity = r["activity"][:ACT_W]

            print(f"  {ticker:<{TK_W}s} │ {activity:<{ACT_W}s} │ "
                  f"{fmt_price(r['avg_price'])} │ {fmt_price(r['target_price'])} │ "
                  f"{wend_str} │ {fmt_price(r['low_in_window'])} │ "
                  f"{fmt_date(r['hit_date'])} │ {fmt_price(r['hit_price'])} │ "
                  f"{col}{BOLD}{r['signal']:<{SIG_W}s}{RESET}")

    # Summary
    buys = df[df["signal"] == "BUY"]
    sells = df[df["signal"] == "SELL"]
    skips = df[df["signal"] == "SKIP"]
    print(f"\n  {BOLD}Summary:{RESET}  "
          f"{GREEN}BUY: {len(buys)}{RESET}  │  "
          f"{CYAN}SELL: {len(sells)}{RESET}  │  "
          f"{RED}SKIP: {len(skips)}{RESET}\n")


# ── Step 4: Compute returns on copied trades ────────────────────────────

def compute_returns(df: pd.DataFrame, histories: dict) -> pd.DataFrame:
    buys = df[df["signal"] == "BUY"].copy()
    sells = df[df["signal"] == "SELL"].copy()

    rows = []
    for _, buy in buys.iterrows():
        ticker = buy["ticker"]
        buy_price = buy["hit_price"]
        buy_date = buy["hit_date"]

        if pd.isna(buy_price) or pd.isna(buy_date):
            continue

        history = histories.get(ticker, pd.Series(dtype=float))

        # find matching sell
        ticker_sells = sells[sells["ticker"] == ticker]
        ticker_sells = ticker_sells[ticker_sells["filing_date"] > buy_date]

        if not ticker_sells.empty:
            sell_row = ticker_sells.iloc[0]
            sell_date = sell_row["filing_date"]
            sell_price = price_on(history, sell_date)
            status = "closed"
        else:
            sell_date = pd.Timestamp.now()
            sell_price = float(history.iloc[-1]) if not history.empty else None
            status = "holding"

        if sell_price is None or buy_price <= 0:
            continue

        total_return = (sell_price - buy_price) / buy_price
        holding_years = (sell_date - buy_date).days / 365.25
        irr = (sell_price / buy_price) ** (1 / holding_years) - 1 if holding_years > 0 else 0

        rows.append({
            "ticker": ticker,
            "buy_date": buy_date,
            "buy_price": buy_price,
            "sell_date": sell_date,
            "sell_price": sell_price,
            "return": total_return,
            "years": holding_years,
            "irr": irr,
            "status": status,
        })

    return pd.DataFrame(rows)


def price_on(history: pd.Series, date: pd.Timestamp) -> float | None:
    if history.empty:
        return None
    before = history[history.index <= date]
    if before.empty:
        return None
    return float(before.iloc[-1])


def color_pct(val, width=9):
    pct = val * 100
    txt = f"{pct:+.1f}%"
    col = GREEN if pct >= 0 else RED
    return f"{col}{txt:>{width}s}{RESET}"


def display_returns(portfolio: pd.DataFrame):
    if portfolio.empty:
        print(f"  {DIM}No copied trades to show.{RESET}\n")
        return

    print(f"\n  {BOLD}{'═' * 100}{RESET}")
    print(f"  {BOLD}{CYAN}COPY PORTFOLIO RETURNS{RESET}")
    print(f"  {BOLD}{'═' * 100}{RESET}")
    print(f"  {DIM}{'─' * 100}{RESET}")
    hdr = (f"{'Ticker':<{TK_W}s} │ {'Buy Date':<{DT_W}s} │ {'Buy':>{PRC_W}s} │ "
           f"{'Sell Date':<{DT_W}s} │ {'Sell':>{PRC_W}s} │ "
           f"{'Return':>9s} │ {'Years':>6s} │ {'IRR':>9s} │ {'Status':<8s}")
    print(f"  {DIM}{hdr}{RESET}")
    print(f"  {DIM}{'─' * 100}{RESET}")

    for _, r in portfolio.iterrows():
        st_col = CYAN if r["status"] == "holding" else DIM
        print(f"  {r['ticker']:<{TK_W}s} │ {fmt_date(r['buy_date'])} │ {fmt_price(r['buy_price'])} │ "
              f"{fmt_date(r['sell_date'])} │ {fmt_price(r['sell_price'])} │ "
              f"{color_pct(r['return'])} │ {r['years']:>6.1f} │ {color_pct(r['irr'])} │ "
              f"{st_col}{r['status']:<8s}{RESET}")

    # totals
    n = len(portfolio)
    winners = (portfolio["return"] > 0).sum()
    avg_ret = portfolio["return"].mean()
    avg_irr = portfolio["irr"].mean()
    med_ret = portfolio["return"].median()
    med_irr = portfolio["irr"].median()
    avg_yrs = portfolio["years"].mean()

    print(f"  {DIM}{'─' * 100}{RESET}")
    print(f"  {BOLD}Trades:{RESET} {n}  ({winners}W / {n - winners}L)  │  "
          f"{BOLD}Win Rate:{RESET} {color_pct(winners / n if n else 0)}  │  "
          f"{BOLD}Avg Holding:{RESET} {avg_yrs:.1f} yrs")
    print(f"  {BOLD}Avg Return:{RESET} {color_pct(avg_ret)}  │  "
          f"{BOLD}Med Return:{RESET} {color_pct(med_ret)}  │  "
          f"{BOLD}Avg IRR:{RESET} {color_pct(avg_irr)}  │  "
          f"{BOLD}Med IRR:{RESET} {color_pct(med_irr)}")
    print()


# ── Main ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backtest copying an investor's trades.")
    parser.add_argument("investor", help="Investor name")
    parser.add_argument(
        "--buy-below", type=float, default=20,
        help="Buy only if price dips X%% below avg entry after filing (default: 20)",
    )
    parser.add_argument("--quarter", default=None, help="Filter to one quarter (e.g. 'Q1 2025')")
    args = parser.parse_args()

    print(f"\n  {BOLD}Investor:{RESET} {args.investor}  │  "
          f"{BOLD}Buy if dips:{RESET} {args.buy_below}%% below avg price\n")

    activity = fetch_activity(args.investor)
    signals, histories = analyze_dips(activity, args.buy_below)
    display(signals, args.quarter)

    portfolio = compute_returns(signals, histories)
    display_returns(portfolio)


if __name__ == "__main__":
    main()
