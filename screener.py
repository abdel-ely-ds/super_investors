"""
Find conviction holdings (≥ min % of portfolio) where the current price
is below the investor's average buy price by at least a given discount.
Only keeps positions initiated (first Buy) within the last --max-age quarters.
Reads enriched data from data/, fetches live prices from Yahoo.
"""

import argparse
import sys
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd

from metrics import quarter_to_date
from yahoo import fetch_current_price

warnings.filterwarnings("ignore")

DATA_DIR = Path(__file__).parent / "data"

BOLD  = "\033[1m"
DIM   = "\033[2m"
GREEN = "\033[32m"
RED   = "\033[31m"
CYAN  = "\033[36m"
YELLOW = "\033[33m"
RESET = "\033[0m"


def _holdings_for_investor(df: pd.DataFrame) -> pd.DataFrame:
    """Return one row per currently-held stock with avg buy price, shares,
    and the quarter the current position was initiated.

    Processes chronologically so Sell-100% → rebuy cycles are handled.
    """
    rows = []
    for ticker in df.stock.unique():
        hist = df[df.stock == ticker].iloc[::-1]  # oldest first

        shares_held = 0
        cost_basis = 0.0
        initiated_quarter = None

        for _, row in hist.iterrows():
            act = row.activity
            if act.startswith("Sell 100"):
                shares_held = 0
                cost_basis = 0.0
                initiated_quarter = None
            elif act.startswith("Buy"):
                if shares_held == 0:
                    initiated_quarter = row.quarter
                shares_held += row.shares
                cost_basis += row.shares * row.price_p50
            elif act.startswith("Add"):
                shares_held += row.shares
                cost_basis += row.shares * row.price_p50
            elif act.startswith("Reduce"):
                frac_sold = row.shares / shares_held if shares_held > 0 else 0
                cost_basis *= (1 - frac_sold)
                shares_held -= row.shares

        if shares_held <= 0:
            continue

        avg_buy_price = cost_basis / shares_held if shares_held > 0 else 0
        rows.append({
            "ticker": ticker,
            "shares_held": shares_held,
            "avg_buy_price": avg_buy_price,
            "initiated": initiated_quarter,
        })
    return pd.DataFrame(rows)


def _load_all_holdings(names: list[str]) -> dict[str, pd.DataFrame]:
    """Phase 1: read CSVs and compute holdings (CPU-only, fast)."""
    investor_holdings = {}
    for name in names:
        data_path = DATA_DIR / f"{name}.csv"
        if not data_path.exists():
            continue
        df = pd.read_csv(data_path)
        if df.empty:
            continue
        h = _holdings_for_investor(df)
        if not h.empty:
            investor_holdings[name] = h
    return investor_holdings


def _fetch_prices_parallel(tickers: set[str], workers: int) -> dict[str, float]:
    """Phase 2: fetch current prices for all unique tickers in parallel."""
    prices = {}
    done = 0
    total = len(tickers)
    lock = threading.Lock()

    def _fetch(tk):
        nonlocal done
        p = fetch_current_price(tk)
        with lock:
            done += 1
            print(f"  {DIM}Fetching prices … {done}/{total}{RESET}   ", end="\r")
        return tk, p

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_fetch, tk) for tk in tickers]
        for fut in as_completed(futures):
            tk, p = fut.result()
            if p is not None:
                prices[tk] = p

    print(" " * 50, end="\r")
    return prices


def _apply_prices_and_filter(
    investor_holdings: dict[str, pd.DataFrame],
    prices: dict[str, float],
    cutoff: datetime | None,
    min_pct: float,
    discount: float,
) -> list[pd.DataFrame]:
    """Phase 3: apply prices, compute portfolio %, filter."""
    all_hits = []
    for name, holdings in investor_holdings.items():
        holdings = holdings.copy()
        holdings["current_price"] = holdings.ticker.map(prices)
        holdings = holdings.dropna(subset=["current_price"])
        if holdings.empty:
            continue

        holdings["current_value"] = holdings.shares_held * holdings.current_price
        total_value = holdings.current_value.sum()
        holdings["pct_portfolio"] = (holdings.current_value / total_value * 100) if total_value > 0 else 0

        if cutoff is not None:
            holdings = holdings[
                holdings.initiated.apply(lambda q: quarter_to_date(q) >= cutoff if pd.notna(q) else False)
            ]
            if holdings.empty:
                continue

        holdings["discount"] = (holdings.current_price / holdings.avg_buy_price - 1) * 100
        holdings["investor"] = name

        hits = holdings[
            (holdings.pct_portfolio >= min_pct) &
            (holdings.discount <= -discount)
        ]
        if not hits.empty:
            all_hits.append(hits)

    return all_hits


def main():
    parser = argparse.ArgumentParser(
        description="Find conviction positions trading below the investor's avg buy price."
    )
    parser.add_argument("--min-pct", type=float, default=5.0,
                        help="Minimum portfolio weight %% (default 5)")
    parser.add_argument("--discount", type=float, default=20.0,
                        help="Max price vs avg buy, e.g. 20 means price ≤ 80%% of avg buy (default 20)")
    parser.add_argument("--max-age", type=int, default=4,
                        help="Only new buys initiated within the last N quarters (default 4 ≈ 1 year)")
    parser.add_argument("--sort", choices=["discount", "pct", "count"],
                        default="discount",
                        help="Sort results by: discount, pct (portfolio weight), count (# holders)")
    parser.add_argument("--workers", type=int, default=8,
                        help="Parallel workers for price fetching (default 8)")
    parser.add_argument("--only", nargs="+", metavar="NAME",
                        help="Only these investors")
    args = parser.parse_args()

    from dateutil.relativedelta import relativedelta
    cutoff = datetime.now() - relativedelta(months=args.max_age * 3)

    names = args.only if args.only else [p.stem for p in sorted(DATA_DIR.glob("*.csv"))]
    total = len(names)

    print(f"\n  {BOLD}{CYAN}SCREENER{RESET}  portfolio ≥ {args.min_pct}%  ·  price ≤ {100 - args.discount:.0f}% of avg buy  ·  new buys ≤ {args.max_age}Q")
    print(f"  {DIM}Scanning {total} investors  (cutoff {cutoff:%b %Y}) …{RESET}\n")

    # --- Phase 1: load holdings (fast, CPU only) ---
    print(f"  {DIM}Reading data …{RESET}", end="\r")
    investor_holdings = _load_all_holdings(names)
    all_tickers = set()
    for h in investor_holdings.values():
        all_tickers.update(h.ticker.tolist())
    print(f"  {DIM}{len(investor_holdings)} investors · {len(all_tickers)} unique tickers{RESET}")

    # --- Phase 2: fetch prices in parallel ---
    prices = _fetch_prices_parallel(all_tickers, args.workers)
    print(f"  {DIM}{len(prices)} prices fetched{RESET}\n")

    # --- Phase 3: apply prices and filter ---
    all_hits = _apply_prices_and_filter(investor_holdings, prices, cutoff, args.min_pct, args.discount)

    if not all_hits:
        print(f"  {DIM}No matches found.{RESET}\n")
        return

    result = pd.concat(all_hits, ignore_index=True)

    ticker_counts = {}
    for name, h in investor_holdings.items():
        for tk in h.ticker.unique():
            ticker_counts[tk] = ticker_counts.get(tk, 0) + 1
    result["n_investors"] = result.ticker.map(ticker_counts).fillna(1).astype(int)

    sort_map = {
        "discount": ("discount", True),
        "pct":      ("pct_portfolio", False),
        "count":    ("n_investors", False),
    }
    sort_col, ascending = sort_map[args.sort]
    result = result.sort_values(sort_col, ascending=ascending)

    TK_W, INV_W, BUY_W, PRC_W, AVG_W, DISC_W, PCT_W, CNT_W = 8, 22, 7, 12, 10, 9, 8, 10
    hdr = (f"{'Ticker':<{TK_W}s} │ {'Investor':<{INV_W}s} │ "
           f"{'Bought':>{BUY_W}s} │ "
           f"{'Cur. Price':>{PRC_W}s} │ {'Avg Buy':>{AVG_W}s} │ "
           f"{'Discount':>{DISC_W}s} │ {'Port%':>{PCT_W}s} │ "
           f"{'Holders':>{CNT_W}s}")
    sep = "─" * (TK_W + 3 + INV_W + 3 + BUY_W + 3 + PRC_W + 3 + AVG_W + 3 + DISC_W + 3 + PCT_W + 3 + CNT_W)

    print(f"  {BOLD}{CYAN}RESULTS  ({len(result)} matches){RESET}")
    print(f"  {DIM}{sep}{RESET}")
    print(f"  {DIM}{hdr}{RESET}")
    print(f"  {DIM}{sep}{RESET}")

    for _, r in result.iterrows():
        inv = r.investor
        if len(inv) > INV_W:
            inv = inv[:INV_W - 1] + "…"
        disc_str = f"{r.discount:+.1f}%"
        disc_color = RED if r.discount <= -20 else YELLOW
        cnt_str = f"{int(r.n_investors)}"
        cnt_color = GREEN if r.n_investors >= 3 else ""
        bought = r.initiated if pd.notna(r.initiated) else "?"
        print(f"  {r.ticker:<{TK_W}s} │ {inv:<{INV_W}s} │ "
              f"{bought:>{BUY_W}s} │ "
              f"${r.current_price:>{PRC_W - 1},.2f} │ ${r.avg_buy_price:>{AVG_W - 1},.2f} │ "
              f"{disc_color}{disc_str:>{DISC_W}s}{RESET} │ "
              f"{BOLD}{r.pct_portfolio:>{PCT_W - 1}.1f}%{RESET} │ "
              f"{cnt_color}{cnt_str:>{CNT_W}s}{RESET}")

    print(f"  {DIM}{sep}{RESET}")

    unique_tickers = result.ticker.nunique()
    unique_investors = result.investor.nunique()
    print(f"\n  {DIM}{unique_tickers} unique tickers across {unique_investors} investors{RESET}\n")


if __name__ == "__main__":
    main()
