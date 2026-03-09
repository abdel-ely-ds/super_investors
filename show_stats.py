"""
Display the raw stats file for a given investor in a readable table format.
"""

import argparse
from pathlib import Path

import pandas as pd

from trade_stats import split_by_sells

DATA_DIR  = Path(__file__).parent / "data"
STATS_DIR = Path(__file__).parent / "stats"

BOLD  = "\033[1m"
DIM   = "\033[2m"
GREEN = "\033[32m"
RED   = "\033[31m"
CYAN  = "\033[36m"
RESET = "\033[0m"


def color_pct(val: float, width: int = 8) -> str:
    if pd.isna(val):
        return f"{DIM}{'n/a':>{width}s}{RESET}"
    pct = val * 100
    txt = f"{pct:+.1f}%"
    col = GREEN if pct >= 0 else RED
    return f"{col}{txt:>{width}s}{RESET}"


def fmt_price(val: float, width: int = 10) -> str:
    if pd.isna(val):
        return f"{DIM}{'n/a':>{width}s}{RESET}"
    if val >= 1000:
        return f"{val:>{width},.0f}"
    return f"{val:>{width},.2f}"


def _compute_cost_per_share(investor: str, mode: str) -> dict[str, float]:
    """Compute weighted avg cost per share for each trade from the raw data file."""
    data_path = DATA_DIR / f"{investor}.csv"
    if not data_path.exists():
        return {}

    price_col_map = {"best": "price_p10", "worst": "price_p90", "avg": "price_p50"}
    price_col = price_col_map[mode]

    raw = pd.read_csv(data_path)
    result = {}

    for ticker in raw.stock.unique():
        stock_hist = raw[raw.stock == ticker]
        splits = split_by_sells(stock_hist)

        for idx, split in enumerate(splits):
            buys = split[split["activity"].str.startswith(("Add", "Buy"))]
            if buys.empty or buys.iloc[-1].activity != "Buy":
                continue

            total_shares = buys.shares.sum()
            total_cost = (buys.shares * buys[price_col]).sum()
            avg_price = total_cost / total_shares if total_shares > 0 else float("nan")

            use_ticker = ticker + str(idx + 1) if len(splits) > 1 else ticker
            result[use_ticker] = avg_price

    return result


def show_stats(investor: str, sort_by: str, ascending: bool, mode: str):
    path = STATS_DIR / f"{investor}.csv"
    if not path.exists():
        print(f"\n  {RED}Stats file not found:{RESET} {path}")
        print(f"  Run {BOLD}python compute_all_stats.py{RESET} first, or check the name with --list\n")
        return

    df = pd.read_csv(path)
    if df.empty:
        print(f"\n  {DIM}No stats for {investor}.{RESET}\n")
        return

    irr_col = f"irr_{mode}"
    cost_col = f"cost_{mode}"
    ret_col = f"ret_{mode}"

    for col in [irr_col, cost_col, ret_col]:
        if col not in df.columns:
            print(f"\n  {RED}Column '{col}' not found. Available modes: avg, best, worst{RESET}\n")
            return

    cps_map = _compute_cost_per_share(investor, mode)
    df["cost_per_share"] = df["ticker"].map(cps_map)

    if sort_by == "irr":
        df = df.sort_values(irr_col, ascending=ascending)
    elif sort_by == "cost":
        df = df.sort_values("cost_per_share", ascending=ascending)
    elif sort_by == "ret":
        df = df.sort_values(ret_col, ascending=ascending)
    elif sort_by == "holding":
        df = df.sort_values("holding_period", ascending=ascending)
    else:
        df = df.sort_values(irr_col, ascending=False)

    n_trades = len(df)
    n_holding = df["holding"].sum() if "holding" in df.columns else 0
    n_closed = n_trades - n_holding

    print(f"\n  {BOLD}{'═' * 90}{RESET}")
    print(f"  {BOLD}{investor}{RESET}  —  stats file  {DIM}(mode={mode}, {n_trades} trades: {int(n_closed)} closed, {int(n_holding)} holding){RESET}")
    print(f"  {BOLD}{'═' * 90}{RESET}\n")

    TK_W, PER_W, IRR_W, CPS_W, RET_W, HP_W, ST_W = 8, 10, 9, 11, 9, 7, 8
    PREFIX = "  "

    def row_line(num, tk, per, irr_s, cps_s, ret_s, hp_s, st_s):
        return (
            f"{PREFIX}{num:>3s}  "
            f"{tk:<{TK_W}s} │ "
            f"{per:<{PER_W}s} │ "
            f"{irr_s:>{IRR_W}s} │ "
            f"{cps_s:>{CPS_W}s} │ "
            f"{ret_s:>{RET_W}s} │ "
            f"{hp_s:>{HP_W}s} │ "
            f"{st_s:>{ST_W}s}"
        )

    plain_hdr = row_line("#", "Ticker", "Period", "IRR", "Cost/Shr", "Return", "Years", "Status")
    sep = PREFIX + "─" * (len(plain_hdr) - len(PREFIX))

    print(f"{BOLD}{plain_hdr}{RESET}")
    print(f"{DIM}{sep}{RESET}")

    for i, (_, row) in enumerate(df.iterrows(), 1):
        ticker = str(row["ticker"])
        if len(ticker) > TK_W:
            ticker = ticker[: TK_W - 1] + "…"

        period = str(row.get("period", ""))
        if len(period) > PER_W:
            period = period[: PER_W - 1] + "…"

        irr = color_pct(row[irr_col], width=IRR_W)
        cps = fmt_price(row["cost_per_share"], width=CPS_W)
        ret = color_pct(row[ret_col], width=RET_W)
        hp = f"{row['holding_period']:>{HP_W}.1f}"

        if row.get("holding", False):
            status = f"{CYAN}{'holding':>{ST_W}s}{RESET}"
        else:
            status = f"{DIM}{'closed':>{ST_W}s}{RESET}"

        print(
            f"{PREFIX}{DIM}{i:>3d}{RESET}  "
            f"{ticker:<{TK_W}s} │ "
            f"{period:<{PER_W}s} │ "
            f"{irr} │ "
            f"{cps} │ "
            f"{ret} │ "
            f"{hp} │ "
            f"{status}"
        )

    print(f"\n  {DIM}Source: {path}{RESET}\n")


def list_investors():
    print(f"\n  {BOLD}Available investors (cached in stats/):{RESET}\n")
    files = sorted(STATS_DIR.glob("*.csv"))
    if not files:
        print(f"  {DIM}No stats files found. Run compute_all_stats.py first.{RESET}\n")
        return
    for f in files:
        print(f"    {f.stem}")
    print(f"\n  {DIM}{len(files)} investors total{RESET}\n")


def main():
    parser = argparse.ArgumentParser(description="Display an investor's stats file.")
    parser.add_argument("investor", nargs="?", help="Investor name (e.g. 'Warren Buffett')")
    parser.add_argument("--mode", default="avg", choices=["avg", "best", "worst"],
                        help="Which price scenario to display (default: avg)")
    parser.add_argument("--sort", default="irr", choices=["irr", "cost", "ret", "holding"],
                        help="Sort trades by this column (default: irr)")
    parser.add_argument("--asc", action="store_true", help="Sort ascending (default: descending)")
    parser.add_argument("--list", "-l", action="store_true", help="List available investors")
    args = parser.parse_args()

    if args.list:
        list_investors()
        return

    if not args.investor:
        parser.error("investor name is required (or use --list)")

    show_stats(args.investor, sort_by=args.sort, ascending=args.asc, mode=args.mode)


if __name__ == "__main__":
    main()
