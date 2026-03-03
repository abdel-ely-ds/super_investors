"""
Analyze a single super-investor's track record. Run from this directory.
"""

import argparse
import re
import shutil
import warnings
from pathlib import Path

import pandas as pd
import numpy as np

from dataroma import get_investor_activity
from yahoo import add_yahoo_quarter_price_stats_batch
from compute_stats import compute_stats

warnings.filterwarnings("ignore")

DATA_DIR  = Path(__file__).parent / "data"
STATS_DIR = Path(__file__).parent / "stats"

BOLD   = "\033[1m"
DIM    = "\033[2m"
GREEN  = "\033[32m"
RED    = "\033[31m"
CYAN   = "\033[36m"
RESET  = "\033[0m"


def color_pct(val: float, width: int = 8, inverse: bool = False) -> str:
    if pd.isna(val):
        return f"{DIM}{'n/a':>{width}s}{RESET}"
    pct = val * 100
    txt = f"{pct:+.1f}%"
    col = (RED if pct >= 0 else GREEN) if inverse else (GREEN if pct >= 0 else RED)
    return f"{col}{txt:>{width}s}{RESET}"


def _drop_invalid_costs(df: pd.DataFrame, min_cost: float = 0.1) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    for mode in ["best", "worst", "avg"]:
        col = f"cost_{mode}"
        if col in df.columns:
            positive = df[col][df[col] > 0]
            if positive.empty:
                return df.iloc[0:0]
            mask &= df[col].between(min_cost, positive.quantile(0.99) * 10, inclusive="both")
    return df[mask]


def load_or_fetch_stats(investor_name: str) -> pd.DataFrame:
    stats_cache = STATS_DIR / f"{investor_name}.csv"
    if stats_cache.exists():
        print(f"  {DIM}Reading cached stats ({stats_cache.name}){RESET}")
        return _drop_invalid_costs(pd.read_csv(stats_cache))

    data_cache = DATA_DIR / f"{investor_name}.csv"
    if data_cache.exists():
        print(f"  {DIM}Reading cached data ({data_cache.name}){RESET}")
        df = pd.read_csv(data_cache)
    else:
        print(f"  Fetching activity for {BOLD}{investor_name}{RESET} …")
        df = get_investor_activity(investor_name)
        df["stock"] = df["stock"].str.replace(".", "-", regex=False).str.upper()
        print(f"  Enriching with Yahoo price data …")
        df = add_yahoo_quarter_price_stats_batch(df)
        df = df.dropna()
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(data_cache, index=False)
        print(f"  {DIM}Saved data ({data_cache.name}){RESET}")

    print(f"  Computing per-trade stats …")
    stats = compute_stats(df)
    STATS_DIR.mkdir(exist_ok=True)
    stats.to_csv(stats_cache, index=False)
    print(f"  {DIM}Cached stats ({stats_cache.name}){RESET}\n")
    return _drop_invalid_costs(stats)


def enrich_stats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for mode in ["avg", "worst", "best"]:
        df[f"return_{mode}"] = (1 + df[f"irr_{mode}"]) ** df.holding_period - 1
        ret_col = f"ret_{mode}"
        if ret_col in df.columns:
            df[f"dollar_return_{mode}"] = df[ret_col]
        else:
            df[f"dollar_return_{mode}"] = df[f"return_{mode}"]
        df[f"exit_{mode}"] = df[f"cost_{mode}"] * (1 + df[f"dollar_return_{mode}"])
    df["cost_avg_pct"] = df.cost_avg / df.cost_avg.sum()
    return df


def _strip_ansi(s: str) -> str:
    return re.sub(r"\033\[[0-9;]*m", "", s)


def _visible_len(s: str) -> int:
    return len(_strip_ansi(s))


def _merge_columns(left: list, right: list, gap: int = 6) -> list:
    left_width = max((_visible_len(l) for l in left), default=0)
    height = max(len(left), len(right))
    merged = []
    for i in range(height):
        l = left[i] if i < len(left) else ""
        r = right[i] if i < len(right) else ""
        pad = left_width - _visible_len(l)
        merged.append(l + " " * (pad + gap) + r)
    return merged


def _get_term_width() -> int:
    return shutil.get_terminal_size((80, 24)).columns


def print_section(title: str):
    print(f"\n  {BOLD}{CYAN}{title}{RESET}")


def print_overview(investor_name: str, stats: pd.DataFrame, mode: str):
    n = len(stats)
    winners = stats[stats[f"irr_{mode}"] > 0]
    losers = stats[stats[f"irr_{mode}"] <= 0]
    win_rate = len(winners) / n if n else 0
    col_cost = f"cost_{mode}"
    col_dret = f"dollar_return_{mode}"
    total_cost = stats[col_cost].sum()
    total_exit = stats[f"exit_{mode}"].sum()
    weighted_return = (total_exit - total_cost) / total_cost if total_cost else 0
    cap_on_winners = winners[col_cost].sum() / total_cost if total_cost else 0
    weighted_irr = (stats[col_cost] * stats[f"irr_{mode}"]).sum() / total_cost if total_cost else 0
    median_irr = stats[f"irr_{mode}"].median()
    dollar_pnl = stats[col_cost] * stats[col_dret]
    profits = dollar_pnl.clip(lower=0).sum()
    losses_abs = abs(dollar_pnl.clip(upper=0).sum())
    profit_factor = profits / losses_abs if losses_abs else float("nan")
    irr_col = stats[f"irr_{mode}"]
    w_irr, l_irr = irr_col[irr_col > 0], irr_col[irr_col <= 0]
    expectancy = (len(w_irr) / n * w_irr.mean() - len(l_irr) / n * abs(l_irr.mean())) if n else 0
    downside_std = irr_col[irr_col < 0].std()
    sortino = median_irr / downside_std if downside_std and downside_std > 0 else float("nan")
    median_return = stats[f"return_{mode}"].median()
    median_return_w = winners[f"return_{mode}"].median() if len(winners) else float("nan")
    median_return_l = losers[f"return_{mode}"].median() if len(losers) else float("nan")
    median_holding = stats.holding_period.median()
    still_holding = stats.holding.sum() if "holding" in stats.columns else 0
    desc = stats[f"return_{mode}"].describe()
    print(f"\n  {BOLD}{'═' * 70}{RESET}")
    print(f"  {BOLD}{investor_name}{RESET}   {DIM}(mode={mode}){RESET}")
    print(f"  {BOLD}{'═' * 70}{RESET}")
    left = [
        f"  {BOLD}{CYAN}OVERVIEW{RESET}",
        f"  {DIM}{'─' * 50}{RESET}",
        f"  Trades:       {BOLD}{n}{RESET}   ({len(winners)} W, {len(losers)} L, {still_holding:.0f} holding)",
        f"  Win Rate:     {color_pct(win_rate)}",
        f"  W. Return:    {color_pct(weighted_return)}  {DIM}(capital-weighted){RESET}",
        f"  Cap% Winners: {color_pct(cap_on_winners)}  {DIM}(% capital on wins){RESET}",
        f"  Med. Holding: {BOLD}{median_holding:.1f}{RESET} yrs",
        "",
        f"  {BOLD}{CYAN}ANNUALIZED / RISK{RESET}",
        f"  {DIM}{'─' * 50}{RESET}",
        f"  Median IRR:   {color_pct(median_irr)}  {DIM}(per-trade ann.){RESET}",
        f"  W. IRR:       {color_pct(weighted_irr)}  {DIM}(capital-weighted ann.){RESET}",
        f"  Expectancy:   {color_pct(expectancy)}  {DIM}(expected ann. return/trade){RESET}",
        f"  Profit Factor:{BOLD}{profit_factor:>7.2f}x{RESET}  {DIM}($ won / $ lost){RESET}" if not pd.isna(profit_factor) else f"  Profit Factor:{DIM}{'n/a':>8s}{RESET}",
        f"  Sortino:      {BOLD}{sortino:>+7.2f}{RESET}   {DIM}(med. IRR / downside vol){RESET}" if not pd.isna(sortino) else f"  Sortino:      {DIM}{'n/a':>8s}{RESET}",
    ]
    right = [
        f"{BOLD}{CYAN}RETURN DISTRIBUTION{RESET}",
        f"{DIM}{'─' * 30}{RESET}",
        f"Median Return:     {color_pct(median_return)}",
        f"Median Ret (W):    {color_pct(median_return_w)}",
        f"Median Ret (L):    {color_pct(median_return_l, inverse=True)}",
        "", f"{'Percentile':<10s} {'Return':>10s}", f"{DIM}{'─' * 22}{RESET}",
    ]
    for pct_label, key in [("Min", "min"), ("25%", "25%"), ("50%", "50%"), ("75%", "75%"), ("Max", "max")]:
        right.append(f"{pct_label:<10s} {color_pct(desc[key], width=10)}")
    left_vis_width = max(_visible_len(l) for l in left)
    right_vis_width = max(_visible_len(r) for r in right)
    if _get_term_width() >= left_vis_width + 6 + right_vis_width + 4:
        for line in _merge_columns(left, right):
            print(line)
    else:
        for line in left:
            print(line)
        print()
        for line in right:
            print(f"  {line}")


def _print_trade_table(df: pd.DataFrame, mode: str):
    TK_W, RET_W, IRR_W, COST_W, HP_W, POS_W, ST_W = 8, 10, 8, 10, 6, 8, 6
    hdr = f"{'Ticker':<{TK_W}s} │ {'Return':>{RET_W}s} │ {'IRR':>{IRR_W}s} │ {'Cost($M)':>{COST_W}s} │ {'Years':>{HP_W}s} │ {'Pos%':>{POS_W}s} │ {'Status':>{ST_W}s}"
    sep = "─" * (TK_W + 3 + RET_W + 3 + IRR_W + 3 + COST_W + 3 + HP_W + 3 + POS_W + 3 + ST_W)
    print(f"  {DIM}{hdr}{RESET}")
    print(f"  {DIM}{sep}{RESET}")
    for _, row in df.iterrows():
        ticker = row["ticker"]
        if len(ticker) > TK_W:
            ticker = ticker[: TK_W - 1] + "…"
        ret = color_pct(row[f"return_{mode}"], width=RET_W)
        irr = color_pct(row[f"irr_{mode}"], width=IRR_W)
        cost = f"{row[f'cost_{mode}']:>{COST_W},.1f}"
        hp = f"{row['holding_period']:>{HP_W}.1f}"
        pos = color_pct(row.get("cost_avg_pct", float("nan")), width=POS_W)
        status = f"{CYAN}{'holding':>{ST_W+1}s}{RESET}" if row.get("holding", False) else f"{DIM}{'closed':>{ST_W+1}s}{RESET}"
        print(f"  {ticker:<{TK_W}s} │ {ret} │ {irr} │ {cost} │ {hp} │ {pos} │ {status}")


def print_top_trades(stats: pd.DataFrame, mode: str, k: int):
    print_section(f"TOP {k} TRADES  (by total return)")
    _print_trade_table(stats.nlargest(k, f"return_{mode}"), mode)


def print_flop_trades(stats: pd.DataFrame, mode: str, k: int):
    print_section(f"FLOP {k} TRADES  (by total return)")
    _print_trade_table(stats.nsmallest(k, f"return_{mode}"), mode)


def print_biggest_positions(stats: pd.DataFrame, mode: str, k: int):
    print_section(f"TOP {k} POSITIONS  (by capital allocated)")
    _print_trade_table(stats.nlargest(k, "cost_avg_pct"), mode)


def main():
    parser = argparse.ArgumentParser(description="Analyze a super-investor's track record.")
    parser.add_argument("investor", nargs="?", help="Investor name")
    parser.add_argument("--mode", default="avg", choices=["avg", "best", "worst"])
    parser.add_argument("--topk", "-k", type=int, default=5)
    parser.add_argument("--list", "-l", action="store_true")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--after-year", type=int, default=None, metavar="YEAR",
                        help="Use only stocks first bought after this year (e.g. 2015 => 2016+)")
    args = parser.parse_args()
    if args.list:
        print(f"\n  {BOLD}Available investors (cached in stats/):{RESET}\n")
        for f in sorted(STATS_DIR.glob("*.csv")):
            print(f"    {f.stem}")
        print()
        return
    if not args.investor:
        parser.error("investor name is required (or use --list)")
    if args.refresh and (STATS_DIR / f"{args.investor}.csv").exists():
        (STATS_DIR / f"{args.investor}.csv").unlink()
    stats = load_or_fetch_stats(args.investor)
    stats.replace([np.inf, -np.inf], np.nan, inplace=True)
    stats = stats.dropna(subset=[f"irr_{args.mode}"])
    if args.after_year is not None:
        buy_year = stats["period"].str.extract(r"(\d{4})", expand=False).astype(int)
        stats = stats.loc[buy_year > args.after_year].copy()
        if stats.empty:
            print(f"  {DIM}No trades with first buy after {args.after_year}.{RESET}\n")
            return
        print(f"  {DIM}Filtered to {len(stats)} trades (first buy after {args.after_year}){RESET}\n")
    stats = enrich_stats(stats)
    print_overview(args.investor, stats, args.mode)
    print_top_trades(stats, args.mode, args.topk)
    print_flop_trades(stats, args.mode, args.topk)
    print_biggest_positions(stats, args.mode, args.topk)
    print()


if __name__ == "__main__":
    main()
