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


def load_or_fetch_stats(investor_name: str) -> pd.DataFrame:
    cache = STATS_DIR / f"{investor_name}.csv"
    if cache.exists():
        print(f"  {DIM}Reading cached stats ({cache.name}){RESET}")
        return pd.read_csv(cache)
    print(f"  Fetching activity for {BOLD}{investor_name}{RESET} …")
    df = get_investor_activity(investor_name)
    df["stock"] = df["stock"].str.replace(".", "-", regex=False).str.upper()
    print(f"  Enriching with Yahoo price data …")
    df = add_yahoo_quarter_price_stats_batch(df)
    df = df.dropna()
    print(f"  Computing per-trade stats …")
    stats = compute_stats(df)
    STATS_DIR.mkdir(exist_ok=True)
    stats.to_csv(cache, index=False)
    print(f"  {DIM}Cached ({cache.name}){RESET}\n")
    return stats


def enrich_stats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for mode in ["avg", "worst", "best"]:
        df[f"exit_{mode}"] = df[f"cost_{mode}"] * (1 + df[f"irr_{mode}"]) ** df.holding_period
        df[f"return_{mode}"] = (1 + df[f"irr_{mode}"]) ** df.holding_period - 1
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
    print(f"  {DIM}{'─' * 70}{RESET}")


def print_overview(investor_name: str, stats: pd.DataFrame, mode: str):
    n = len(stats)
    winners = stats[stats[f"irr_{mode}"] > 0]
    losers = stats[stats[f"irr_{mode}"] <= 0]
    win_rate = len(winners) / n if n else 0
    total_cost = stats[f"cost_{mode}"].sum()
    total_exit = stats[f"exit_{mode}"].sum()
    weighted_return = (total_exit - total_cost) / total_cost if total_cost else 0
    cap_on_winners = winners[f"cost_{mode}"].sum() / total_cost if total_cost else 0
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
    hdr = f"  {'Ticker':<{TK_W}s} │ {'Return':>{RET_W}s} │ {'IRR':>{IRR_W}s} │ {'Cost($M)':>{COST_W}s} │ {'Years':>{HP_W}s} │ {'Pos%':>{POS_W}s} │ {'Status':>{ST_W}s}"
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
    stats = enrich_stats(stats)
    print_overview(args.investor, stats, args.mode)
    print_top_trades(stats, args.mode, args.topk)
    print_flop_trades(stats, args.mode, args.topk)
    print_biggest_positions(stats, args.mode, args.topk)
    print()


if __name__ == "__main__":
    main()
