"""
Rank super-investors by a chosen metric. Run from this directory so stats/ is found.
"""

import argparse
from pathlib import Path

import pandas as pd
import numpy as np

pd.set_option("display.float_format", "{:.4f}".format)

STATS_DIR = Path(__file__).parent / "stats"

# ── metric functions ────────────────────────────────────────────────────

def compute_win_rate(df: pd.DataFrame, mode: str) -> float:
    return (df[f"irr_{mode}"] > 0).mean()


def compute_percentage_capital_on_winning_stocks(df: pd.DataFrame, mode: str) -> float:
    return df.loc[df[f"irr_{mode}"] >= 0, f"cost_{mode}"].sum() / df[f"cost_{mode}"].sum()


def compute_weighted_return(df: pd.DataFrame, mode: str) -> float:
    tmp = df.copy()
    tmp["ending_value"] = tmp[f"cost_{mode}"] * (1 + tmp[f"irr_{mode}"]) ** tmp.holding_period
    return (tmp["ending_value"].sum() - tmp[f"cost_{mode}"].sum()) / tmp[f"cost_{mode}"].sum()


def compute_median_return(df: pd.DataFrame, mode: str) -> float:
    tmp = df.copy()
    tmp["total_return"] = (1 + tmp[f"irr_{mode}"]) ** tmp.holding_period - 1
    return tmp["total_return"].median()


def compute_median_return_on_winners(df: pd.DataFrame, mode: str) -> float:
    tmp = df.copy()
    tmp["total_return"] = (1 + tmp[f"irr_{mode}"]) ** tmp.holding_period - 1
    return tmp.loc[tmp["total_return"] >= 0, "total_return"].median()


def compute_median_return_on_losers(df: pd.DataFrame, mode: str) -> float:
    tmp = df.copy()
    tmp["total_return"] = (1 + tmp[f"irr_{mode}"]) ** tmp.holding_period - 1
    return tmp.loc[tmp["total_return"] < 0, "total_return"].median()


# ── build the full comparison table ─────────────────────────────────────

SORTABLE_METRICS = [
    "Win_Rate",
    "Pct_Capital_Winning_Stocks",
    "Median_Return",
    "Weighted_Return",
    "Median_Return_Winners",
    "Median_Return_Losers",
    "Sizing_Skill",
    "Safety_And_Returns",
]


def build_investor_stats(mode: str = "avg", after_year: int | None = None) -> pd.DataFrame:
    rows = []
    for csv_path in sorted(STATS_DIR.glob("*.csv")):
        investor_name = csv_path.stem
        df = pd.read_csv(csv_path)
        if df.empty:
            continue
        if after_year is not None and "period" in df.columns:
            buy_year = df["period"].str.extract(r"(\d{4})", expand=False).astype(int)
            df = df.loc[buy_year > after_year]
        if df.empty:
            continue
        rows.append({
            "Investor": investor_name,
            "Num_Trades": len(df),
            "Win_Rate": compute_win_rate(df, mode),
            "Pct_Capital_Winning_Stocks": compute_percentage_capital_on_winning_stocks(df, mode),
            "Median_Return": compute_median_return(df, mode),
            "Weighted_Return": compute_weighted_return(df, mode),
            "Median_Return_Winners": compute_median_return_on_winners(df, mode),
            "Median_Return_Losers": compute_median_return_on_losers(df, mode),
        })
    stats = pd.DataFrame(rows)
    stats["Sizing_Skill"] = stats["Weighted_Return"] - stats["Median_Return"]
    mask = (stats["Weighted_Return"] > 0) & (stats["Pct_Capital_Winning_Stocks"] > 0)
    stats["Safety_And_Returns"] = np.where(
        mask,
        2 / (1 / stats["Pct_Capital_Winning_Stocks"] + 1 / stats["Weighted_Return"]),
        np.nan,
    )
    return stats


# ── ANSI helpers ─────────────────────────────────────────────────────────

BOLD  = "\033[1m"
DIM   = "\033[2m"
GREEN = "\033[32m"
RED   = "\033[31m"
CYAN  = "\033[36m"
RESET = "\033[0m"

METRIC_LABELS = {
    "Win_Rate":                     "Win Rate",
    "Pct_Capital_Winning_Stocks":   "Cap % on Winners",
    "Median_Return":                "Median Return",
    "Weighted_Return":              "Weighted Return",
    "Median_Return_Winners":        "Med. Return (W)",
    "Median_Return_Losers":         "Med. Return (L)",
    "Sizing_Skill":                 "Sizing Skill",
    "Safety_And_Returns":           "Safety & Returns",
}


def color_pct(val: float, width: int = 8, inverse: bool = False) -> str:
    if pd.isna(val):
        return f"{DIM}{'n/a':>{width}s}{RESET}"
    pct = val * 100
    txt = f"{pct:+.1f}%"
    if inverse:
        col = RED if pct >= 0 else GREEN
    else:
        col = GREEN if pct >= 0 else RED
    return f"{col}{txt:>{width}s}{RESET}"


DISPLAY_COLS = [
    ("Win_Rate",                     "WR",        False),
    ("Weighted_Return",              "W.Ret",     False),
    ("Pct_Capital_Winning_Stocks",   "Cap%Win",   False),
    ("Median_Return_Losers",         "Med.Loss",  True),
]


def print_header(metric: str, mode: str, min_trades: int, total: int, after_year: int | None = None):
    label = METRIC_LABELS.get(metric, metric)
    print()
    print(f"  {BOLD}{CYAN}RANKING BY{RESET}  {BOLD}{label}{RESET}")
    sub = f"mode={mode}  min_trades={min_trades}  investors={total}"
    if after_year is not None:
        sub += f"  after_year={after_year}"
    print(f"  {DIM}{sub}{RESET}")
    print()


def _visual_len(s: str) -> int:
    import unicodedata
    n = 0
    for ch in s:
        w = unicodedata.east_asian_width(ch)
        n += 2 if w in ("W", "F") else 1
    return n


def _pad_right(s: str, width: int) -> str:
    return s + " " * max(0, width - _visual_len(s))


def print_ranking_block(title: str, df: pd.DataFrame, metric: str, *, is_top: bool):
    medals = {0: "🥇", 1: "🥈", 2: "🥉"}
    accent = GREEN if is_top else RED
    label = METRIC_LABELS.get(metric, metric)
    RANK_W  = 4
    INV_W   = 25
    MAIN_W  = max(10, len(label))
    COL_W   = 8
    TRADE_W = 6
    secondary = [(k, l, inv) for k, l, inv in DISPLAY_COLS if k != metric]
    print(f"  {accent}{BOLD}{'▲' if is_top else '▼'} {title}{RESET}")
    sep = "─" * (RANK_W + 1 + INV_W + 3 + MAIN_W + sum(3 + COL_W for _ in secondary) + 3 + TRADE_W)
    print(f"  {DIM}{sep}{RESET}")
    hdr = f"{'#':>{RANK_W}s} {'Investor':<{INV_W}s} │ {label:>{MAIN_W}s}"
    for _, cl, _ in secondary:
        hdr += f" │ {cl:>{COL_W}s}"
    hdr += f" │ {'Trades':>{TRADE_W}s}"
    print(f"  {DIM}{hdr}{RESET}")
    print(f"  {DIM}{sep}{RESET}")
    is_loser_metric = metric == "Median_Return_Losers"
    for i, (_, row) in enumerate(df.iterrows()):
        investor = row["Investor"]
        trades = int(row["Num_Trades"])
        if is_top and i in medals:
            rank_cell = f"{medals[i]} "
        else:
            rank_cell = f"  {i+1} "
        rank_cell = _pad_right(rank_cell, RANK_W + 1)
        if len(investor) > INV_W:
            investor = investor[: INV_W - 1] + "…"
        inv_cell = _pad_right(investor, INV_W)
        line = f"  {rank_cell}{inv_cell} │ {color_pct(row[metric], MAIN_W, inverse=is_loser_metric)}"
        for col_key, _, inverse in secondary:
            line += f" │ {color_pct(row[col_key], COL_W, inverse=inverse)}"
        line += f" │ {DIM}{trades:>{TRADE_W}d}{RESET}"
        print(line)
    print()


def main():
    parser = argparse.ArgumentParser(description="Rank super-investors by a chosen metric.")
    parser.add_argument("--metric", "-m", default="Weighted_Return", choices=SORTABLE_METRICS)
    parser.add_argument("--mode", default="avg", choices=["avg", "best", "worst"])
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--topk", "-k", type=int, default=3)
    parser.add_argument("--list-metrics", action="store_true")
    parser.add_argument("--after-year", type=int, default=None, metavar="YEAR",
                        help="Use only stocks first bought after this year (e.g. 2015 => 2016+)")
    args = parser.parse_args()
    if args.list_metrics:
        for m in SORTABLE_METRICS:
            print(f"  - {m}")
        return
    stats = build_investor_stats(mode=args.mode, after_year=args.after_year)
    stats = stats[stats["Num_Trades"] >= args.min_trades]
    stats.replace([np.inf, -np.inf], np.nan, inplace=True)
    ascending_for_losers = args.metric == "Median_Return_Losers"
    sorted_df = stats.sort_values(by=args.metric, ascending=ascending_for_losers, na_position="last")
    k = args.topk
    top = sorted_df.head(k) if not ascending_for_losers else sorted_df.tail(k).iloc[::-1]
    flop = sorted_df.tail(k).iloc[::-1] if not ascending_for_losers else sorted_df.head(k)
    print_header(args.metric, args.mode, args.min_trades, len(stats), args.after_year)
    print_ranking_block(f"TOP {k}", top, args.metric, is_top=True)
    print_ranking_block(f"FLOP {k}", flop, args.metric, is_top=False)


if __name__ == "__main__":
    main()
