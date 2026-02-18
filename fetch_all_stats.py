"""
Fetch activity data for all investors and compute per-trade stats.
Skips investors that already have a cached CSV in stats/ unless --refresh is used.
"""

import argparse
import time
import warnings
from pathlib import Path

import pandas as pd

from dataroma import get_investor_activity
from yahoo import add_yahoo_quarter_price_stats_batch
from compute_stats import compute_stats
from investors import investors

warnings.filterwarnings("ignore")

STATS_DIR = Path(__file__).parent / "stats"

BOLD  = "\033[1m"
DIM   = "\033[2m"
GREEN = "\033[32m"
RED   = "\033[31m"
CYAN  = "\033[36m"
RESET = "\033[0m"


def fetch_one(investor_name: str) -> bool:
    try:
        df = get_investor_activity(investor_name)
        df["stock"] = df["stock"].str.replace(".", "-", regex=False).str.upper()
        df = add_yahoo_quarter_price_stats_batch(df)
        df = df.dropna()
        if df.empty:
            print(f"    {RED}No usable data after cleaning{RESET}")
            return False
        stats = compute_stats(df)
        STATS_DIR.mkdir(exist_ok=True)
        stats.to_csv(STATS_DIR / f"{investor_name}.csv", index=False)
        print(f"    {GREEN}Saved {len(stats)} trades{RESET}")
        return True
    except Exception as e:
        print(f"    {RED}Error: {e}{RESET}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Fetch and cache stats for all investors.")
    parser.add_argument("--refresh", action="store_true", help="Re-fetch all (ignore cache)")
    parser.add_argument("--only", nargs="+", metavar="NAME", help="Only these investors")
    args = parser.parse_args()
    names = args.only if args.only else list(investors.keys())
    total = len(names)
    skipped = success = failed = 0
    print(f"\n  {BOLD}{CYAN}Fetching stats for {total} investors{RESET}")
    print(f"  {DIM}Cache: stats/{RESET}\n")
    t0 = time.time()
    for i, name in enumerate(names, 1):
        if (STATS_DIR / f"{name}.csv").exists() and not args.refresh:
            skipped += 1
            print(f"  [{i:>2}/{total}] {DIM}{name} — cached{RESET}")
            continue
        print(f"  [{i:>2}/{total}] {BOLD}{name}{RESET}")
        if fetch_one(name):
            success += 1
        else:
            failed += 1
    print(f"\n  {BOLD}{'═' * 50}{RESET}")
    print(f"  {GREEN}Done in {time.time() - t0:.0f}s{RESET}  {success} fetched  ·  {skipped} cached  ·  {failed} failed\n")


if __name__ == "__main__":
    main()
