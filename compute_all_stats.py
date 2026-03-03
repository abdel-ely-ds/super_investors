"""
Compute per-trade stats from the enriched data in data/.
Reads each investor CSV from data/, runs compute_stats(), saves to stats/.
"""

import argparse
import time
import warnings
from pathlib import Path

import pandas as pd

from compute_stats import compute_stats

warnings.filterwarnings("ignore")

DATA_DIR  = Path(__file__).parent / "data"
STATS_DIR = Path(__file__).parent / "stats"

BOLD  = "\033[1m"
DIM   = "\033[2m"
GREEN = "\033[32m"
RED   = "\033[31m"
CYAN  = "\033[36m"
RESET = "\033[0m"


def compute_one(investor_name: str) -> bool:
    data_path = DATA_DIR / f"{investor_name}.csv"
    if not data_path.exists():
        print(f"    {RED}No data file (run fetch_all_data first){RESET}")
        return False
    try:
        df = pd.read_csv(data_path)
        if df.empty:
            print(f"    {RED}Empty data file{RESET}")
            return False
        stats = compute_stats(df)
        if stats.empty:
            print(f"    {RED}No valid trades after computation{RESET}")
            return False
        STATS_DIR.mkdir(exist_ok=True)
        stats.to_csv(STATS_DIR / f"{investor_name}.csv", index=False)
        print(f"    {GREEN}Saved {len(stats)} trades{RESET}")
        return True
    except Exception as e:
        print(f"    {RED}Error: {e}{RESET}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Compute stats from cached data.")
    parser.add_argument("--refresh", action="store_true", help="Recompute all (ignore existing stats)")
    parser.add_argument("--only", nargs="+", metavar="NAME", help="Only these investors")
    args = parser.parse_args()

    available = [p.stem for p in sorted(DATA_DIR.glob("*.csv"))] if DATA_DIR.exists() else []
    if not available:
        print(f"\n  {RED}No data files found in data/. Run fetch_all_data first.{RESET}\n")
        return

    names = args.only if args.only else available
    total = len(names)
    skipped = success = failed = 0

    print(f"\n  {BOLD}{CYAN}Computing stats for {total} investors{RESET}")
    print(f"  {DIM}Input: data/  →  Output: stats/{RESET}\n")
    t0 = time.time()

    for i, name in enumerate(names, 1):
        stats_path = STATS_DIR / f"{name}.csv"
        if stats_path.exists() and not args.refresh:
            skipped += 1
            print(f"  [{i:>2}/{total}] {DIM}{name} — cached{RESET}")
            continue
        print(f"  [{i:>2}/{total}] {BOLD}{name}{RESET}")
        if compute_one(name):
            success += 1
        else:
            failed += 1

    elapsed = time.time() - t0
    print(f"\n  {BOLD}{'═' * 50}{RESET}")
    print(f"  {GREEN}Done in {elapsed:.0f}s{RESET}  {success} computed  ·  {skipped} cached  ·  {failed} failed\n")


if __name__ == "__main__":
    main()
