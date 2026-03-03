"""
Fetch activity data for all investors and enrich with Yahoo price data.
Saves raw enriched CSVs to data/ (one per investor).
Does NOT compute stats — use compute_all_stats.py for that.
"""

import argparse
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

from dataroma import get_investor_activity
from yahoo import add_yahoo_quarter_price_stats_batch
from investors import investors

warnings.filterwarnings("ignore")

DATA_DIR = Path(__file__).parent / "data"

BOLD  = "\033[1m"
DIM   = "\033[2m"
GREEN = "\033[32m"
RED   = "\033[31m"
CYAN  = "\033[36m"
RESET = "\033[0m"

_print_lock = Lock()
_yahoo_lock = Lock()


def _log(msg: str):
    with _print_lock:
        print(msg)


def fetch_one(investor_name: str) -> tuple[str, bool, str]:
    """Returns (name, success, message)."""
    try:
        df = get_investor_activity(investor_name)
        df["stock"] = df["stock"].str.replace(".", "-", regex=False).str.upper()
        with _yahoo_lock:
            df = add_yahoo_quarter_price_stats_batch(df)
        df = df.dropna()
        if df.empty:
            return investor_name, False, "No usable data after cleaning"
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / f"{investor_name}.csv", index=False)
        tickers = df["stock"].nunique()
        return investor_name, True, f"{len(df)} rows ({tickers} tickers)"
    except Exception as e:
        return investor_name, False, str(e)


def main():
    parser = argparse.ArgumentParser(description="Fetch and cache enriched data for all investors.")
    parser.add_argument("--refresh", action="store_true", help="Re-fetch all (ignore cache)")
    parser.add_argument("--only", nargs="+", metavar="NAME", help="Only these investors")
    parser.add_argument("--workers", "-w", type=int, default=4, help="Parallel workers (default 4)")
    args = parser.parse_args()

    names = args.only if args.only else list(investors.keys())

    to_fetch = []
    skipped = 0
    for name in names:
        if (DATA_DIR / f"{name}.csv").exists() and not args.refresh:
            skipped += 1
        else:
            to_fetch.append(name)

    total = len(names)
    print(f"\n  {BOLD}{CYAN}Fetching data for {total} investors  ({args.workers} workers){RESET}")
    print(f"  {DIM}Cache: data/{RESET}")
    if skipped:
        print(f"  {DIM}{skipped} already cached, {len(to_fetch)} to fetch{RESET}")
    print()

    success = failed = 0
    done = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_one, name): name for name in to_fetch}
        for future in as_completed(futures):
            done += 1
            name, ok, msg = future.result()
            if ok:
                success += 1
                _log(f"  [{done:>2}/{len(to_fetch)}] {GREEN}✓{RESET} {name}  {DIM}({msg}){RESET}")
            else:
                failed += 1
                _log(f"  [{done:>2}/{len(to_fetch)}] {RED}✗{RESET} {name}  {RED}{msg}{RESET}")

    elapsed = time.time() - t0
    print(f"\n  {BOLD}{'═' * 50}{RESET}")
    print(f"  {GREEN}Done in {elapsed:.0f}s{RESET}  {success} fetched  ·  {skipped} cached  ·  {failed} failed\n")


if __name__ == "__main__":
    main()
