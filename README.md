# Super Investors

Fetch, rank, and analyze track records of famous investors using 13F data from [Dataroma](https://www.dataroma.com/) and price data from Yahoo Finance.

## Setup

```bash
cd super_investors_open
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

## Usage

Run all commands from the `super_investors_open` directory.

### 1. Fetch stats (build the cache)

Download activity from Dataroma, enrich with Yahoo quarter prices, compute per-trade IRRs, and save one CSV per investor in `stats/`.

```bash
python fetch_all_stats.py                  # fetch only missing investors
python fetch_all_stats.py --refresh        # re-fetch everyone
python fetch_all_stats.py --only "Warren Buffett" "Li Lu"   # specific names
```

### 2. Rank investors

Show top / flop investors by a chosen metric.

```bash
python rank_investors.py                           # default: Weighted_Return
python rank_investors.py -m Win_Rate -k 5
python rank_investors.py -m Median_Return_Losers --min-trades 10
python rank_investors.py --list-metrics            # list metrics
```

Metrics: `Win_Rate`, `Pct_Capital_Winning_Stocks`, `Median_Return`, `Weighted_Return`, `Median_Return_Winners`, `Median_Return_Losers`, `Sizing_Skill`, `Safety_And_Returns`.

### 3. Analyze one investor

Overview, return distribution, top/flop trades, and biggest positions.

```bash
python analyze_investor.py "Warren Buffett"
python analyze_investor.py "Thomas Russo" -k 3 --mode best
python analyze_investor.py --list    # list cached investors
python analyze_investor.py "Li Lu" --refresh   # re-fetch then analyze
```

## Data

- **Source**: [Dataroma](https://www.dataroma.com/m/home.php) (13F-style activity).
- **Investors**: Defined in `investors.py` (name → Dataroma fund ID). You can add more by finding the fund ID in the site URL.
- **Pre-built stats**: Unzip the included archive to get cached per-trade stats (so you can rank/analyze without fetching):

  ```bash
  unzip stats.zip
  ```

  This creates `stats/<Investor Name>.csv` for each investor. To refresh or build from scratch, run `python fetch_all_stats.py`.

## License

MIT.
