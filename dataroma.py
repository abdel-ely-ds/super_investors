import pandas as pd
from bs4 import BeautifulSoup
import requests

from investors import investors


def parse_activity(soup):
    data = []
    quarter_text = None
    elements = soup.find_all(["tr", "td"])
    i = 0

    while i < len(elements):
        el = elements[i]
        if el.name == "tr" and "q_chg" in el.get("class", []):
            quarter_text = " ".join([b.get_text(strip=True) for b in el.find_all("b")])
            i += 1
            continue
        if el.name == "td" and "stock" in el.get("class", []):
            stock_symbol = el.find("a").get_text().split()[0]
            activity = None
            activity_index = None
            j = i + 1
            while j < len(elements):
                next_td = elements[j]
                if next_td.name == "td" and (
                    "buy" in next_td.get("class", []) or "sell" in next_td.get("class", [])
                ):
                    activity = next_td.get_text(strip=True)
                    activity_index = j
                    break
                j += 1
            shares = None
            shares_index = None
            if activity_index is not None:
                k = activity_index + 1
                while k < len(elements):
                    td_k = elements[k]
                    if td_k.name == "td" and (
                        "buy" in td_k.get("class", []) or "sell" in td_k.get("class", [])
                    ):
                        shares_text = td_k.get_text(strip=True).replace(",", "")
                        try:
                            shares = int(shares_text)
                        except ValueError:
                            shares = None
                        shares_index = k
                        break
                    k += 1
            pct_change = None
            if shares_index is not None:
                k = shares_index + 1
                while k < len(elements):
                    td_k = elements[k]
                    if td_k.name == "td" and not set(td_k.get("class", [])) & {
                        "hist",
                        "stock",
                        "buy",
                        "sell",
                    }:
                        pct_change = float(td_k.get_text(strip=True))
                        break
                    k += 1
            if quarter_text and stock_symbol and activity:
                data.append(
                    {
                        "quarter": quarter_text,
                        "stock": stock_symbol,
                        "activity": activity,
                        "shares": shares,
                        "pct_change": pct_change,
                    }
                )
        i += 1
    return pd.DataFrame(data, columns=["quarter", "stock", "activity", "shares", "pct_change"])


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def get_investor_activity_one_page(investor_id, page=1):
    url = f"https://www.dataroma.com/m/m_activity.php?m={investor_id}&typ=a&L={page}&o=a"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser")


def get_investor_activity(investor_name):
    investor_id = investors[investor_name]
    page = 1
    dfs = []
    stop = False
    first_page_signature = None

    while not stop:
        print(f"  Fetching page {page}...", end="", flush=True)
        activity_soup = get_investor_activity_one_page(investor_id, page=page)
        df_activity = parse_activity(activity_soup)

        if df_activity.empty:
            stop = True
            print(" (no more data)")
        else:
            if len(df_activity) >= 3:
                page_signature = df_activity.head(3).to_string()
            else:
                page_signature = df_activity.to_string()
            if page == 1:
                first_page_signature = page_signature
                dfs.append(df_activity)
                print(f" ✓ ({len(df_activity)} activities)")
            elif page_signature == first_page_signature:
                stop = True
                print(" (loop detected, no more pages)")
            else:
                dfs.append(df_activity)
                print(f" ✓ ({len(df_activity)} activities)")
        page += 1

    total_df = pd.concat(dfs, ignore_index=True)
    print(f"✅ Fetched {len(total_df)} total activities from {len(dfs)} pages\n")
    return total_df
