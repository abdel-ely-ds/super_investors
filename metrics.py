from datetime import datetime


def compute_irr(profits, costs, holding_period):
    x = (1 + profits / costs)
    return -1 + x ** (1 / holding_period)


def quarter_to_date(q_str: str) -> datetime:
    """Convert 'Q1 2020' to a mid-quarter datetime."""
    quarter_part, year_str = q_str.split()
    year = int(year_str)
    quarter = int(quarter_part[-1])
    month = {1: 2, 2: 5, 3: 8, 4: 11}[quarter]
    return datetime(year, month, 15)


def compute_xirr(cash_flows: list) -> float:
    """Money-weighted annualised return (XIRR).

    *cash_flows* is a sorted list of ``(datetime, amount)`` tuples.
    Negative amounts are outflows (buys), positive are inflows (sells /
    unrealised value).  Returns the annual rate *r* that zeroes the NPV.
    """
    if len(cash_flows) < 2:
        return 0.0

    t0 = cash_flows[0][0]
    years = [(d - t0).days / 365.25 for d, _ in cash_flows]
    amounts = [a for _, a in cash_flows]

    if not any(a > 0 for a in amounts) or not any(a < 0 for a in amounts):
        return 0.0

    def xnpv(rate):
        return sum(a / (1 + rate) ** t for t, a in zip(years, amounts))

    lo, hi = -0.99, 10.0
    try:
        val_lo, val_hi = xnpv(lo), xnpv(hi)
    except (ZeroDivisionError, OverflowError):
        return 0.0

    if val_lo * val_hi > 0:
        total_out = sum(-a for a in amounts if a < 0)
        total_in = sum(a for a in amounts if a > 0)
        max_t = max(years) if max(years) > 0 else 1.0
        if total_out > 0:
            return (total_in / total_out) ** (1 / max_t) - 1
        return 0.0

    for _ in range(300):
        mid = (lo + hi) / 2
        try:
            val = xnpv(mid)
        except (ZeroDivisionError, OverflowError):
            hi = mid
            continue
        if val > 0:
            lo = mid
        else:
            hi = mid
        if abs(hi - lo) < 1e-10:
            break

    return (lo + hi) / 2


def quarter_diff_years(q_str1: str, q_str2: str) -> float:
    def to_total_quarters(q_str):
        quarter_part, year_str = q_str.split()
        year = int(year_str)
        quarter = int(quarter_part[-1])
        return year * 4 + quarter

    q1_total = to_total_quarters(q_str1)
    q2_total = to_total_quarters(q_str2)
    quarter_diff = abs(q1_total - q2_total)
    return quarter_diff / 4.0


def compute_position_sizing_skill(df, mode="avg"):
    df = df.copy()
    df["trade_ret"] = (1 + df[f"irr_{mode}"]) ** df["holding_period"] - 1
    weighted_exp_ret = (df[f"cost_{mode}"] * df["trade_ret"]).sum() / df[f"cost_{mode}"].sum()
    unweighted_exp_ret = df["trade_ret"].mean()
    return {
        "weighted_exp_ret": weighted_exp_ret,
        "unweighted_exp_ret": unweighted_exp_ret,
        "sizing_skill_score": weighted_exp_ret - unweighted_exp_ret,
    }
