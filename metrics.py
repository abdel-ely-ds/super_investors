def compute_irr(profits, costs, holding_period):
    x = (1 + profits / costs)
    return -1 + x ** (1 / holding_period)


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
