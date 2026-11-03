"""AN-5: evaluate the rolling-origin backtest: MAE, relative MAE vs seasonal naive, WIS, 80% PI coverage (per disease & horizon)."""
import os
import numpy as np
import pandas as pd
from scipy.stats import nbinom
from pipeline.common import lake, ROOT

ALPHAS = {0.05: ("q0025", "q0975"), 0.2: ("q0100", "q0900"), 0.5: ("q0250", "q0750")}


def interval_score(y, lo, hi, a):
    return (hi - lo) + (2 / a) * np.maximum(lo - y, 0) + (2 / a) * np.maximum(y - hi, 0)


def wis(df):
    K = len(ALPHAS)
    s = 0.5 * np.abs(df["actual"] - df["q0500"])
    for a, (lo, hi) in ALPHAS.items():
        s = s + (a / 2) * interval_score(df["actual"], df[lo], df[hi], a)
    return s / (K + 0.5)


def baseline_quantiles(df):
    """Seasonal-naive point forecast with NB intervals using the model's alpha, so interval scores are comparable."""
    mu = np.maximum(df["seasonal_naive"].astype(float), 0.5)
    n = 1.0 / df["alpha"]; p = n / (n + mu)
    out = pd.DataFrame({"actual": df["actual"].values})
    for name, q in [("q0025", .025), ("q0100", .1), ("q0250", .25), ("q0500", .5), ("q0750", .75), ("q0900", .9), ("q0975", .975)]:
        out[name] = nbinom.ppf(q, n.values, p.values)
    return out


def evaluate(bt: pd.DataFrame):
    bt = bt.dropna(subset=["actual", "seasonal_naive"]).reset_index(drop=True).copy()
    base = baseline_quantiles(bt)
    bt["wis_model"] = wis(bt)
    bt["wis_base"] = wis(base)
    bt["ae_mean"] = (bt["mean"] - bt["actual"]).abs()
    bt["ae_base"] = (bt["seasonal_naive"] - bt["actual"]).abs()
    bt["cover80"] = ((bt["actual"] >= bt["q0100"]) & (bt["actual"] <= bt["q0900"])).astype(float)
    g = bt.groupby(["disease_group", "horizon"]).agg(
        n=("actual", "size"), mae_seasonal_naive=("ae_base", "mean"), mae_model=("ae_mean", "mean"),
        wis_seasonal_naive=("wis_base", "mean"), wis_model=("wis_model", "mean"), coverage80=("cover80", "mean")).reset_index()
    g["relative_mae"] = g["mae_model"] / g["mae_seasonal_naive"]
    g["relative_wis"] = g["wis_model"] / g["wis_seasonal_naive"]
    return g.round(4), bt


def main(spark):
    bt = spark.read.parquet(lake("gold_forecast_backtest")).toPandas()
    res, detail = evaluate(bt)
    os.makedirs(os.path.join(ROOT, "docs/experiments"), exist_ok=True)
    res.to_csv(os.path.join(ROOT, "docs/experiments/backtest_results.csv"), index=False)
    print(res.to_string(index=False))
    return res
