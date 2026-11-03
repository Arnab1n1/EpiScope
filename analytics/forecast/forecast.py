"""AN-4 / AN-5: 1-4 week-ahead forecasts per district and disease with lagged weather covariates.
Model (interpretable, C5): Poisson-GLM mean with log link + moment-estimated NB2 dispersion
   log mu(t+h) = log(pop) + log(completeness) + b0 + harmonics(week-of-year, j=1,2) + g1*log1p(y_t) + g2*log1p(y_{t-1})
                 + d_p*precip(t+h-k) + d_T*tmean(t+h-k),  k >= h so every covariate is observed at forecast time
Prediction intervals: NB2 quantiles with alpha from Cameron-Trivedi moment regression. Trained per group in parallel (Spark applyInPandas).
Target = cases REPORTED in the week (what is known as-of the origin: no retrospective-revision leakage); artefact-suspect weeks are masked.
Baselines: seasonal naive y(t+h-52). All fits use only data up to the origin (rolling origin, refit every REFIT_STRIDE weeks)."""
import warnings
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import types as T
from scipy.stats import nbinom, poisson
import statsmodels.api as sm
from pipeline.common import lake

warnings.filterwarnings("ignore")
QUANT = [0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975]
HORIZONS = [1, 2, 3, 4]
REFIT_STRIDE = 4
N_TEST = 52
MIN_TRAIN = 70

OUT_SCHEMA = T.StructType([
    T.StructField("district_code", T.StringType()), T.StructField("disease_group", T.StringType()),
    T.StructField("origin_week", T.DateType()), T.StructField("horizon", T.IntegerType()),
    T.StructField("target_week", T.DateType()), T.StructField("mean", T.DoubleType()),
    *[T.StructField(f"q{int(q*1000):04d}", T.DoubleType()) for q in QUANT],
    T.StructField("seasonal_naive", T.DoubleType()), T.StructField("actual", T.DoubleType()),
    T.StructField("alpha", T.DoubleType()), T.StructField("weather_lag", T.IntegerType()),
    T.StructField("is_backtest", T.BooleanType()),
])
INPUT_COLS = ["district_code", "disease_group", "week_start", "cases_reported", "artefact_suspect", "completeness", "population",
              "precip_mm", "tmean_c"]


def _harm(woy):
    a = 2 * np.pi * woy / 52.18
    return np.column_stack([np.sin(a), np.cos(a), np.sin(2 * a), np.cos(2 * a)])


def _design(j_idx, t_idx, h, k, y, p, tm, woy, pop, comp):
    """Design rows for targets j_idx with origin t_idx = j_idx - h."""
    l0 = np.log1p(y[t_idx]); l1 = np.log1p(y[np.maximum(t_idx - 1, 0)])
    X = np.column_stack([np.ones(len(j_idx)), _harm(woy[j_idx]), l0, l1, p[j_idx - k], tm[j_idx - k]])
    off = np.log(pop / 1e5) + np.log(np.clip(comp[t_idx], 0.3, 1.0))
    return X, off


def _fit(X, yv, off):
    mu_std = X[:, -2:].std(axis=0) + 1e-9
    mu_mean = X[:, -2:].mean(axis=0)
    Xs = X.copy(); Xs[:, -2:] = (X[:, -2:] - mu_mean) / mu_std
    m = sm.GLM(yv, Xs, family=sm.families.Poisson(), offset=off).fit(maxiter=60)
    mu = m.predict(Xs, offset=off)
    a = float(np.sum(((yv - mu) ** 2 - mu) * mu ** 2) / max(np.sum(mu ** 4), 1e-9))
    return m, mu_mean, mu_std, float(np.clip(a, 1e-3, 5.0))


def _pred(m, mean_, std_, X, off):
    Xs = X.copy(); Xs[:, -2:] = (X[:, -2:] - mean_) / std_
    return np.clip(m.predict(Xs, offset=off), 1e-3, 1e5)


def _quantiles(mu, alpha):
    n = 1.0 / alpha
    p = n / (n + mu)
    return np.column_stack([nbinom.ppf(q, n, p) for q in QUANT])


def _choose_lag(y, p, h, upto):
    best, bk = -1, max(h, 2)
    idx = np.arange(12, upto)
    for k in range(max(h, 1), 9):
        a, b = np.log1p(y[idx]), p[idx - k]
        ok = np.isfinite(a) & np.isfinite(b)
        if ok.sum() > 30:
            c = abs(np.corrcoef(a[ok], b[ok])[0, 1])
            if c > best:
                best, bk = c, k
    return bk


def fit_group(pdf: pd.DataFrame, mode: str = "backtest") -> pd.DataFrame:
    pdf = pdf.sort_values("week_start").reset_index(drop=True)
    N = len(pdf)
    dc, dis = pdf.district_code.iloc[0], pdf.disease_group.iloc[0]
    pop = float(pdf.population.iloc[0])
    y = pdf.cases_reported.values.astype(float).copy()
    y[pdf.artefact_suspect.values.astype(bool)] = np.nan          # mask artefact weeks
    y_fill = np.where(np.isfinite(y), y, np.nan)
    yy = pd.Series(y_fill).interpolate(limit_direction="both").values   # used only for features, never as targets
    p = pdf.precip_mm.fillna(0).values.astype(float); tm = pdf.tmean_c.ffill().bfill().fillna(25).values.astype(float)
    comp = pdf.completeness.fillna(1.0).values.astype(float)
    weeks = pd.to_datetime(pdf.week_start).values
    woy = pd.DatetimeIndex(weeks).isocalendar().week.values.astype(float)
    rows = []

    def forecast_from(refit_t, origins, backtest):
        for h in HORIZONS:
            k = _choose_lag(yy, p, h, refit_t - h + 1)
            js = np.arange(14, refit_t - h + 1) + h              # target indices whose targets are known at refit_t
            js = js[np.isfinite(y[js])]
            if len(js) < MIN_TRAIN:
                continue
            X, off = _design(js, js - h, h, k, yy, p, tm, woy, pop, comp)
            ok = np.isfinite(X).all(axis=1)
            try:
                m, mm, ss, alpha = _fit(X[ok], y[js][ok], off[ok])
            except Exception:
                continue
            for t in origins:
                j = t + h
                if backtest:
                    if j > N - 1 or j < N - N_TEST:
                        continue
                    actual = y[j]
                    wk_target = weeks[j]
                else:
                    actual = np.nan
                    wk_target = (pd.Timestamp(weeks[N - 1]) + pd.Timedelta(days=7 * h)).to_datetime64()
                woy_j = woy.copy()
                if not backtest:
                    woy_j = np.append(woy, pd.Timestamp(wk_target).isocalendar()[1])
                jj = np.array([j])
                if not backtest:    # future target: build features directly
                    Xf = np.array([[1, *_harm(np.array([woy_j[-1]]))[0], np.log1p(yy[t]), np.log1p(yy[max(t - 1, 0)]),
                                    p[j - k], tm[j - k]]])
                    offf = np.log(pop / 1e5) + np.log(np.clip(comp[t], 0.3, 1.0))
                    offf = np.array([offf])
                else:
                    Xf, offf = _design(jj, jj - h, h, k, yy, p, tm, woy, pop, comp)
                mu = _pred(m, mm, ss, Xf, offf)
                q = _quantiles(mu, alpha)[0]
                sn_idx = j - 52
                sn = y[sn_idx] if (backtest and sn_idx >= 0 and np.isfinite(y[sn_idx])) else (y[N - 52 + h - 1] if (not backtest and N - 52 + h - 1 >= 0) else np.nan)
                rows.append((dc, dis, pd.Timestamp(weeks[t]).date(), h, pd.Timestamp(wk_target).date(), float(mu[0]),
                             *[float(v) for v in q], float(sn) if np.isfinite(sn) else None,
                             float(actual) if np.isfinite(actual) else None, float(alpha), int(k), bool(backtest)))

    if mode == "backtest":
        first = N - N_TEST - 4
        for r in range(first, N - 1, REFIT_STRIDE):
            forecast_from(r, [t for t in range(r, min(r + REFIT_STRIDE, N - 1))], True)
    else:
        forecast_from(N - 1, [N - 1], False)
    return pd.DataFrame(rows, columns=[f.name for f in OUT_SCHEMA.fields])


def run(spark, diseases, mode):
    s = spark.read.parquet(lake("gold_surveillance")).filter(F.col("disease_group").isin(diseases))
    s = s.select(*INPUT_COLS).withColumn("week_start", F.col("week_start").cast("date"))
    from functools import partial
    fn = partial(fit_group, mode=mode)

    def wrapper(pdf):
        return fn(pdf)
    out = s.groupBy("district_code", "disease_group").applyInPandas(wrapper, schema=OUT_SCHEMA)
    return out


def main(spark, mode, diseases=None):
    diseases = diseases or (["DENGUE", "DIARRHOEAL"] if mode == "backtest" else ["DENGUE", "DIARRHOEAL", "ILI", "MALARIA"])
    out = run(spark, diseases, mode)
    out.write.mode("overwrite").parquet(lake("gold_forecast_" + mode))
    print(f"[forecast] {mode}: wrote gold_forecast_{mode} for {diseases}")
