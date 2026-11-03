"""AN-2 / AN-3: score detection against ground truth (evaluation only; the DETECTOR never imports or reads ground-truth files).
Metrics: sensitivity, false alerts per district-year, median days to detection; threshold trade-off curve; artefact comparison."""
import json, os
import numpy as np
import pandas as pd
from analytics.detection.detect import alerts_from

FA_CAP_PER_DISTRICT_YEAR = 12.0       # operating-point rule (conflict C1): max sensitivity s.t. false alerts <= 12 / district-year


def load_gt(gt_dir, districts):
    ob = pd.read_csv(os.path.join(gt_dir, "ground_truth_outbreaks.csv"), parse_dates=["start", "end"])
    name2code = dict(zip(districts.district_name, districts.district_code))
    rows = []
    for r in ob.itertuples():
        codes = list(districts.district_code) if r.district == "ALL" else [name2code[r.district]]
        for c in codes:
            rows.append((r.outbreak_id, c, r.disease, r.start, r.end, r.shape))
    return pd.DataFrame(rows, columns=["outbreak_id", "district_code", "disease_group", "start", "end", "shape"])


def score(scored, gt, tau, method="c2", n_districts=64):
    al = alerts_from(scored, tau, method)
    al = al.assign(week_start=pd.to_datetime(al.week_start))
    det, days, rows = 0, [], []
    for g in gt.itertuples():
        a = al[(al.district_code == g.district_code) & (al.disease_group == g.disease_group) &
               (al.week_start + pd.Timedelta(days=6) >= g.start) & (al.week_start <= g.end + pd.Timedelta(days=14))]
        if len(a):
            det += 1
            days.append(max(0, (a.week_start.min() - g.start).days))
            rows.append((g.shape, 1, days[-1]))
        else:
            rows.append((g.shape, 0, np.nan))
    # false alerts: alerts outside every outbreak window (+tail) of the same district & disease
    fa = 0
    if len(al):
        keys = {(r.district_code, r.disease_group): [] for r in gt.itertuples()}
        for r in gt.itertuples():
            keys[(r.district_code, r.disease_group)].append((r.start - pd.Timedelta(days=7), r.end + pd.Timedelta(days=21)))
        for r in al.itertuples():
            wins = keys.get((r.district_code, r.disease_group), [])
            if not any(a <= r.week_start <= b for a, b in wins):
                fa += 1
    span_years = max((pd.to_datetime(scored.week_start).max() - pd.to_datetime(scored.week_start).min()).days / 365.25, 0.1)
    by_type = pd.DataFrame(rows, columns=["shape", "detected", "days"]).groupby("shape").agg(
        n=("detected", "size"), detected=("detected", "sum"), median_days=("days", "median")).reset_index()
    return {"threshold": tau, "method": method, "sensitivity": det / max(len(gt), 1), "outbreaks_detected": int(det),
            "outbreaks_total": int(len(gt)), "false_alerts": int(fa),
            "false_alerts_per_district_year": fa / (n_districts * span_years),
            "median_days_to_detection": float(np.median(days)) if days else np.nan}, by_type


def sweep(scored, gt, method="c2", grid=None):
    grid = grid or ([1.5, 2, 2.5, 3, 3.5, 4, 5, 6, 8] if method == "c2" else [1, 2, 3, 4, 5, 6, 8])
    return pd.DataFrame([score(scored, gt, t, method)[0] for t in grid])


def operating_point(curve, cap=FA_CAP_PER_DISTRICT_YEAR):
    ok = curve[curve.false_alerts_per_district_year <= cap]
    if ok.empty:
        return curve.sort_values("false_alerts_per_district_year").iloc[0]
    return ok.sort_values(["sensitivity", "threshold"], ascending=[False, True]).iloc[0]


def artefact_alerts(scored, art, tau, method="c2"):
    """Alerts raised in the artefact district during the silence window and the back-fill week (should be none for the gated detector)."""
    al = alerts_from(scored, tau, method)
    al = al.assign(week_start=pd.to_datetime(al.week_start))
    code = scored[scored.district_name == art["district"]].district_code.iloc[0]
    lo = pd.Timestamp(art["start"]) - pd.Timedelta(days=6); hi = pd.Timestamp(art["backfill"]) + pd.Timedelta(days=6)
    return int(((al.district_code == code) & (al.week_start >= lo) & (al.week_start <= hi)).sum())
