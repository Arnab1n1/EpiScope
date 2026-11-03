"""Serving layer (VIS-4, PRV-2, PRV-3): gold + analytics -> small, aggregated, suppressed JSON for the D3 dashboard.
The browser never receives line-level data. Total payload is asserted <= 10 MB (tests/test_payload.py).
Counts (patient counts) are suppressed when 1..4; zeros are published; forecasts/allocations are model outputs, not patient counts."""
import json, os, datetime as dt
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pipeline.common import lake, OUT, ROOT
from serving.suppress import suppress_series, complementary_mask, THRESHOLD

DISEASES = ["DENGUE", "DIARRHOEAL", "ILI", "MALARIA"]
DEMO_DISTRICTS = ["D01", "D14", "D15"]       # district-role bundles generated for these (config: EPISCOPE_DEMO_DISTRICTS)


def _w(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, separators=(",", ":"), default=lambda o: None if (isinstance(o, float) and np.isnan(o)) else float(o) if isinstance(o, (np.floating,)) else int(o) if isinstance(o, (np.integer,)) else str(o))


def export_all(spark, extras: dict):
    """extras: dict of pandas frames/objs produced by analytics: forecast, demand, stock, weights, grid_cells, grid_summary,
    levels, alphas, calendar, curves, detection_eval, backtest, thresholds."""
    out = OUT
    ref = spark.read.parquet(lake("ref_districts")).toPandas()
    ref["population"] = ref["population"].astype(int)
    ref = ref.sort_values("district_code").reset_index(drop=True)
    codes = list(ref.district_code)
    surv = (spark.read.parquet(lake("gold_surveillance")).filter(F.col("disease_group").isin(DISEASES))
            .select("district_code", "disease_group", "week_start", "cases_reported", "artefact_suspect", "completeness").toPandas())
    surv["week_start"] = pd.to_datetime(surv.week_start)
    weeks = sorted(surv.week_start.unique())
    wk_str = [pd.Timestamp(w).strftime("%Y-%m-%d") for w in weeks]
    data_through = wk_str[-1]

    # ---------------- series (district x disease x week), suppressed ----------------
    piv = surv.pivot_table(index=["district_code", "disease_group"], columns="week_start", values="cases_reported", aggfunc="sum").fillna(0)
    series, art_weeks = {}, {}
    for c in codes:
        series[c] = {}
        for d in DISEASES:
            if (c, d) in piv.index:
                series[c][d] = suppress_series(piv.loc[(c, d)].reindex(weeks).values)
    sus = surv[surv.artefact_suspect].groupby("district_code").week_start.apply(lambda s: sorted({wk_str[weeks.index(x)] for x in s if x in weeks}))
    art_weeks = {c: v for c, v in sus.items()}
    # division x week matrices with complementary suppression (published instead of raw district cells for other-district views)
    div_of = dict(zip(ref.district_code, ref.division))
    divs = sorted(set(div_of.values()))
    div_tab = {}
    for d in DISEASES:
        M = np.array([[sum((piv.loc[(c, d)].reindex(weeks).values if (c, d) in piv.index else np.zeros(len(weeks)))
                           for c in codes if div_of[c] == dv)][0] for dv in divs]).reshape(len(divs), len(weeks))
        mask = complementary_mask(M)
        div_tab[d] = [[None if mask[i, j] else int(M[i, j]) for j in range(len(weeks))] for i in range(len(divs))]
    series_obj = {"weeks": wk_str, "series": series, "artefact_weeks": art_weeks, "suppression_threshold": THRESHOLD,
                  "note": "null = suppressed cell (1-4 cases) - shown hatched, never as zero"}
    _w(f"{out}/series.json", series_obj)
    _w(f"{out}/division_week.json", {"weeks": wk_str, "divisions": divs, "diseases": div_tab})
    # ---------------- districts ----------------
    _w(f"{out}/districts.json", [{"code": r.district_code, "name": r.district_name, "division": r.division, "lat": float(r.lat),
                                  "lon": float(r.lon), "population": int(r.population)} for r in ref.itertuples()])

    # ---------------- forecasts + demand ----------------
    fc = extras["forecast"]
    fobj = {}
    for (c, d), g in fc.groupby(["district_code", "disease_group"]):
        g = g.sort_values("horizon")
        fobj.setdefault(c, {})[d] = {"target_weeks": [str(x) for x in g.target_week], "median": [round(float(x), 1) for x in g.q0500],
                                      "q10": [round(float(x), 1) for x in g.q0100], "q90": [round(float(x), 1) for x in g.q0900],
                                      "q025": [round(float(x), 1) for x in g.q0025], "q975": [round(float(x), 1) for x in g.q0975]}
    _w(f"{out}/forecasts.json", {"origin_week": data_through, "forecasts": fobj, "demand_assumptions": extras["assumptions"],
                                 "demand": {r.district_code: {k: round(float(getattr(r, k)), 1) for k in
                                                              ["beds_needed", "IV_SALINE", "NS1_KITS", "ORS", "ANTIMALARIALS"]}
                                            for r in extras["demand"].itertuples()}})

    # ---------------- alerts: threshold lines + recent C2 scores (client can re-threshold) ----------------
    det = extras["detection_gated"]
    det = det.assign(week_start=pd.to_datetime(det.week_start))
    tau = float(extras["tau"])
    last104 = [w for w in weeks][-104:]
    thr, c2recent = {}, {}
    for (c, d), g in det.groupby(["district_code", "disease_group"]):
        g = g.set_index("week_start").reindex(last104)
        thr.setdefault(c, {})[d] = {"mean": [None if pd.isna(m) else round(float(m), 2) for m in g.base_mean],
                                    "sd": [None if pd.isna(v) else round(float(v), 2) for v in g.base_sd]}
        c2recent.setdefault(c, {})[d] = [None if pd.isna(x) else round(float(x), 2) for x in g.c2.values[-12:]]
    _w(f"{out}/alerts.json", {"weeks_threshold": [pd.Timestamp(w).strftime("%Y-%m-%d") for w in last104], "tau": tau, "threshold": thr,
                              "c2_recent_weeks": [pd.Timestamp(w).strftime("%Y-%m-%d") for w in last104[-12:]], "c2_recent": c2recent,
                              "min_cases": 4})

    # ---------------- allocation grid ----------------
    cells, summ = extras["grid_cells"], extras["grid_summary"]
    levels, alphas = extras["levels"], extras["alphas"]
    RES = ["IV_SALINE", "NS1_KITS", "ORS", "ANTIMALARIALS"]
    gobj = {}
    for (li, ai), g in cells.groupby(["li", "ai"]):
        gobj[f"{li}_{ai}"] = {r: [round(float(g[(g.district_code == c) & (g.resource == r)].allocation.iloc[0]), 1) for c in codes] for r in RES}
    stock, dem, wts = extras["stock"], extras["demand"], extras["weights"]
    SC = {"IV_SALINE": "iv_saline", "NS1_KITS": "ns1_kits", "ORS": "ors_sachets", "ANTIMALARIALS": "antimalarials"}
    _w(f"{out}/allocation_grid.json", {
        "districts": codes, "resources": RES, "levels": levels, "alphas": alphas, "alloc": gobj,
        "demand": {r: [round(float(dem.set_index("district_code").loc[c, r]), 1) for c in codes] for r in RES},
        "stock": {r: [round(float(stock.set_index("district_code").loc[c, SC[r]]), 1) for c in codes] for r in RES},
        "weights": {"w": [round(float(wts.set_index("district_code").loc[c, "w"]), 3) for c in codes],
                    "alert": [float(wts.set_index("district_code").loc[c, "w_alert"]) for c in codes],
                    "capacity": [round(float(wts.set_index("district_code").loc[c, "w_capacity"]), 3) for c in codes]},
        "summary": json.loads(summ.round(4).to_json(orient="records")),
        "unit_cost": extras["assumptions"]["unit_cost"]})

    # ---------------- calendar ----------------
    _w(f"{out}/calendar.json", {"rows": json.loads(extras["calendar"].to_json(orient="records")), "curves": extras["curves"],
                                "lead_times_note": "pre-positioning and campaign lead times are stated assumptions (analytics/calendar)"})

    # ---------------- DQ report (DQ-2) ----------------
    dq = spark.read.parquet(lake("gold_dq_report"))
    rules = [c for c in dq.columns if c.startswith("fail_")]
    tot = dq.agg(F.sum("rows_in").alias("rows_in"), F.sum("rows_quarantined").alias("q"), *[F.sum(c).alias(c) for c in rules]).collect()[0].asDict()
    byrun = spark.read.parquet(lake("gold_dq_run")).orderBy(F.col("run_ts").desc()).limit(1).toPandas().iloc[0].to_dict()
    qre = (spark.read.parquet(lake("quarantine_encounters")).groupBy("reason_codes").count().toPandas())
    rows_in = int(tot["rows_in"] or 0)
    from pipeline.quality.rules import ROW_RULES, SET_RULES
    rule_tab = [{"rule": r[0], "severity": r[1], "description": r[2], "failed": int(tot.get("fail_" + r[0], 0) or 0),
                 "pass_rate": round(1 - (int(tot.get("fail_" + r[0], 0) or 0) / max(rows_in, 1)), 5)} for r in ROW_RULES]
    _w(f"{out}/dq_report.json", {"rows_in": rows_in, "rows_quarantined": int(tot["q"] or 0), "rules": rule_tab,
                                 "set_rules": [{"rule": r[0], "severity": r[1], "description": r[2]} for r in SET_RULES],
                                 "patient_dups_merged": int(byrun.get("patient_dups_merged", 0)),
                                 "late_extreme_excluded": int(byrun.get("late_extreme_excluded", 0)),
                                 "low_completeness_district_weeks": int(byrun.get("district_weeks_low_completeness", 0)),
                                 "quarantine_by_reason": json.loads(qre.to_json(orient="records"))})

    # ---------------- PRV-3: district-role bundles (own district in detail; others only at division level) ----------------
    # Written as separate files containing ONLY that district's series, alerts and forecasts: access control by construction, not by browser filtering.
    for c in os.environ.get("EPISCOPE_DEMO_DISTRICTS", ",".join(DEMO_DISTRICTS)).split(","):
        if c in series:
            _w(f"{out}/district/{c}.json", {
                "role": "district", "district": c, "weeks": wk_str, "series": {c: series[c]},
                "division_week": {"divisions": divs, "diseases": div_tab}, "artefact_weeks": {c: art_weeks.get(c, [])},
                "alerts": {"weeks_threshold": [pd.Timestamp(w).strftime("%Y-%m-%d") for w in last104], "tau": tau,
                           "threshold": {c: thr.get(c, {})}, "c2_recent_weeks": [pd.Timestamp(w).strftime("%Y-%m-%d") for w in last104[-12:]],
                           "c2_recent": {c: c2recent.get(c, {})}, "min_cases": 4},
                "forecasts": {c: fobj.get(c, {})}})

    # ---------------- evaluation summaries + meta ----------------
    _w(f"{out}/evaluation.json", {"detection": extras.get("detection_eval", {}), "backtest": extras.get("backtest", [])})
    _w(f"{out}/meta.json", {"generated_at": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "data_through_week": data_through,
                            "pipeline_version": "1.0", "synthetic_data": True, "suppression": {"threshold": THRESHOLD, "complementary": True},
                            "tau_default": tau, "n_districts": len(codes), "divisions": divs})
    total = sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(out) for f in fs if f.endswith(".json"))
    print(f"[serving] exported dashboard data: {total/1e6:.2f} MB (limit 10 MB)")
    return total
