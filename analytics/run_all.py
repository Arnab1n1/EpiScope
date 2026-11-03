"""Run detection, forecasting, evaluation, optimisation, calendar and the serving export.  python -m analytics.run_all [--gt DIR] [--skip-backtest]
Ground truth (if present) is used ONLY for evaluation reporting, never by the detector."""
import argparse, json, os, time
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pipeline.common import spark_session, lake, ROOT
from analytics.detection import detect, evaluate as dev
from analytics.forecast import forecast as fcst, backtest as bt
from analytics.optimise import allocate as alloc
from analytics.calendar.calendar import build_calendar
from serving.export import export_all

EXP = os.path.join(ROOT, "docs/experiments")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gt", default=os.path.join(ROOT, "data/ground_truth"))
    ap.add_argument("--skip-backtest", action="store_true")
    ap.add_argument("--fa-cap", type=float, default=dev.FA_CAP_PER_DISTRICT_YEAR)
    a = ap.parse_args()
    os.makedirs(EXP, exist_ok=True)
    spark = spark_session("episcope-analytics")
    T = {}
    t = time.time(); detect.main(spark); T["detection"] = round(time.time() - t, 1)
    ref = spark.read.parquet(lake("ref_districts")).toPandas()
    ref["population"] = ref.population.astype(int)
    ex = {"assumptions": alloc.ASSUMPTIONS}
    gated = spark.read.parquet(lake("gold_detection_gated")).toPandas()
    naive = spark.read.parquet(lake("gold_detection_naive")).toPandas()
    for d in (gated, naive):
        d["week_start"] = pd.to_datetime(d.week_start)
    # ---- detection evaluation (needs ground truth; skipped gracefully when absent, e.g. a hidden drop) ----
    tau = 3.0
    ev = {}
    gt_ok = os.path.exists(os.path.join(a.gt, "ground_truth_outbreaks.csv"))
    if gt_ok:
        gt = dev.load_gt(a.gt, ref)
        curve_g = dev.sweep(gated, gt, "c2"); curve_n = dev.sweep(naive, gt, "c2"); curve_c = dev.sweep(gated, gt, "cusum")
        op = dev.operating_point(curve_g, a.fa_cap)
        tau = float(op.threshold)
        curve_g.assign(variant="gated_c2").to_csv(f"{EXP}/detection_curve_gated_c2.csv", index=False)
        curve_n.assign(variant="naive_c2").to_csv(f"{EXP}/detection_curve_naive_c2.csv", index=False)
        curve_c.assign(variant="gated_cusum").to_csv(f"{EXP}/detection_curve_gated_cusum.csv", index=False)
        _, by_c2 = dev.score(gated, gt, tau, "c2")
        opc = dev.operating_point(curve_c, a.fa_cap)
        _, by_cu = dev.score(gated, gt, float(opc.threshold), "cusum")
        by_c2.to_csv(f"{EXP}/detection_by_type_c2.csv", index=False); by_cu.to_csv(f"{EXP}/detection_by_type_cusum.csv", index=False)
        art = json.load(open(os.path.join(a.gt, "ground_truth_artefact.json"))) if os.path.exists(os.path.join(a.gt, "ground_truth_artefact.json")) else None
        ev = {"operating_point_c2": json.loads(op.to_json()), "operating_point_cusum": json.loads(opc.to_json()),
              "fa_cap_per_district_year": a.fa_cap,
              "artefact_alerts_naive": dev.artefact_alerts(naive, art, tau) if art else None,
              "artefact_alerts_gated": dev.artefact_alerts(gated, art, tau) if art else None,
              "curve_gated_c2": json.loads(curve_g.round(4).to_json(orient="records")),
              "curve_naive_c2": json.loads(curve_n.round(4).to_json(orient="records"))}
        try:
            import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(6.5, 4.2))
            for cv, lab, st in [(curve_g, "EARS C2 gated (timely reports)", "o-"), (curve_n, "EARS C2 naive (no gate)", "s--"), (curve_c, "CUSUM gated", "^-")]:
                ax.plot(cv.false_alerts_per_district_year, cv.sensitivity, st, label=lab)
                for r in cv.itertuples():
                    ax.annotate(f"{r.threshold:g}", (r.false_alerts_per_district_year, r.sensitivity), fontsize=6, xytext=(2, 2), textcoords="offset points")
            ax.axvline(a.fa_cap, color="grey", ls=":"); ax.set_xlabel("false alerts per district-year"); ax.set_ylabel("sensitivity")
            ax.set_title("Detection trade-off (conflict C1)"); ax.legend(fontsize=7); fig.tight_layout(); fig.savefig(f"{EXP}/fig_detection_tradeoff.png", dpi=160)
        except Exception as e:
            print("plot skipped:", e)
        print(f"[eval] operating point C2: tau={tau} sens={op.sensitivity:.2f} FA/dy={op.false_alerts_per_district_year:.2f} median_days={op.median_days_to_detection}")
        print(f"[eval] artefact-window alerts naive={ev['artefact_alerts_naive']} gated={ev['artefact_alerts_gated']}")
    else:
        print("[eval] no ground truth found -> detection evaluation skipped (normal for a hidden drop)")
    ex["tau"] = tau; ex["detection_eval"] = ev; ex["detection_gated"] = gated
    # ---- forecasting ----
    t = time.time()
    if not a.skip_backtest:
        fcst.main(spark, "backtest")
        res = bt.main(spark)
        ex["backtest"] = json.loads(res.to_json(orient="records"))
    elif os.path.exists(f"{EXP}/backtest_results.csv"):
        ex["backtest"] = json.loads(pd.read_csv(f"{EXP}/backtest_results.csv").to_json(orient="records"))   # reuse last full backtest
    T["backtest"] = round(time.time() - t, 1)
    t = time.time(); fcst.main(spark, "production"); T["forecast_production"] = round(time.time() - t, 1)
    fc = spark.read.parquet(lake("gold_forecast_production")).toPandas()
    ex["forecast"] = fc
    # ---- demand, weights, allocation grid ----
    t = time.time()
    demand = alloc.demand_from_forecast(fc)
    stock = spark.read.parquet(lake("gold_stock_district")).toPandas()
    latest = gated[gated.week_start == gated.week_start.max()]
    alerts_now = set(latest[(latest.c2 > tau) & (latest.value >= detect.MIN_CASES)].district_code)
    weights = alloc.severity_weights(ref, demand, stock, alerts_now)
    cells, summ, levels, alphas = alloc.grid(demand, stock, weights, ref, alerts_now)
    summ.to_csv(f"{EXP}/allocation_summary.csv", index=False)
    ex.update(demand=demand, stock=stock, weights=weights, grid_cells=cells, grid_summary=summ, levels=levels, alphas=alphas)
    T["optimisation"] = round(time.time() - t, 1)
    # ---- calendar ----
    onset = spark.read.parquet(lake("gold_epiweek_onset")).filter(F.col("disease_group") != "OTHER").select(
        "division", "disease_group", "week_start", "cases").toPandas()
    pop_by_div = ref.groupby("division").population.sum().to_dict()
    cal, curves = build_calendar(onset, pop_by_div)
    cal.to_csv(f"{EXP}/preparedness_calendar.csv", index=False)
    ex["calendar"], ex["curves"] = cal, curves
    # ---- serving ----
    export_all(spark, ex)
    json.dump(T, open(f"{EXP}/last_analytics_timings.json", "w"))
    spark.stop()


if __name__ == "__main__":
    main()
