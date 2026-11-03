"""AN-6 / AN-7 / AN-8: demand conversion + allocation LP (PuLP/CBC) + efficiency-equity grid.
min  sum_d sum_r w_d * u_dr
s.t. u_dr >= yhat_dr - s_dr - x_dr ; sum c_r x_dr <= B ; x_dr >= alpha * p_d * sum_d' x_d'r ; sum_{d in k} sum_r v_r x_dr <= T_k
yhat uses the UPPER prediction quantile (q90) of forecast cases over the 4-week horizon."""
import json, os
import numpy as np
import pandas as pd
import pulp
import pyspark.sql.functions as F
from pipeline.common import lake, ROOT

# ---- AN-6 conversion assumptions (STATED; configurable). Replace with Ministry operational rates. ----
ASSUMPTIONS = {
    "admission_rate": {"DENGUE": 0.22, "DIARRHOEAL": 0.10, "ILI": 0.05, "MALARIA": 0.15},
    "length_of_stay_days": {"DENGUE": 4.0, "DIARRHOEAL": 2.0, "ILI": 3.0, "MALARIA": 3.0},
    "iv_saline_units_per_admission": {"DENGUE": 6.0, "DIARRHOEAL": 8.0, "ILI": 1.0, "MALARIA": 3.0},
    "ns1_test_rate": 0.60, "ns1_wastage": 0.10,
    "ors_sachets_per_case": 4.0,
    "antimalarial_courses_per_case": 1.0, "antimalarial_wastage": 0.10,
    "unit_cost": {"IV_SALINE": 1.0, "NS1_KITS": 3.0, "ORS": 0.2, "ANTIMALARIALS": 2.0},      # relative cost units
    "volume": {"IV_SALINE": 1.0, "NS1_KITS": 0.1, "ORS": 0.05, "ANTIMALARIALS": 0.05},        # transport volume per unit
    "horizon_weeks": 4,
}
RES = ["IV_SALINE", "NS1_KITS", "ORS", "ANTIMALARIALS"]
STOCK_COL = {"IV_SALINE": "iv_saline", "NS1_KITS": "ns1_kits", "ORS": "ors_sachets", "ANTIMALARIALS": "antimalarials"}


def demand_from_forecast(fc: pd.DataFrame, quantile="q0900", A=ASSUMPTIONS):
    """fc: production forecasts (district, disease, horizon, quantile cols). Returns per-district demand for the horizon."""
    f = fc.copy()
    f["cases"] = f[quantile]
    piv = f.groupby(["district_code", "disease_group"])["cases"].sum().unstack(fill_value=0.0)
    for d in ["DENGUE", "DIARRHOEAL", "ILI", "MALARIA"]:
        if d not in piv:
            piv[d] = 0.0
    out = pd.DataFrame(index=piv.index)
    adm = {d: piv[d] * A["admission_rate"][d] for d in A["admission_rate"]}
    out["beds_needed"] = sum(adm[d] * A["length_of_stay_days"][d] / 7.0 for d in adm)       # average occupied beds over a week
    out["IV_SALINE"] = sum(adm[d] * A["iv_saline_units_per_admission"][d] for d in adm)
    out["NS1_KITS"] = piv["DENGUE"] * A["ns1_test_rate"] * (1 + A["ns1_wastage"])
    out["ORS"] = piv["DIARRHOEAL"] * A["ors_sachets_per_case"]
    out["ANTIMALARIALS"] = piv["MALARIA"] * A["antimalarial_courses_per_case"] * (1 + A["antimalarial_wastage"])
    return out.reset_index()


def severity_weights(ref, demand, stock, alerts_now):
    d = ref.merge(demand[["district_code", "beds_needed"]], on="district_code", how="left").merge(
        stock[["district_code", "beds", "occupied_beds"]], on="district_code", how="left")
    free = (d["beds"] - d["occupied_beds"]).clip(lower=1)
    cap_def = (d["beds_needed"] / free).clip(0, 3).fillna(0)
    alert = d["district_code"].isin(alerts_now).astype(float)
    w = 1.0 + 0.5 * alert + 0.5 * cap_def          # components published separately in the dashboard (explainability)
    return pd.DataFrame({"district_code": d["district_code"], "w": w, "w_alert": alert, "w_capacity": cap_def})


def solve(demand, stock, weights, pop_share, budget, alpha, div_of, A=ASSUMPTIONS, transport_mult=1.5):
    D = list(demand.district_code)
    dm = demand.set_index("district_code"); st = stock.set_index("district_code"); w = weights.set_index("district_code")["w"]
    prob = pulp.LpProblem("alloc", pulp.LpMinimize)
    x = {(d, r): pulp.LpVariable(f"x_{d}_{r}", lowBound=0) for d in D for r in RES}
    u = {(d, r): pulp.LpVariable(f"u_{d}_{r}", lowBound=0) for d in D for r in RES}
    prob += pulp.lpSum(w[d] * u[d, r] for d in D for r in RES)
    for d in D:
        for r in RES:
            prob += u[d, r] >= dm.loc[d, r] - st.loc[d, STOCK_COL[r]] - x[d, r]
    prob += pulp.lpSum(A["unit_cost"][r] * x[d, r] for d in D for r in RES) <= budget
    if alpha > 0:
        for r in RES:
            tot = pulp.lpSum(x[d, r] for d in D)
            for d in D:
                prob += x[d, r] >= alpha * pop_share[d] * tot
    total_vol_cap = budget / np.mean(list(A["unit_cost"].values())) * np.mean(list(A["volume"].values()))
    for k in set(div_of.values()):
        ds = [d for d in D if div_of[d] == k]
        share = sum(pop_share[d] for d in ds)
        prob += pulp.lpSum(A["volume"][r] * x[d, r] for d in ds for r in RES) <= transport_mult * share * total_vol_cap * 3
    status = prob.solve(pulp.PULP_CBC_CMD(msg=0, timeLimit=60))
    ok = pulp.LpStatus[status] == "Optimal"
    alloc = pd.DataFrame([{"district_code": d, "resource": r, "allocation": (x[d, r].value() or 0.0) if ok else np.nan,
                           "unmet": (u[d, r].value() or 0.0) if ok else np.nan} for d in D for r in RES])
    return alloc, pulp.LpStatus[status]


def baseline_population(demand, stock, weights, pop_share, budget, A=ASSUMPTIONS):
    """Population-proportional baseline: each district gets pop_share of the budget, split across resources by national demand cost share."""
    dm = demand.set_index("district_code"); st = stock.set_index("district_code"); w = weights.set_index("district_code")["w"]
    cost_share = {r: dm[r].sum() * A["unit_cost"][r] for r in RES}
    tot = sum(cost_share.values()) or 1.0
    rows = []
    for d in dm.index:
        for r in RES:
            xr = pop_share[d] * budget * (cost_share[r] / tot) / A["unit_cost"][r]
            rows.append({"district_code": d, "resource": r, "allocation": xr,
                         "unmet": max(0.0, dm.loc[d, r] - st.loc[d, STOCK_COL[r]] - xr)})
    return pd.DataFrame(rows)


def weighted_unmet(alloc, weights):
    w = weights.set_index("district_code")["w"]
    return float((alloc["unmet"] * alloc["district_code"].map(w)).sum())


def gini(v):
    v = np.sort(np.asarray(v, float)); n = len(v)
    return float((2 * np.arange(1, n + 1) - n - 1).dot(v) / (n * v.sum())) if v.sum() > 0 else 0.0


def grid(demand, stock, weights, ref, alerts_now):
    pop_share = (ref.set_index("district_code")["population"] / ref["population"].sum()).to_dict()
    div_of = ref.set_index("district_code")["division"].to_dict()
    full_cost = sum(demand[r].sum() * ASSUMPTIONS["unit_cost"][r] for r in RES)
    gap_cost = sum(max(0.0, (demand.set_index("district_code")[r] - stock.set_index("district_code")[STOCK_COL[r]]).clip(lower=0).sum()) *
                   ASSUMPTIONS["unit_cost"][r] for r in RES)
    levels = [0.1, 0.25, 0.5, 0.75, 1.0]
    alphas = [0.0, 0.1, 0.2, 0.3, 0.5]
    cells, summary = [], []
    for li, lv in enumerate(levels):
        B = lv * gap_cost
        base = baseline_population(demand, stock, weights, pop_share, B)
        for a in alphas:
            alloc, status = solve(demand, stock, weights, pop_share, B, a, div_of)
            if status == "Optimal":
                per_cap = (alloc.groupby("district_code")["allocation"].sum() / pd.Series(pop_share)).min()
                g = gini(alloc.groupby("district_code")["allocation"].sum().values)
                wu = weighted_unmet(alloc, weights)
            else:
                per_cap, g, wu = np.nan, np.nan, np.nan
            summary.append({"budget_level": lv, "budget": round(B, 1), "alpha": a, "status": status,
                            "weighted_unmet_lp": wu, "weighted_unmet_baseline": weighted_unmet(base, weights),
                            "min_alloc_per_pop_share": per_cap, "gini_allocation": g})
            if status == "Optimal":
                al = alloc.copy(); al["li"] = li; al["ai"] = alphas.index(a)
                cells.append(al)
    return pd.concat(cells, ignore_index=True), pd.DataFrame(summary), levels, alphas
