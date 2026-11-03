"""EpiScope synthetic generator (S3 fallback, S4, S5, S6 + ground truth).

Deterministic for a given (--seed, --sf). At SF=1 targets ~25M encounters for 2021-2025.
Usage:
  python -m generator.generate --sf 0.05 --seed 42 --out data/raw
  python -m generator.generate --drop --seed 777 --out data/drop      # unannounced 14-day incremental drop
Ground truth is written to <out>/../ground_truth/ and must never be read by analytics/detection.
"""
import argparse, csv, json, os, hashlib
from datetime import date, timedelta
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST = pd.read_csv(os.path.join(ROOT, "data/reference/districts.csv"))
DISEASES = ["DENGUE", "DIARRHOEAL", "ILI", "MALARIA", "OTHER"]
ICD = {"DENGUE": ["A90", "A91"], "DIARRHOEAL": ["A00", "A09"], "ILI": ["J10", "J11"],
       "MALARIA": ["B50", "B51", "B52", "B53", "B54"],
       "OTHER": ["K29", "I10", "E11", "L03", "N39", "R50", "J06", "M54"]}
HILL = {"Bandarban", "Rangamati", "Khagrachhari"}
COASTAL = {"Cox's Bazar", "Barguna", "Patuakhali", "Bhola", "Satkhira", "Bagerhat", "Khulna", "Noakhali", "Lakshmipur", "Chattogram"}
URBAN = {"Dhaka", "Chattogram", "Gazipur", "Narayanganj", "Khulna", "Rajshahi", "Sylhet", "Barishal", "Cumilla", "Mymensingh"}
SIZE = {"DENGUE": 6.0, "DIARRHOEAL": 12.0, "ILI": 10.0, "MALARIA": 5.0, "OTHER": 40.0}  # NB dispersion (size)
LAG_DAYS = {"DENGUE": 42, "DIARRHOEAL": 10, "ILI": 14, "MALARIA": 35}
SHARE = {"DENGUE": 0.07, "DIARRHOEAL": 0.17, "ILI": 0.14, "MALARIA": 0.01, "OTHER": 0.61}
ARTEFACT = dict(district="Chattogram", start="2024-06-03", end="2024-06-23", backfill="2024-06-24")


def day_range(a, b):
    return pd.date_range(a, b, freq="D")


# ---------------------------------------------------------------- weather (S3)
def make_weather(start="2021-01-01", end="2026-12-31", cache=None):
    """Deterministic synthetic fallback. Replace with real Open-Meteo data via generator/fetch_weather.py
    (same columns: district_code,date,precip_mm,tmean_c,humidity); the generator uses the cached file if present."""
    if cache and os.path.exists(cache):
        return pd.read_csv(cache, parse_dates=["date"])
    days = day_range(start, end)
    doy = days.dayofyear.values
    out = []
    for i, r in DIST.iterrows():
        rng = np.random.default_rng([20260101, i])
        mon = np.exp(-((doy - 200) / 55.0) ** 2) + 0.15
        wet = rng.random(len(days)) < np.clip(0.55 * mon + 0.05, 0, 0.8)
        pr = np.where(wet, rng.gamma(1.6, 9 * mon * (1.4 if r.division in ("Sylhet", "Chattogram") else 1.0)), 0.0)
        ext = rng.random(len(days)) < 0.004
        pr = pr + ext * rng.gamma(3, 45, len(days))
        temp = 25.5 + 5.0 * np.sin(2 * np.pi * (doy - 110) / 365) + rng.normal(0, 1.2, len(days)) - 0.02 * (r.lat - 24) * 10
        hum = np.clip(70 + 12 * mon + rng.normal(0, 4, len(days)), 35, 99)
        out.append(pd.DataFrame({"district_code": r.district_code, "date": days, "precip_mm": pr.round(1),
                                 "tmean_c": temp.round(1), "humidity": hum.round(1)}))
    return pd.concat(out, ignore_index=True)


# ---------------------------------------------------------------- outbreaks
def make_outbreaks(seed, start, end):
    rng = np.random.default_rng([seed, 99])
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    span = (e - s).days
    rows = []
    # 1 slow-growing outbreak, 1 nationwide novel respiratory surge, then >= 30 local ones
    rows.append(dict(district="Cumilla", disease="DENGUE", start=s + pd.Timedelta(days=int(span * 0.55)), weeks=12, peak=2.4, shape="slow"))
    rows.append(dict(district="ALL", disease="ILI", start=s + pd.Timedelta(days=int(span * 0.40)), weeks=9, peak=4.5, shape="national"))
    names = DIST.district_name.tolist()
    n_local = 32 if span > 365 else 2
    for k in range(n_local):
        dis = rng.choice(["DENGUE", "DIARRHOEAL", "ILI", "MALARIA"], p=[0.35, 0.35, 0.2, 0.1])
        if dis == "MALARIA":
            d = rng.choice(sorted(HILL))
        elif dis == "DENGUE":
            d = rng.choice(sorted(URBAN | {"Cox's Bazar", "Jashore"}))
        else:
            d = rng.choice(names)
        st = s + pd.Timedelta(days=int(rng.integers(60, max(61, span - 70))))
        rows.append(dict(district=d, disease=dis, start=st, weeks=int(rng.integers(3, 9)),
                         peak=float(rng.uniform(2.8, 6.5)), shape="sharp"))
    out = pd.DataFrame(rows)
    out["end"] = out.start + pd.to_timedelta(out.weeks * 7, unit="D")
    out.insert(0, "outbreak_id", [f"OB{i+1:03d}" for i in range(len(out))])
    return out


def outbreak_multiplier(ob, days):
    t = ((days - ob.start).days / 7.0).values
    m = np.ones(len(days))
    inside = (t >= 0) & (t < ob.weeks)
    if ob.shape == "slow":
        m[inside] = 1 + (ob.peak - 1) * (t[inside] / ob.weeks)
    elif ob.shape == "national":
        m[inside] = 1 + (ob.peak - 1) * np.sin(np.pi * t[inside] / ob.weeks) ** 1.5
    else:
        up = np.minimum(1.0, (t[inside] + 0.5) / 1.2)
        dec = np.exp(-np.maximum(0, t[inside] - 1.5) / (ob.weeks / 2.2))
        m[inside] = 1 + (ob.peak - 1) * up * dec
    return m


# ---------------------------------------------------------------- registry (S5)
def make_registry(seed, n_fac=640):
    rng = np.random.default_rng([seed, 5])
    w = DIST.population / DIST.population.sum()
    n = np.maximum(6, np.round(w * n_fac)).astype(int)
    rows, fid = [], 1
    for i, r in DIST.iterrows():
        for j in range(n[i]):
            typ = "HOSPITAL" if j == 0 else ("UHC" if j < 4 else "CLINIC")
            beds = {"HOSPITAL": int(rng.integers(150, 900)), "UHC": int(rng.integers(30, 80)), "CLINIC": int(rng.integers(0, 15))}[typ]
            rows.append(dict(facility_id=f"F{fid:04d}", facility_name=f"{r.district_name} {typ.title()} {j+1}",
                             district_code=r.district_code, facility_type=typ,
                             lat=round(r.lat + rng.normal(0, 0.12), 4), lon=round(r.lon + rng.normal(0, 0.12), 4),
                             beds=beds, icu_beds=int(beds * 0.06) if typ != "CLINIC" else 0))
            fid += 1
    reg = pd.DataFrame(rows)
    reg.loc[rng.choice(len(reg), 3, replace=False), "lat"] = np.nan     # injected defect
    return reg


# ---------------------------------------------------------------- expected counts
def expected_matrix(seed, start, end, weather, outbreaks, sf):
    """E[d, g, t]: expected daily counts. Baseline (no outbreaks) is scaled so that 2021-2025 ~= 24e6*sf;
    outbreaks are then layered on top (total ~25M at SF=1). Computed over the full weather span and sliced."""
    full_days = day_range("2021-01-01", "2026-12-31")
    days = full_days
    T, nD = len(days), len(DIST)
    doy = days.dayofyear.values
    peak = {"DENGUE": 255, "DIARRHOEAL": 120, "ILI": 215, "MALARIA": 200}
    E = np.zeros((nD, 5, T))
    wide = weather.pivot(index="date", columns="district_code", values="precip_mm").reindex(days)
    wt = weather.pivot(index="date", columns="district_code", values="tmean_c").reindex(days)
    for i, r in DIST.iterrows():
        pop = r.population
        pr = wide[r.district_code].fillna(0).rolling(14, min_periods=1).sum()
        tm = wt[r.district_code].ffill().bfill()
        zp = ((pr - pr.mean()) / (pr.std() + 1e-9)).values
        zt = ((tm - tm.mean()) / (tm.std() + 1e-9)).values
        for g, dis in enumerate(DISEASES):
            if dis == "OTHER":
                E[i, g] = pop * SHARE["OTHER"]
                continue
            amp = {"DENGUE": 2.2 if r.district_name in URBAN else 0.5,
                   "DIARRHOEAL": 1.5 if r.district_name in COASTAL else 1.0,
                   "ILI": 1.3 if r.district_name in URBAN else 1.0,
                   "MALARIA": 40.0 if r.district_name in HILL else (0.8 if r.district_name == "Sylhet" else 0.02)}[dis]
            seas = np.exp(1.0 * np.cos(2 * np.pi * (doy - peak[dis]) / 365) + 0.25 * np.cos(4 * np.pi * (doy - peak[dis]) / 365))
            lag = LAG_DAYS[dis]
            zpl = np.roll(zp, lag); zpl[:lag] = 0
            ztl = np.roll(zt, lag); ztl[:lag] = 0
            clim = np.exp(0.45 * zpl + (0.3 * ztl if dis in ("DENGUE", "MALARIA") else 0.0))
            E[i, g] = pop * SHARE[dis] * amp * seas * clim
    mask = (days >= "2021-01-01") & (days <= "2025-12-31")
    E = E * (24e6 * sf / E[:, :, mask].sum())
    # outbreaks
    for ob in outbreaks.itertuples():
        g = DISEASES.index(ob.disease)
        rows_ = range(nD) if ob.district == "ALL" else [int(DIST.index[DIST.district_name == ob.district][0])]
        m = outbreak_multiplier(ob, days)
        for i in rows_:
            E[i, g] *= m
    keep = (days >= start) & (days <= end)
    return E[:, :, keep], days[keep]


# ---------------------------------------------------------------- main generation
def generate(out, sf, seed, start, end, drop=False, weather_cache=None):
    os.makedirs(out, exist_ok=True)
    gt_dir = os.path.join(os.path.dirname(os.path.abspath(out)), "ground_truth" + ("_drop" if drop else ""))
    os.makedirs(gt_dir, exist_ok=True)
    full_start, full_end = "2021-01-01", "2026-12-31"
    weather = make_weather(full_start, full_end, weather_cache)
    outbreaks = make_outbreaks(seed, start, end)
    registry = make_registry(20260101)  # registry is the same for base data and any drop (stable contract)
    # E is computed over the generation window; scale by window length relative to 5 years
    E, days = expected_matrix(seed, start, end, weather, outbreaks, sf)
    reg_by_d = {c: g.reset_index(drop=True) for c, g in registry.groupby("district_code")}
    # facility volume shares (the artefact facility is the dominant one in its district)
    shares = {}
    for c, g in reg_by_d.items():
        w = np.where(g.facility_type == "HOSPITAL", 6.0, np.where(g.facility_type == "UHC", 2.0, 1.0))
        shares[c] = w / w.sum()
    art_dist = DIST.loc[DIST.district_name == ARTEFACT["district"], "district_code"].iloc[0]
    art_fac_idx = 0
    if not drop:
        shares[art_dist] = shares[art_dist] * 0.5; shares[art_dist][art_fac_idx] = 0.5 + shares[art_dist][art_fac_idx]
        shares[art_dist] /= shares[art_dist].sum()
    art = {"facility_id": reg_by_d[art_dist].facility_id.iloc[art_fac_idx], **ARTEFACT} if not drop else None

    rngs = [np.random.default_rng([seed, 1000 + i]) for i in range(len(DIST))]
    fac_ids_all = registry.facility_id.values
    enc_counter = 0
    raw_dir = os.path.join(out, "s4_encounters")
    os.makedirs(raw_dir, exist_ok=True)
    gt_dup = []
    month_starts = pd.date_range(days[0].replace(day=1), days[-1], freq="MS")
    total = 0
    for ms in month_starts:
        me = min(ms + pd.offsets.MonthEnd(0), days[-1])
        sel = np.where((days >= max(ms, days[0])) & (days <= me))[0]
        frames = []
        for i, r in DIST.iterrows():
            rng = rngs[i]
            fr = reg_by_d[r.district_code]
            for g, dis in enumerate(DISEASES):
                mu = E[i, g, sel]
                size = SIZE[dis]
                cnt = rng.negative_binomial(size, size / (size + np.maximum(mu, 1e-9)))
                n = int(cnt.sum())
                if n == 0:
                    continue
                onset = np.repeat(days[sel].values, cnt)
                fidx = rng.choice(len(fr), n, p=shares[r.district_code])
                age = np.clip(rng.gamma(2.2, 14, n), 0, 95).astype(int)
                if dis in ("DENGUE", "MALARIA", "ILI"):
                    age = np.clip(rng.gamma(3.0, 10, n), 0, 90).astype(int)
                sex = rng.choice(["M", "F"], n, p=[0.5, 0.5])
                icd = rng.choice(ICD[dis], n)
                sev = {"DENGUE": 0.22, "DIARRHOEAL": 0.10, "ILI": 0.05, "MALARIA": 0.15, "OTHER": 0.03}[dis]
                # admission rises in outbreaks (approximation via local multiplier on severity)
                adm = rng.random(n) < sev
                lab = rng.random(n) < {"DENGUE": 0.45, "DIARRHOEAL": 0.12, "ILI": 0.08, "MALARIA": 0.9, "OTHER": 0.02}[dis]
                death = adm & (rng.random(n) < {"DENGUE": 0.008, "DIARRHOEAL": 0.01, "ILI": 0.02, "MALARIA": 0.01, "OTHER": 0.02}[dis])
                delay = np.minimum(21, np.floor(rng.gamma(1.4, 2.2, n))).astype(int)
                rep = onset + pd.to_timedelta(delay, unit="D").values
                pat = rng.integers(10**9, 10**10, n).astype(str)
                f = pd.DataFrame({"patient_id": pat, "facility_id": fr.facility_id.values[fidx],
                                  "onset_date": onset, "report_date": rep, "age_years": age, "sex": sex,
                                  "icd10_code": icd, "lab_confirmed": lab, "admitted": adm,
                                  "outcome": np.where(death, "DIED", "RECOVERED"), "_d": r.district_code, "_g": dis})
                frames.append(f)
        if not frames:
            continue
        df = pd.concat(frames, ignore_index=True)
        rng = np.random.default_rng([seed, 7, int(ms.strftime("%Y%m"))])
        # weekend / holiday dip: Fri(4)/Sat(5) reports mostly slip to Sunday
        wd = pd.DatetimeIndex(df.report_date).dayofweek
        slip = np.isin(wd, [4, 5]) & (rng.random(len(df)) < 0.6)
        df.loc[slip, "report_date"] = df.loc[slip, "report_date"] + pd.to_timedelta(np.where(wd[slip] == 4, 2, 1), unit="D")
        # reporting artefact: silent facility, back-filled in one submission
        if art is not None:
            m = (df.facility_id == art["facility_id"]) & (df.report_date >= art["start"]) & (df.report_date <= art["end"])
            df.loc[m, "report_date"] = pd.Timestamp(art["backfill"])
        # cross-facility duplicate patients (same patient, other facility, within 7 days)
        k = int(0.03 * len(df))
        if k:
            idx = rng.choice(len(df), k, replace=False)
            dup = df.iloc[idx].copy()
            others = registry.facility_id.values
            dup["facility_id"] = [rng.choice(reg_by_d[d].facility_id.values) for d in dup["_d"]]
            dup["onset_date"] = dup["onset_date"] + pd.to_timedelta(rng.integers(0, 5, k), unit="D")
            dup["report_date"] = np.maximum(dup["report_date"], dup["onset_date"]) + pd.to_timedelta(rng.integers(0, 3, k), unit="D")
            dup["_dup"] = True
            df["_dup"] = False
            df = pd.concat([df, dup], ignore_index=True)
        else:
            df["_dup"] = False
        n = len(df)
        df["encounter_id"] = [f"E{seed}-{enc_counter + j:010d}" for j in range(n)]
        enc_counter += n
        gt_dup.append(df.loc[df._dup, "encounter_id"])
        # injected defects
        bad_age = rng.random(n) < 0.01
        df.loc[bad_age, "age_years"] = rng.choice([212, -3, 150, 999], bad_age.sum())
        bad_sex = rng.random(n) < 0.002
        df["sex"] = df["sex"].astype(object)
        df.loc[bad_sex, "sex"] = "X9"
        orphan = rng.random(n) < 0.005
        df.loc[orphan, "facility_id"] = "F9" + pd.Series(rng.integers(100, 999, orphan.sum()).astype(str), index=df.index[orphan])
        imp = rng.random(n) < 0.003
        df.loc[imp, "onset_date"] = df.loc[imp, "report_date"] + pd.Timedelta(days=int(rng.integers(1, 6)))
        badicd = rng.random(n) < 0.003
        df["icd10_code"] = df["icd10_code"].astype(object)
        df.loc[badicd, "icd10_code"] = rng.choice(["A9", "ZZZ", "99X"], badicd.sum())
        df["load_date"] = df["report_date"]
        # ~2% resubmitted (identical payload, later load_date)
        resub = df.sample(frac=0.02, random_state=int(ms.strftime("%Y%m"))).copy()
        resub["load_date"] = resub["load_date"] + pd.to_timedelta(rng.integers(1, 6, len(resub)), unit="D")
        df = pd.concat([df, resub], ignore_index=True)
        df["onset_date"] = pd.to_datetime(df.onset_date).dt.strftime("%Y-%m-%d")
        df["report_date"] = pd.to_datetime(df.report_date).dt.strftime("%Y-%m-%d")
        df["load_date"] = pd.to_datetime(df.load_date).dt.strftime("%Y-%m-%d")
        cols = ["encounter_id", "patient_id", "facility_id", "onset_date", "report_date", "age_years", "sex",
                "icd10_code", "lab_confirmed", "admitted", "outcome", "load_date"]
        df = df[cols]
        total += len(df)
        for ld, g in df.groupby("load_date"):
            d = os.path.join(raw_dir, f"load_date={ld}")
            os.makedirs(d, exist_ok=True)
            p = os.path.join(d, "encounters.csv")
            g.drop(columns=["load_date"]).to_csv(p, mode="a", header=not os.path.exists(p), index=False)
    # side files
    reg_dir = os.path.join(out, "s5_registry"); os.makedirs(reg_dir, exist_ok=True)
    registry.to_csv(os.path.join(reg_dir, "facilities.csv"), index=False)
    w_dir = os.path.join(out, "s3_weather"); os.makedirs(w_dir, exist_ok=True)
    weather.assign(date=weather.date.dt.strftime("%Y-%m-%d")).to_csv(os.path.join(w_dir, "weather.csv"), index=False)
    # S6 stock / beds snapshots (JSON lines, daily per facility; sampled facilities at low SF)
    stk_dir = os.path.join(out, "s6_stock"); os.makedirs(stk_dir, exist_ok=True)
    srng = np.random.default_rng([seed, 6])
    for ld in pd.date_range(days[0], days[-1], freq="D"):
        fac = registry.sample(frac=1.0, random_state=int(ld.strftime("%Y%m%d")) % 100000)
        n = len(fac)
        fac_beds = fac.beds.values
        rec = pd.DataFrame({"snapshot_date": ld.strftime("%Y-%m-%d"), "facility_id": fac.facility_id.values,
                            "iv_saline": (fac_beds * srng.uniform(1, 4, n)).astype(int),
                            "ns1_kits": srng.integers(0, 200, n), "ors_sachets": srng.integers(0, 3000, n),
                            "antimalarials": srng.integers(0, 150, n),
                            "occupied_beds": (fac_beds * srng.uniform(0.3, 1.05, n)).astype(int)})
        d = os.path.join(stk_dir, f"snapshot_date={ld.strftime('%Y-%m-%d')}")
        os.makedirs(d, exist_ok=True)
        rec.drop(columns=["snapshot_date"]).to_json(os.path.join(d, "stock.json"), orient="records", lines=True)
    # ground truth (kept outside raw/, never read by analytics)
    ob = outbreaks.copy()
    ob["start"] = ob.start.dt.strftime("%Y-%m-%d"); ob["end"] = ob.end.dt.strftime("%Y-%m-%d")
    ob.to_csv(os.path.join(gt_dir, "ground_truth_outbreaks.csv"), index=False)
    if art:
        json.dump(art, open(os.path.join(gt_dir, "ground_truth_artefact.json"), "w"), indent=2)
    if gt_dup:
        pd.concat(gt_dup).to_frame("encounter_id").to_csv(os.path.join(gt_dir, "ground_truth_duplicates.csv"), index=False)
    man = dict(seed=seed, sf=sf, start=start, end=end, rows_written=int(total), drop=drop)
    json.dump(man, open(os.path.join(gt_dir, "manifest.json"), "w"), indent=2)
    print(f"generated {total:,} encounter rows ({start}..{end}), SF={sf}, seed={seed} -> {out}")
    return man


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=float, default=0.05)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--start", default="2021-01-01")
    ap.add_argument("--end", default="2025-12-31")
    ap.add_argument("--drop", action="store_true", help="14-day incremental drop after the base window")
    ap.add_argument("--weather-cache", default=None)
    a = ap.parse_args()
    if a.drop:
        a.start, a.end = "2026-01-01", "2026-01-14"
    generate(a.out, a.sf, a.seed, a.start, a.end, a.drop, a.weather_cache)
