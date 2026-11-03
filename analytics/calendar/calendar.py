"""AN-9: seasonal preparedness calendar from STL decomposition (per division x disease group), next 52 weeks.
Seasonal component -> typical onset week (first week above annual mean), peak week, duration; recommendations are offset by lead times."""
import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL

LEAD = {"DENGUE": {"prepositioning_weeks": 4, "campaign_weeks": 6, "what": "NS1 kits, IV saline; larval-control campaign"},
        "DIARRHOEAL": {"prepositioning_weeks": 2, "campaign_weeks": 3, "what": "ORS, IV saline; WASH campaign"},
        "ILI": {"prepositioning_weeks": 3, "campaign_weeks": 4, "what": "beds, antivirals; vaccination/awareness"},
        "MALARIA": {"prepositioning_weeks": 4, "campaign_weeks": 6, "what": "antimalarials, bed nets"}}


def build_calendar(onset: pd.DataFrame, pop_by_div: dict):
    """onset: columns division, disease_group, week_start, cases (district rows, onset-week mart)."""
    out, curves = [], {}
    d = onset.groupby(["division", "disease_group", "week_start"], as_index=False)["cases"].sum()
    for (div, dis), g in d.groupby(["division", "disease_group"]):
        if dis == "OTHER":
            continue
        g = g.sort_values("week_start")
        s = pd.Series(g["cases"].values / pop_by_div[div] * 1e5, index=pd.to_datetime(g["week_start"]))
        s = s.iloc[:-3]                                  # last weeks are incomplete (reporting delay)
        if len(s) < 110:
            continue
        res = STL(s, period=52, robust=True).fit()
        seas = res.seasonal.iloc[-52:]
        woy = seas.index.isocalendar().week.values
        base = np.array(seas.values) + res.trend.iloc[-52:].mean()
        thr = base.mean()
        above = base > thr
        peak_i = int(np.argmax(base))
        # onset = first week of the longest above-mean run
        best, cur, start = (0, 0), 0, 0
        for i, a in enumerate(np.concatenate([above, above])):
            if a:
                if cur == 0: start = i
                cur += 1
                if cur > best[0]: best = (cur, start)
            else:
                cur = 0
        onset_i = best[1] % 52
        L = LEAD[dis]
        woy_arr = list(woy)
        out.append({"division": div, "disease_group": dis, "typical_onset_week": int(woy_arr[onset_i]),
                    "peak_week": int(woy_arr[peak_i]), "weeks_above_mean": int(best[0] if best[0] <= 52 else 52),
                    "preposition_from_week": int(((woy_arr[onset_i] - L["prepositioning_weeks"] - 1) % 52) + 1),
                    "campaign_from_week": int(((woy_arr[onset_i] - L["campaign_weeks"] - 1) % 52) + 1),
                    "action": L["what"], "seasonal_amplitude": round(float((base.max() - base.min()) / max(base.mean(), 1e-9)), 2)})
        curves[f"{div}|{dis}"] = {"woy": [int(w) for w in woy_arr], "seasonal_index": [round(float(v / thr), 3) for v in base]}
    return pd.DataFrame(out), curves
