"""AN-1 / AN-3: outbreak detection at scale in Spark. This module must NEVER read ground-truth files (enforced by a unit test).
 - EARS C2 : Spark window functions, baseline = weeks t-9..t-3 (2-week guard band), z = (x - mean) / max(sd, sd_floor)
 - CUSUM   : grouped applyInPandas, S_t = max(0, S_{t-1} + z_t - k)
 - variants: 'naive' (all reports, no gate)  vs  'gated' (timely reports + artefact gate)  -> AN-3 artefact comparison
"""
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql import types as T
from pipeline.common import lake

DISEASES = ["DENGUE", "DIARRHOEAL", "ILI", "MALARIA"]
SD_FLOOR = 1.0
MIN_CASES = 4          # ignore weeks with fewer cases than this (tiny counts are noise, not outbreaks)


def c2_scores(surv, value_col, gate):
    """Return surv + baseline mean/sd + C2 score. If gate=True, artefact-suspect weeks cannot alert and are excluded from baselines."""
    w = Window.partitionBy("district_code", "disease_group").orderBy("week_start").rowsBetween(-9, -3)
    x = F.col(value_col).cast("double")
    if gate:
        x_base = F.when(F.col("artefact_suspect"), F.lit(None).cast("double")).otherwise(x)
    else:
        x_base = x
    df = surv.withColumn("_xb", x_base)
    df = (df.withColumn("base_mean", F.avg("_xb").over(w)).withColumn("base_sd", F.stddev("_xb").over(w))
          .withColumn("base_n", F.count("_xb").over(w)))
    sd = F.greatest(F.coalesce("base_sd", F.lit(0.0)), F.lit(SD_FLOOR))
    df = df.withColumn("value", x).withColumn("c2", (x - F.col("base_mean")) / sd)
    df = df.withColumn("c2", F.when(F.col("base_n") >= 5, F.col("c2")))
    if gate:
        df = df.withColumn("c2", F.when(F.col("artefact_suspect"), F.lit(None)).otherwise(F.col("c2")))
    return df.drop("_xb")


def _cusum_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("week_start")
    k, s, out = 0.5, 0.0, []
    for z in pdf["c2"].values:
        if z is None or pd.isna(z):
            out.append(0.0 if s == 0 else s)   # hold state through gated/insufficient weeks
            continue
        s = max(0.0, s + float(z) - k)
        out.append(s)
    pdf["cusum"] = out
    return pdf


CUSUM_SCHEMA = None


def add_cusum(df):
    schema = T.StructType(df.schema.fields + [T.StructField("cusum", T.DoubleType())])
    return df.groupBy("district_code", "disease_group").applyInPandas(_cusum_pdf, schema=schema)


def run_detection(spark, variant="gated"):
    surv = spark.read.parquet(lake("gold_surveillance")).filter(F.col("disease_group").isin(DISEASES))
    if variant == "gated":
        scored = c2_scores(surv, "cases_timely", gate=True)
    else:
        scored = c2_scores(surv.withColumn("artefact_suspect", F.lit(False)), "cases_reported", gate=False)
    scored = scored.select("district_code", "district_name", "division", "disease_group", "week_start", "value",
                           "base_mean", "base_sd", "c2", "artefact_suspect", "completeness", "late_share", "population")
    scored = add_cusum(scored)
    return scored


def alerts_from(scored_pdf, tau, method="c2", h=None):
    d = scored_pdf.copy()
    if method == "c2":
        d["alert"] = (d["c2"] > tau) & (d["value"] >= MIN_CASES)
    else:
        d["alert"] = (d["cusum"] > (h if h is not None else tau)) & (d["value"] >= MIN_CASES)
    return d[d["alert"]]


def main(spark):
    for v in ("gated", "naive"):
        sc = run_detection(spark, v)
        sc.write.mode("overwrite").parquet(lake(f"gold_detection_{v}"))
        print(f"[detect] wrote gold_detection_{v}")
