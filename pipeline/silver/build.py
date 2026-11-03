"""SILVER: typed, conformed, quality-checked, de-identified (PRV-1). Identifiers end here: patient_id -> salted hash, age -> band.
Only bronze batches not yet processed are read (incremental, idempotent). Failing rows go to quarantine with reason codes."""
import os
import pyspark.sql.functions as F
from pyspark.sql import Window
from pipeline.common import lake, get_salt, read_manifest, append_manifest
from pipeline.quality.rules import ROW_RULES, ALL_RULE_IDS


def week_start(c):
    """Monday of the ISO week containing date column c."""
    return F.date_sub(c, (F.dayofweek(c) + 5) % 7)


def disease_group(icd):
    return (F.when(icd.rlike("^A9[01]$"), "DENGUE")
            .when(icd.rlike("^A0[0-9]$"), "DIARRHOEAL")
            .when(icd.rlike("^J1[01]$"), "ILI")
            .when(icd.rlike("^B5[0-4]$"), "MALARIA")
            .otherwise("OTHER"))


def build_reference(spark):
    reg = spark.read.parquet(lake("bronze_registry"))
    w = Window.partitionBy("facility_id").orderBy(F.col("_load_ts").desc())
    fac = (reg.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")
           .select("facility_id", "facility_name", "district_code", "facility_type",
                   F.col("lat").cast("double"), F.col("lon").cast("double"),
                   F.col("beds").cast("int"), F.col("icu_beds").cast("int")))
    fac.write.mode("overwrite").parquet(lake("silver_facilities"))
    wx = spark.read.parquet(lake("bronze_weather"))
    w2 = Window.partitionBy("district_code", "date").orderBy(F.col("_load_ts").desc())
    wx = (wx.withColumn("_rn", F.row_number().over(w2)).filter("_rn=1")
          .select("district_code", F.to_date("date").alias("date"), F.col("precip_mm").cast("double"),
                  F.col("tmean_c").cast("double"), F.col("humidity").cast("double")))
    wx.write.mode("overwrite").parquet(lake("silver_weather"))
    st = spark.read.parquet(lake("bronze_stock"))
    w3 = Window.partitionBy("facility_id", "snapshot_date").orderBy(F.col("_load_ts").desc())
    st = (st.withColumn("_rn", F.row_number().over(w3)).filter("_rn=1")
          .select("facility_id", F.to_date("snapshot_date").alias("snapshot_date"),
                  *[F.col(c).cast("int") for c in ["iv_saline", "ns1_kits", "ors_sachets", "antimalarials", "occupied_beds"]]))
    st.write.mode("overwrite").parquet(lake("silver_stock"))


def build_encounters(spark):
    done = {m["batch_id"] for m in read_manifest("silver.jsonl")}
    bronze = spark.read.parquet(lake("bronze_encounters"))
    all_batches = {r[0] for r in bronze.select("_batch_id").distinct().collect()}
    todo = sorted(all_batches - done)
    if not todo:
        print("[silver] no new bronze batches")
        return {"rows_in": 0}
    b = bronze.filter(F.col("_batch_id").isin(todo))
    fac = spark.read.parquet(lake("silver_facilities")).select(
        "facility_id", F.col("district_code").alias("fac_district"), F.col("beds").alias("fac_beds"))
    refd = spark.read.parquet(lake("ref_districts")).select(
        F.col("district_code").alias("ref_district"), "division", F.col("population").cast("long").alias("population"))
    df = (b.select("encounter_id", "patient_id", "facility_id", "load_date", "outcome", "sex", "icd10_code",
                   F.col("onset_date").alias("onset_date_raw"), F.col("report_date").alias("report_date_raw"),
                   F.col("age_years").alias("age_raw"),
                   F.to_date("onset_date").alias("onset_date"), F.to_date("report_date").alias("report_date"),
                   F.col("age_years").cast("int").alias("age_years"),
                   F.col("lab_confirmed").cast("boolean").alias("lab_confirmed"),
                   F.col("admitted").cast("boolean").alias("admitted"),
                   "_batch_id", "_load_ts")
          .join(F.broadcast(fac), "facility_id", "left")                      # ENG-3: broadcast join of the registry
          .join(F.broadcast(refd), F.col("fac_district") == F.col("ref_district"), "left")
          .withColumn("disease_group", disease_group(F.col("icd10_code"))))
    # ---- Q11: remove resubmissions (within this run and against what silver already holds) ----
    w = Window.partitionBy("encounter_id").orderBy("_load_ts", "load_date")
    df = df.withColumn("_rn", F.row_number().over(w))
    n_resub_inbatch = df.filter("_rn > 1").count()
    df = df.filter("_rn = 1").drop("_rn")
    try:
        existing = spark.read.parquet(lake("silver_encounters")).select("encounter_id")
        q_exist = spark.read.parquet(lake("quarantine_encounters")).select("encounter_id")
        known = existing.union(q_exist).distinct()
        n_before = df.count()
        df = df.join(known, "encounter_id", "left_anti")
        n_resub_hist = n_before - df.count()
    except Exception:
        n_resub_hist = 0
    # ---- row-level rules ----
    for rid, sev, desc, cond in ROW_RULES:
        df = df.withColumn("fail_" + rid, F.coalesce(F.expr(cond), F.lit(False)))
    qcols = [r[0] for r in ROW_RULES if r[1] == "quarantine"]
    df = df.withColumn("reason_codes", F.filter(F.array(*[F.when(F.col("fail_" + q), F.lit(q)) for q in qcols]), lambda x: x.isNotNull()))
    df = df.withColumn("is_quarantined", F.size("reason_codes") > 0).cache()
    # ---- DQ report numbers (DQ-2) ----
    agg = [F.count("*").alias("rows_in"), F.sum(F.col("is_quarantined").cast("int")).alias("rows_quarantined")]
    agg += [F.sum(F.col("fail_" + r[0]).cast("int")).alias("fail_" + r[0]) for r in ROW_RULES]
    dq = df.groupBy("_batch_id", "load_date").agg(*agg)
    now = df.agg(F.max("_load_ts")).collect()[0][0]
    # stats must be taken BEFORE writing silver: the write invalidates the cache and the anti-join would then see the new rows
    stats = df.agg(F.count("*").alias("rows"), F.sum(F.col("is_quarantined").cast("int")).alias("q")).collect()[0]
    dq = dq.withColumn("resubmissions_removed_run", F.lit(n_resub_inbatch + n_resub_hist)).withColumn("dq_run_ts", F.lit(now))
    dq.write.mode("append").parquet(lake("gold_dq_report"))
    # ---- quarantine ----
    q = df.filter("is_quarantined").select(
        "encounter_id", "facility_id", "onset_date_raw", "report_date_raw", "age_raw", "sex", "icd10_code", "load_date",
        F.concat_ws(",", "reason_codes").alias("reason_codes"), "_batch_id", "_load_ts")
    q.write.mode("append").partitionBy("load_date").parquet(lake("quarantine_encounters"))
    # ---- good rows -> de-identified silver ----
    salt = get_salt()
    good = df.filter(~F.col("is_quarantined"))
    good = (good.withColumn("patient_hash", F.sha2(F.concat_ws("|", F.lit(salt), F.col("patient_id")), 256).substr(1, 32))
            .withColumn("age_band", F.when(F.col("age_years") >= 85, "85+")
                        .otherwise(F.concat((F.floor(F.col("age_years") / 5) * 5).cast("string"), F.lit("-"),
                                            (F.floor(F.col("age_years") / 5) * 5 + 4).cast("string"))))
            .withColumn("onset_week", week_start("onset_date")).withColumn("report_week", week_start("report_date"))
            .withColumn("epi_year", F.year(F.date_add("onset_week", 3)))
            .withColumn("epi_week", F.weekofyear("onset_week"))
            .withColumn("report_delay_days", F.datediff("report_date", "onset_date"))
            .withColumn("late_extreme", F.col("fail_Q7"))
            .select("encounter_id", "patient_hash", "facility_id", F.col("fac_district").alias("district_code"), "division",
                    "onset_date", "report_date", "onset_week", "report_week", "epi_year", "epi_week", "age_band", "sex",
                    "icd10_code", "disease_group", "lab_confirmed", "admitted", "outcome", "report_delay_days",
                    "late_extreme", "fail_Q13", "fail_Q14", "_batch_id", "_load_ts"))
    # patient_id is NOT selected above: it never leaves bronze (PRV-1)
    good.write.mode("append").partitionBy("epi_year", "division").parquet(lake("silver_encounters"))
    append_manifest("silver.jsonl", [{"batch_id": t} for t in todo])
    print(f"[silver] batches={len(todo)} rows(after resubmission removal)={stats['rows']} quarantined={stats['q']} "
          f"resubmissions_removed={n_resub_inbatch + n_resub_hist}")
    df.unpersist()
    return {"rows_in": stats["rows"], "quarantined": stats["q"], "resub": n_resub_inbatch + n_resub_hist}


def run(spark):
    build_reference(spark)
    return build_encounters(spark)
