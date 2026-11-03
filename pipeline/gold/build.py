"""GOLD: epi-week x district x disease marts + weather lag features + lineage (src_batch_ids).
Gold is fully recomputed from silver each run (aggregation is cheap relative to ingest); because silver already contains the
late-arriving records, every affected epi-week is automatically updated (ING-4). The set of epi-weeks touched is reported."""
import pyspark.sql.functions as F
from pyspark.sql import Window
from pipeline.common import lake
from pipeline.silver.build import week_start

DISEASES = ["DENGUE", "DIARRHOEAL", "ILI", "MALARIA"]
MIN_EXPECTED = 2.0      # a facility needs >= 2 expected reports/week to count as "silent" when it reports 0
LATE_DAYS = 7           # "timely" = reported within 7 days of onset


def patient_dedup(spark):
    """Q12 / DQ-4: flag later records of the same (patient hash, disease) at a different facility within 7 days."""
    s = spark.read.parquet(lake("silver_encounters")).select("encounter_id", "patient_hash", "disease_group", "facility_id",
                                                              "onset_date", "sex", "age_band")
    w = Window.partitionBy("patient_hash", "disease_group").orderBy("onset_date", "encounter_id")
    x = (s.withColumn("p_onset", F.lag("onset_date").over(w)).withColumn("p_fac", F.lag("facility_id").over(w))
         .withColumn("p_sex", F.lag("sex").over(w)).withColumn("p_age", F.lag("age_band").over(w)))
    dups = x.filter((F.datediff("onset_date", "p_onset") <= 7) & (F.col("facility_id") != F.col("p_fac")) &
                    (F.col("sex") == F.col("p_sex")) & (F.col("age_band") == F.col("p_age"))
                    ).select("encounter_id", "patient_hash", "disease_group", "facility_id", F.col("p_fac").alias("merged_into_facility"))
    dups.write.mode("overwrite").parquet(lake("silver_patient_dedup_audit"))
    return spark.read.parquet(lake("silver_patient_dedup_audit"))


def weekly_weather(spark):
    wx = spark.read.parquet(lake("silver_weather")).withColumn("week_start", week_start("date"))
    wk = wx.groupBy("district_code", "week_start").agg(F.sum("precip_mm").alias("precip_mm"), F.avg("tmean_c").alias("tmean_c"),
                                                       F.avg("humidity").alias("humidity"))
    w = Window.partitionBy("district_code").orderBy("week_start")
    for k in range(1, 9):
        wk = wk.withColumn(f"precip_lag{k}", F.lag("precip_mm", k).over(w)).withColumn(f"tmean_lag{k}", F.lag("tmean_c", k).over(w))
    return wk


def facility_completeness(spark, s):
    """Volume-weighted completeness per district-week: 1 - (expected volume of silent facilities / total expected volume)."""
    fac = spark.read.parquet(lake("silver_facilities")).select("facility_id", "district_code")
    bounds = s.agg(F.min("report_week").alias("a"), F.max("report_week").alias("b")).collect()[0]
    weeks = spark.sql(f"SELECT explode(sequence(DATE'{bounds['a']}', DATE'{bounds['b']}', interval 7 day)) AS report_week")
    vol = s.groupBy("facility_id", "report_week").agg(F.count("*").alias("vol"))
    grid = fac.crossJoin(weeks)
    g = grid.join(vol, ["facility_id", "report_week"], "left").withColumn("vol", F.coalesce("vol", F.lit(0)))
    w = Window.partitionBy("facility_id").orderBy("report_week").rowsBetween(-8, -1)
    g = (g.withColumn("expected", F.avg("vol").over(w)).withColumn("n_prior", F.count("vol").over(w))
         .withColumn("silent", (F.col("vol") == 0) & (F.col("expected") >= MIN_EXPECTED) & (F.col("n_prior") >= 4)))
    g.write.mode("overwrite").parquet(lake("gold_facility_week"))
    comp = (g.groupBy("district_code", "report_week")
            .agg((1 - F.sum(F.when(F.col("silent"), F.col("expected")).otherwise(0.0)) / F.greatest(F.sum("expected"), F.lit(1e-9))).alias("completeness"),
                 F.sum(F.col("silent").cast("int")).alias("silent_facilities")))
    return comp.withColumn("completeness", F.coalesce("completeness", F.lit(1.0)))


def build(spark):
    ref = spark.read.parquet(lake("ref_districts")).select("district_code", "district_name", "division",
                                                           F.col("population").cast("long").alias("population"))
    dups = patient_dedup(spark)
    s_all = spark.read.parquet(lake("silver_encounters"))
    n_late = s_all.filter("late_extreme").count()
    n_dups = dups.count()
    s = (s_all.filter(~F.col("late_extreme")).join(dups.select("encounter_id"), "encounter_id", "left_anti"))
    s = s.drop("division")
    s.cache()
    comp = facility_completeness(spark, s)
    # ---------- weather lags ----------
    wk = weekly_weather(spark)
    # ---------- onset-week mart (revised as late records arrive) ----------
    bounds = s.agg(F.min("onset_week").alias("a"), F.max("onset_week").alias("b")).collect()[0]
    weeks = spark.sql(f"SELECT explode(sequence(DATE'{bounds['a']}', DATE'{bounds['b']}', interval 7 day)) AS week_start")
    grid = ref.crossJoin(F.broadcast(spark.createDataFrame([(d,) for d in DISEASES + ["OTHER"]], ["disease_group"]))).crossJoin(weeks)
    agg = (s.groupBy("district_code", "disease_group", F.col("onset_week").alias("week_start"))
           .agg(F.count("*").alias("cases"), F.sum(F.col("lab_confirmed").cast("int")).alias("cases_lab_confirmed"),
                F.sum(F.col("admitted").cast("int")).alias("admissions"),
                F.sum((F.col("outcome") == "DIED").cast("int")).alias("deaths"),
                F.collect_set("_batch_id").alias("src_batch_ids")))
    onset = grid.join(agg, ["district_code", "disease_group", "week_start"], "left")
    for c in ["cases", "cases_lab_confirmed", "admissions", "deaths"]:
        onset = onset.withColumn(c, F.coalesce(c, F.lit(0)))
    onset = (onset.withColumn("src_batch_ids", F.coalesce("src_batch_ids", F.array().cast("array<string>")))
             .join(wk, ["district_code", "week_start"], "left")
             .withColumn("incidence_per_100k", F.round(F.col("cases") / F.col("population") * 1e5, 3))
             .withColumn("epi_year", F.year(F.date_add("week_start", 3))).withColumn("epi_week", F.weekofyear("week_start"))
             .withColumn("gold_built_at", F.current_timestamp()))
    onset.write.mode("overwrite").partitionBy("epi_year", "division").parquet(lake("gold_epiweek_onset"))
    # ---------- report-week surveillance mart (what was KNOWN that week; used by detection + forecasting) ----------
    rb = s.agg(F.min("report_week").alias("a"), F.max("report_week").alias("b")).collect()[0]
    rweeks = spark.sql(f"SELECT explode(sequence(DATE'{rb['a']}', DATE'{rb['b']}', interval 7 day)) AS week_start")
    rgrid = ref.crossJoin(F.broadcast(spark.createDataFrame([(d,) for d in DISEASES + ["OTHER"]], ["disease_group"]))).crossJoin(rweeks)
    ragg = (s.groupBy("district_code", "disease_group", F.col("report_week").alias("week_start"))
            .agg(F.count("*").alias("cases_reported"),
                 F.sum((F.col("report_delay_days") <= LATE_DAYS).cast("int")).alias("cases_timely"),
                 F.sum(F.col("admitted").cast("int")).alias("admissions_reported"),
                 F.sum((F.col("outcome") == "DIED").cast("int")).alias("deaths_reported"),
                 F.collect_set("_batch_id").alias("src_batch_ids")))
    dw = (s.groupBy("district_code", F.col("report_week").alias("week_start"))
          .agg((F.sum((F.col("report_delay_days") > LATE_DAYS).cast("int")) / F.count("*")).alias("late_share"),
               F.count("*").alias("all_reports")))
    surv = rgrid.join(ragg, ["district_code", "disease_group", "week_start"], "left")
    for c in ["cases_reported", "cases_timely", "admissions_reported", "deaths_reported"]:
        surv = surv.withColumn(c, F.coalesce(c, F.lit(0)))
    surv = (surv.withColumn("src_batch_ids", F.coalesce("src_batch_ids", F.array().cast("array<string>")))
            .join(dw, ["district_code", "week_start"], "left")
            .join(comp.withColumnRenamed("report_week", "week_start"), ["district_code", "week_start"], "left")
            .withColumn("late_share", F.coalesce("late_share", F.lit(0.0)))
            .withColumn("completeness", F.coalesce("completeness", F.lit(1.0)))
            .withColumn("artefact_suspect", (F.col("completeness") < 0.6) | (F.col("late_share") > 0.35))
            .join(wk, ["district_code", "week_start"], "left")
            .withColumn("epi_year", F.year(F.date_add("week_start", 3))).withColumn("epi_week", F.weekofyear("week_start"))
            .withColumn("incidence_per_100k", F.round(F.col("cases_reported") / F.col("population") * 1e5, 3))
            .withColumn("gold_built_at", F.current_timestamp()))
    surv.write.mode("overwrite").partitionBy("epi_year", "division").parquet(lake("gold_surveillance"))
    # ---------- district stock snapshot (latest per facility) ----------
    st = spark.read.parquet(lake("silver_stock"))
    w = Window.partitionBy("facility_id").orderBy(F.col("snapshot_date").desc())
    last = st.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")
    fac = spark.read.parquet(lake("silver_facilities")).select("facility_id", "district_code", "beds")
    ds = (last.join(fac, "facility_id").groupBy("district_code")
          .agg(F.sum("iv_saline").alias("iv_saline"), F.sum("ns1_kits").alias("ns1_kits"), F.sum("ors_sachets").alias("ors_sachets"),
               F.sum("antimalarials").alias("antimalarials"), F.sum("occupied_beds").alias("occupied_beds"), F.sum("beds").alias("beds"),
               F.max("snapshot_date").alias("snapshot_date")))
    ds.write.mode("overwrite").parquet(lake("gold_stock_district"))
    # ---------- DQ run summary (DQ-2) ----------
    n_silver = s_all.count()
    low = comp.filter("completeness < 0.6").count()
    summ = spark.createDataFrame([(int(n_silver), int(n_late), int(n_dups), int(low))],
                                 ["silver_rows", "late_extreme_excluded", "patient_dups_merged", "district_weeks_low_completeness"]
                                 ).withColumn("run_ts", F.current_timestamp())
    summ.write.mode("append").parquet(lake("gold_dq_run"))
    gw = onset.agg(F.count("*")).collect()[0][0]
    print(f"[gold] onset rows={gw:,} silver_rows={n_silver:,} patient_dups_merged={n_dups:,} late_extreme_excluded={n_late:,} "
          f"low_completeness_district_weeks={low}")
    s.unpersist()


def run(spark):
    build(spark)
