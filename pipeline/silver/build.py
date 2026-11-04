
"""SILVER: typed, conformed, quality-checked, de-identified (PRV-1).
Identifiers end here: patient_id -> salted hash, age -> band.
Only bronze batches not yet processed are read.
Failing rows go to quarantine with reason codes.
"""

import os

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark import StorageLevel

from pipeline.common import (
    lake,
    get_salt,
    read_manifest,
    append_manifest,
)
from pipeline.quality.rules import ROW_RULES


def week_start(c):
    """Monday of the ISO week containing date column c."""
    return F.date_sub(c, (F.dayofweek(c) + 5) % 7)


def disease_group(icd):
    return (
        F.when(icd.rlike("^A9[01]$"), "DENGUE")
        .when(icd.rlike("^A0[0-9]$"), "DIARRHOEAL")
        .when(icd.rlike("^J1[01]$"), "ILI")
        .when(icd.rlike("^B5[0-4]$"), "MALARIA")
        .otherwise("OTHER")
    )


def build_reference(spark):
    """Build conformed facility, weather and stock reference tables."""

    # Facilities
    reg = spark.read.parquet(lake("bronze_registry"))

    w = Window.partitionBy("facility_id").orderBy(
        F.col("_load_ts").desc()
    )

    fac = (
        reg.withColumn("_rn", F.row_number().over(w))
        .filter("_rn=1")
        .drop("_rn")
        .select(
            "facility_id",
            "facility_name",
            "district_code",
            "facility_type",
            F.col("lat").cast("double"),
            F.col("lon").cast("double"),
            F.col("beds").cast("int"),
            F.col("icu_beds").cast("int"),
        )
    )

    fac.write.mode("overwrite").parquet(
        lake("silver_facilities")
    )

    # Weather
    wx = spark.read.parquet(lake("bronze_weather"))

    w2 = Window.partitionBy(
        "district_code", "date"
    ).orderBy(F.col("_load_ts").desc())

    wx = (
        wx.withColumn("_rn", F.row_number().over(w2))
        .filter("_rn=1")
        .select(
            "district_code",
            F.to_date("date").alias("date"),
            F.col("precip_mm").cast("double"),
            F.col("tmean_c").cast("double"),
            F.col("humidity").cast("double"),
        )
    )

    wx.write.mode("overwrite").parquet(
        lake("silver_weather")
    )

    # Stock
    st = spark.read.parquet(lake("bronze_stock"))

    w3 = Window.partitionBy(
        "facility_id", "snapshot_date"
    ).orderBy(F.col("_load_ts").desc())

    st = (
        st.withColumn("_rn", F.row_number().over(w3))
        .filter("_rn=1")
        .select(
            "facility_id",
            F.to_date("snapshot_date").alias("snapshot_date"),
            *[
                F.col(c).cast("int")
                for c in [
                    "iv_saline",
                    "ns1_kits",
                    "ors_sachets",
                    "antimalarials",
                    "occupied_beds",
                ]
            ],
        )
    )

    st.write.mode("overwrite").parquet(
        lake("silver_stock")
    )

    print("[silver] reference tables built successfully")


def build_encounters(spark):

    # Identify batches that have not been processed
    done = {
        m["batch_id"]
        for m in read_manifest("silver.jsonl")
    }

    bronze = spark.read.parquet(
        lake("bronze_encounters")
    )

    all_batches = {
        r[0]
        for r in bronze.select("_batch_id").distinct().collect()
    }

    todo = sorted(all_batches - done)

    if not todo:
        print("[silver] no new bronze batches")
        return {"rows_in": 0}

    print(f"[silver] processing {len(todo)} new batches")

    b = bronze.filter(
        F.col("_batch_id").isin(todo)
    )

    # Reference tables
    fac = spark.read.parquet(
        lake("silver_facilities")
    ).select(
        "facility_id",
        F.col("district_code").alias("fac_district"),
        F.col("beds").alias("fac_beds"),
    )

    refd = spark.read.parquet(
        lake("ref_districts")
    ).select(
        F.col("district_code").alias("ref_district"),
        "division",
        F.col("population").cast("long").alias("population"),
    )

    # Join and conform data
    df = (
        b.select(
            "encounter_id",
            "patient_id",
            "facility_id",
            "load_date",
            "outcome",
            "sex",
            "icd10_code",
            F.col("onset_date").alias("onset_date_raw"),
            F.col("report_date").alias("report_date_raw"),
            F.col("age_years").alias("age_raw"),
            F.to_date("onset_date").alias("onset_date"),
            F.to_date("report_date").alias("report_date"),
            F.col("age_years").cast("int").alias("age_years"),
            F.col("lab_confirmed").cast("boolean").alias("lab_confirmed"),
            F.col("admitted").cast("boolean").alias("admitted"),
            "_batch_id",
            "_load_ts",
        )
        .join(
            F.broadcast(fac),
            "facility_id",
            "left",
        )
        .join(
            F.broadcast(refd),
            F.col("fac_district") == F.col("ref_district"),
            "left",
        )
        .withColumn(
            "disease_group",
            disease_group(F.col("icd10_code")),
        )
    )

    # Remove resubmissions within the current run
    w = Window.partitionBy(
        "encounter_id"
    ).orderBy(
        "_load_ts",
        "load_date",
    )

    df = df.withColumn(
        "_rn",
        F.row_number().over(w),
    )

    n_resub_inbatch = (
        df.filter("_rn > 1").count()
    )

    df = (
        df.filter("_rn = 1")
        .drop("_rn")
    )

    # Remove records already present in Silver or quarantine
    existing_path = lake("silver_encounters")
    quarantine_path = lake("quarantine_encounters")

    if os.path.exists(existing_path):
        existing = spark.read.parquet(
            existing_path
        ).select("encounter_id")

        if os.path.exists(quarantine_path):
            q_exist = spark.read.parquet(
                quarantine_path
            ).select("encounter_id")

            known = existing.unionByName(
                q_exist
            ).distinct()
        else:
            known = existing.distinct()

        n_before = df.count()

        df = df.join(
            known,
            "encounter_id",
            "left_anti",
        )

        n_resub_hist = n_before - df.count()

    elif os.path.exists(quarantine_path):

        q_exist = spark.read.parquet(
            quarantine_path
        ).select("encounter_id").distinct()

        n_before = df.count()

        df = df.join(
            q_exist,
            "encounter_id",
            "left_anti",
        )

        n_resub_hist = n_before - df.count()

    else:
        n_resub_hist = 0

    # Apply row-level quality rules
    for rid, severity, description, condition in ROW_RULES:
        df = df.withColumn(
            "fail_" + rid,
            F.coalesce(
                F.expr(condition),
                F.lit(False),
            ),
        )

    qcols = [
        rule[0]
        for rule in ROW_RULES
        if rule[1] == "quarantine"
    ]

    df = df.withColumn(
        "reason_codes",
        F.filter(
            F.array(
                *[
                    F.when(
                        F.col("fail_" + q),
                        F.lit(q),
                    )
                    for q in qcols
                ]
            ),
            lambda x: x.isNotNull(),
        ),
    )

    # Use disk-based persistence instead of memory cache
    df = df.withColumn(
        "is_quarantined",
        F.size("reason_codes") > 0,
    ).persist(StorageLevel.DISK_ONLY)

    try:

        # DQ report aggregation
        dq = df.groupBy(
            "_batch_id",
            "load_date",
        ).agg(
            F.count("*").alias("rows_in"),
            F.sum(
                F.col("is_quarantined").cast("int")
            ).alias("rows_quarantined"),
            *[
                F.sum(
                    F.col("fail_" + rule[0]).cast("int")
                ).alias("fail_" + rule[0])
                for rule in ROW_RULES
            ],
        )

        now = df.agg(
            F.max("_load_ts")
        ).collect()[0][0]

        stats = df.agg(
            F.count("*").alias("rows"),
            F.sum(
                F.col("is_quarantined").cast("int")
            ).alias("q"),
        ).collect()[0]

        total_resub = (
            n_resub_inbatch + n_resub_hist
        )

        dq = (
            dq.withColumn(
                "resubmissions_removed_run",
                F.lit(total_resub),
            )
            .withColumn(
                "dq_run_ts",
                F.lit(now),
            )
        )

        # Prepare quarantine rows
        q = df.filter(
            "is_quarantined"
        ).select(
            "encounter_id",
            "facility_id",
            "onset_date_raw",
            "report_date_raw",
            "age_raw",
            "sex",
            "icd10_code",
            "load_date",
            F.concat_ws(
                ",",
                "reason_codes",
            ).alias("reason_codes"),
            "_batch_id",
            "_load_ts",
        )

        # Prepare de-identified Silver rows
        salt = get_salt()

        good = df.filter(
            ~F.col("is_quarantined")
        )

        good = (
            good.withColumn(
                "patient_hash",
                F.sha2(
                    F.concat_ws(
                        "|",
                        F.lit(salt),
                        F.col("patient_id"),
                    ),
                    256,
                ).substr(1, 32),
            )
            .withColumn(
                "age_band",
                F.when(
                    F.col("age_years") >= 85,
                    "85+",
                ).otherwise(
                    F.concat(
                        (
                            F.floor(
                                F.col("age_years") / 5
                            ) * 5
                        ).cast("string"),
                        F.lit("-"),
                        (
                            F.floor(
                                F.col("age_years") / 5
                            ) * 5 + 4
                        ).cast("string"),
                    )
                ),
            )
            .withColumn(
                "onset_week",
                week_start("onset_date"),
            )
            .withColumn(
                "report_week",
                week_start("report_date"),
            )
            .withColumn(
                "epi_year",
                F.year(
                    F.date_add("onset_week", 3)
                ),
            )
            .withColumn(
                "epi_week",
                F.weekofyear("onset_week"),
            )
            .withColumn(
                "report_delay_days",
                F.datediff(
                    "report_date",
                    "onset_date",
                ),
            )
            .withColumn(
                "late_extreme",
                F.col("fail_Q7"),
            )
            .select(
                "encounter_id",
                "patient_hash",
                "facility_id",
                F.col("fac_district").alias("district_code"),
                "division",
                "onset_date",
                "report_date",
                "onset_week",
                "report_week",
                "epi_year",
                "epi_week",
                "age_band",
                "sex",
                "icd10_code",
                "disease_group",
                "lab_confirmed",
                "admitted",
                "outcome",
                "report_delay_days",
                "late_extreme",
                "fail_Q13",
                "fail_Q14",
                "_batch_id",
                "_load_ts",
            )
        )

        # patient_id is NOT selected above.
        # It never leaves Bronze (PRV-1).

        # IMPORTANT: Write Silver first.
        # If this large write fails, DQ and quarantine remain untouched.
        print("[silver] writing de-identified encounters...")

        good.write.mode(
            "append"
        ).partitionBy(
            "epi_year",
            "division",
        ).parquet(
            lake("silver_encounters")
        )

        print("[silver] encounters write completed")

        # Write quarantine only after Silver write succeeds
        print("[silver] writing quarantine records...")

        q.write.mode(
            "append"
        ).partitionBy(
            "load_date"
        ).parquet(
            lake("quarantine_encounters")
        )

        # Write DQ report after Silver and quarantine succeed
        print("[silver] writing DQ report...")

        dq.write.mode(
            "append"
        ).parquet(
            lake("gold_dq_report")
        )

        # Update manifest only after all writes succeed
        append_manifest(
            "silver.jsonl",
            [
                {"batch_id": batch_id}
                for batch_id in todo
            ],
        )

        print(
            f"[silver] batches={len(todo)} "
            f"rows={stats['rows']} "
            f"quarantined={stats['q']} "
            f"resubmissions_removed={total_resub}"
        )

        return {
            "rows_in": stats["rows"],
            "quarantined": stats["q"],
            "resub": total_resub,
        }

    finally:
        # Release disk persistence after processing
        df.unpersist()


def run(spark):
    build_reference(spark)
    return build_encounters(spark)
