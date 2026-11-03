"""ENG-3: before/after evidence for Spark optimisations on the CURRENT lake. Prints + writes docs/experiments/spark_optimisations.csv.
Take Spark UI screenshots at http://localhost:4040 while this runs (it keeps the session open for 20 s at the end with --hold).
 1. broadcast join of the facility registry vs sort-merge join (autoBroadcastJoinThreshold=-1)
 2. skew handling: group-by on a hot key (Dhaka-heavy) with AQE skew/coalesce off vs on, plus manual salting
 3. partition pruning: filtered scan of gold (epi_year/division) vs full scan
 4. file compaction: many small files vs coalesced files (scan time + file count)"""
import csv, os, shutil, sys, time, glob
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pyspark.sql.functions as F
from pipeline.common import spark_session, lake, ROOT


def timeit(fn, reps=3):
    ts = []
    for _ in range(reps):
        t = time.time(); r = fn(); ts.append(time.time() - t)
    return round(min(ts), 3), r


def files(path):
    fs = glob.glob(path + "/**/*.parquet", recursive=True)
    return len(fs), round(sum(os.path.getsize(f) for f in fs) / 1e6, 2)


def main():
    spark = spark_session("episcope-spark-opt")
    rows = []
    enc = spark.read.parquet(lake("silver_encounters")); fac = spark.read.parquet(lake("silver_facilities"))
    enc.count(); fac.count()
    # 1. broadcast vs sort-merge
    def join_job():
        return enc.join(fac.select("facility_id", "facility_type"), "facility_id").groupBy("facility_type").count().collect()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1); spark.conf.set("spark.sql.adaptive.enabled", "false")
    t0, _ = timeit(join_job)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    t1, _ = timeit(lambda: enc.join(F.broadcast(fac.select("facility_id", "facility_type")), "facility_id").groupBy("facility_type").count().collect())
    rows.append(("broadcast join of registry", "runtime_s", t0, t1, "sort-merge join shuffles the large side; broadcast ships the ~600-row registry to every task"))
    # 2. skew: aggregate by district with a hot key
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    t0, _ = timeit(lambda: enc.groupBy("district_code", "disease_group").count().collect())
    spark.conf.set("spark.sql.adaptive.enabled", "true"); spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    t1, _ = timeit(lambda: enc.groupBy("district_code", "disease_group").count().collect())
    salted = lambda: (enc.withColumn("salt", (F.rand(7) * 8).cast("int")).groupBy("district_code", "disease_group", "salt").count()
                      .groupBy("district_code", "disease_group").agg(F.sum("count")).collect())
    t2, _ = timeit(salted)
    rows.append(("Dhaka skew: AQE coalesce (off -> on)", "runtime_s", t0, t1, "AQE merges tiny post-shuffle partitions and splits skewed ones"))
    rows.append(("Dhaka skew: two-stage salted aggregation vs plain (on)", "runtime_s", t1, t2, "salting spreads the hot key; helps only when one key dominates a stage"))
    # 3. partition pruning
    g = spark.read.parquet(lake("gold_epiweek_onset"))
    years = [r[0] for r in g.select("epi_year").distinct().orderBy("epi_year").collect()]
    ty = years[len(years) // 2]
    full, _ = timeit(lambda: g.filter(F.col("cases") > 0).count())
    pr, _ = timeit(lambda: g.filter((F.col("epi_year") == ty) & (F.col("division") == "Dhaka") & (F.col("cases") > 0)).count())
    rows.append((f"partition pruning (epi_year={ty}, division=Dhaka)", "runtime_s", full, pr, "filter on partition columns skips whole directories; see 'number of files read' in the scan node"))
    # 4. compaction
    src = lake("silver_encounters"); tmp = lake("_compacted_silver")
    shutil.rmtree(tmp, ignore_errors=True)
    n0, mb0 = files(src)
    enc.repartition("epi_year", "division").write.partitionBy("epi_year", "division").mode("overwrite").parquet(tmp)
    n1, mb1 = files(tmp)
    a, _ = timeit(lambda: spark.read.parquet(src).groupBy("disease_group").count().collect())
    b, _ = timeit(lambda: spark.read.parquet(tmp).groupBy("disease_group").count().collect())
    rows.append((f"file compaction ({n0} -> {n1} files)", "runtime_s", a, b, "daily incremental appends create many small files; compaction cuts task and listing overhead"))
    rows.append(("file compaction file count", "files", n0, n1, f"size {mb0} MB -> {mb1} MB"))
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(os.path.join(ROOT, "docs/experiments"), exist_ok=True)
    with open(os.path.join(ROOT, "docs/experiments/spark_optimisations.csv"), "w", newline="") as f:
        w = csv.writer(f); w.writerow(["optimisation", "metric", "before", "after", "note"]); w.writerows(rows)
    for r in rows:
        print(r)
    if "--hold" in sys.argv:
        time.sleep(20)
    spark.stop()


if __name__ == "__main__":
    main()
