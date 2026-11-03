"""BRONZE: raw, immutable, append-only. Adds _source, _load_ts, _batch_id, _file_checksum, _input_file (ING-1).
Idempotent (ING-4): files already ingested (same path AND checksum) are skipped, so a re-run adds nothing."""
import glob, os, hashlib, datetime as dt
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pipeline.common import lake, RAW, REF, read_manifest, append_manifest, sha256_file

SOURCES = {
    # name: (glob relative to RAW, format, partition column, table)
    "s4_encounters": ("s4_encounters/*/encounters.csv", "csv", "load_date", "bronze_encounters"),
    "s5_registry": ("s5_registry/*.csv", "csv", None, "bronze_registry"),
    "s3_weather": ("s3_weather/*.csv", "csv", None, "bronze_weather"),
    "s6_stock": ("s6_stock/*/stock.json", "json", "snapshot_date", "bronze_stock"),
}


def _uri(p):
    return "file://" + os.path.abspath(p)


def ingest_source(spark, name, raw_root=None):
    pattern, fmt, part, table = SOURCES[name]
    raw_root = raw_root or RAW
    files = sorted(glob.glob(os.path.join(raw_root, pattern)))
    done = {(m["path"], m["sha256"]) for m in read_manifest("bronze.jsonl") if m["source"] == name}
    todo = []
    for f in files:
        sha = sha256_file(f)
        if (os.path.abspath(f), sha) not in done:
            todo.append((os.path.abspath(f), sha))
    if not todo:
        print(f"[bronze] {name}: nothing new ({len(files)} files already ingested)")
        return 0
    now = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    meta = [(_uri(p), hashlib.sha1((p + s).encode()).hexdigest()[:12], s, p) for p, s in todo]
    mdf = spark.createDataFrame(meta, ["_uri", "_batch_id", "_file_checksum", "_input_file"])
    paths = [_uri(p) for p, _ in todo]
    if fmt == "csv":
        df = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(paths)   # all columns as strings: no silent coercion
    else:
        df = spark.read.json(paths)
        df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
    df = df.withColumn("_uri", F.input_file_name())
    if part and part not in df.columns:   # partition value lives in the directory name (hive style)
        df = df.withColumn(part, F.regexp_extract("_uri", part + r"=([0-9\-]+)", 1))
    df = (df.join(F.broadcast(mdf), "_uri", "left").drop("_uri")
          .withColumn("_source", F.lit(name)).withColumn("_load_ts", F.lit(now).cast("timestamp")))
    w = df.write.mode("append")
    if part:
        w = w.partitionBy(part)
    w.parquet(lake(table))
    append_manifest("bronze.jsonl", [{"source": name, "path": p, "sha256": s, "loaded_at": now,
                                       "batch_id": hashlib.sha1((p + s).encode()).hexdigest()[:12]} for p, s in todo])
    print(f"[bronze] {name}: ingested {len(todo)} new file(s)")
    return len(todo)


def run(spark, raw_root=None):
    n = 0
    for s in SOURCES:
        n += ingest_source(spark, s, raw_root)
    # S1/S2 reference (district boundaries/population) is copied as a small, versioned reference table
    ref = spark.read.option("header", True).csv(os.path.join(REF, "districts.csv"))
    ref.write.mode("overwrite").parquet(lake("ref_districts"))
    return n
