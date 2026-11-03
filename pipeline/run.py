"""Pipeline entry point.  python -m pipeline.run [--raw DIR] [--stages bronze,silver,gold]
Used for the initial load AND for any incremental drop (point --raw at the drop folder; nothing is hard-coded)."""
import argparse, time, json, os
from pipeline.common import spark_session, register_tables, ROOT
from pipeline.bronze import ingest
from pipeline.silver import build as silver
from pipeline.gold import build as gold

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=None, help="raw source root (default: data/raw or $EPISCOPE_RAW)")
    ap.add_argument("--stages", default="bronze,silver,gold")
    a = ap.parse_args()
    spark = spark_session("episcope-pipeline")
    timings = {}
    for st in a.stages.split(","):
        t = time.time()
        if st == "bronze": ingest.run(spark, a.raw)
        elif st == "silver": silver.run(spark)
        elif st == "gold": gold.run(spark)
        timings[st] = round(time.time() - t, 1)
        print(f"== {st} done in {timings[st]}s")
    register_tables(spark)
    os.makedirs(os.path.join(ROOT, "docs/experiments"), exist_ok=True)
    with open(os.path.join(ROOT, "docs/experiments/last_pipeline_timings.json"), "w") as f:
        json.dump(timings, f)
    spark.stop()
