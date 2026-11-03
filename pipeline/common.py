"""Shared config + Spark session. All paths are overridable by environment variables (no hard-coded dates)."""
import os, json, hashlib, glob, secrets
from pyspark.sql import SparkSession

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LAKE = os.environ.get("EPISCOPE_LAKE", os.path.join(ROOT, "data/lake"))      # local path, or s3a://bucket/lake for MinIO
RAW = os.environ.get("EPISCOPE_RAW", os.path.join(ROOT, "data/raw"))
REF = os.path.join(ROOT, "data/reference")
OUT = os.environ.get("EPISCOPE_OUT", os.path.join(ROOT, "dashboard/data"))
MANIFESTS = os.environ.get("EPISCOPE_MANIFESTS", (LAKE.rstrip("/") + "_manifests") if not LAKE.startswith("s3a") else os.path.join(ROOT, "data/lake_manifests"))


JAVA_OPTS = ("-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED "
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED "
             "--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED")


def lake(*p):
    return "/".join([LAKE.rstrip("/")] + list(p))


def get_salt():
    """Salt lives OUTSIDE the repo contents: env var, or a git-ignored .secrets/salt generated on first run."""
    s = os.environ.get("EPISCOPE_SALT")
    if s:
        return s
    p = os.path.join(ROOT, ".secrets", "salt")
    if not os.path.exists(p):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        open(p, "w").write(secrets.token_hex(32))
    return open(p).read().strip()


def spark_session(app="episcope"):
    os.environ["JAVA_TOOL_OPTIONS"] = (os.environ.get("JAVA_TOOL_OPTIONS", "") + " " + JAVA_OPTS).strip()   # reaches the driver JVM in local mode
    b = (SparkSession.builder.appName(app)
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "16"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.enabled", "true")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
         .config("spark.ui.showConsoleProgress", "false")
         # Java 17/21 + Arrow (applyInPandas): needed for DirectByteBuffer access
         .config("spark.driver.extraJavaOptions", JAVA_OPTS).config("spark.executor.extraJavaOptions", JAVA_OPTS)
         .config("spark.sql.execution.arrow.pyspark.enabled", "true"))
    if os.environ.get("MINIO_ENDPOINT"):   # ING-5: S3-compatible object store
        b = (b.config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
             .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY", ""))
             .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY", ""))
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4"))
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


def sha256_file(path, block=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(block), b""):
            h.update(b)
    return h.hexdigest()


def read_manifest(name):
    p = os.path.join(MANIFESTS, name)
    if not os.path.exists(p):
        return []
    return [json.loads(l) for l in open(p) if l.strip()]


def append_manifest(name, rows):
    os.makedirs(MANIFESTS, exist_ok=True)
    with open(os.path.join(MANIFESTS, name), "a") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def register_tables(spark):
    """Register Parquet tables in the Spark SQL catalog (session-scoped; enable Hive support for a persistent metastore)."""
    for t in ["bronze_encounters", "bronze_registry", "bronze_weather", "bronze_stock", "silver_encounters",
              "silver_facilities", "silver_weather", "silver_stock", "quarantine_encounters", "gold_epiweek_onset",
              "gold_surveillance", "gold_dq_report", "gold_stock_district", "gold_facility_week"]:
        try:
            spark.read.parquet(lake(t)).createOrReplaceTempView(t)
        except Exception:
            pass
