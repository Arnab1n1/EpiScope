import os, re, glob, json
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_detector_never_reads_ground_truth():
    """Detection / pipeline / forecasting / serving code must not reference ground-truth files (evaluate.py + run_all.py are the allowed evaluators)."""
    allowed = {"evaluate.py", "run_all.py"}
    for pat in ["analytics/detection/*.py", "analytics/forecast/*.py", "analytics/optimise/*.py", "analytics/calendar/*.py",
                "pipeline/**/*.py", "serving/*.py"]:
        for f in glob.glob(os.path.join(ROOT, pat), recursive=True):
            if os.path.basename(f) in allowed:
                continue
            assert "ground_truth" not in open(f).read(), f"{f} references ground truth"


def test_no_secrets_committed():
    assert not os.path.exists(os.path.join(ROOT, ".env")) or ".env" in open(os.path.join(ROOT, ".gitignore")).read()
    assert "EPISCOPE_SALT=" in open(os.path.join(ROOT, ".env.example")).read()
    assert ".secrets/" in open(os.path.join(ROOT, ".gitignore")).read()


def test_silver_has_no_direct_identifiers():
    import pytest
    p = os.path.join(ROOT, "data/lake/silver_encounters")
    if not os.path.isdir(p):
        pytest.skip("run the pipeline first")
    import pyarrow.dataset as ds
    cols = set(ds.dataset(p, format="parquet", partitioning="hive").schema.names)
    assert "patient_id" not in cols and "age_years" not in cols
    assert {"patient_hash", "age_band"} <= cols
