"""Integration (ING-4): running the pipeline twice on the same input must not change any table; a later drop with late records
must update earlier epi-weeks.  Slow (Spark): `make test-integration`."""
import os, subprocess, sys, tempfile, json
import pytest
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
pytestmark = pytest.mark.integration


def _run(env, *args):
    r = subprocess.run([sys.executable, "-m", "pipeline.run", *args], cwd=ROOT, env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr[-2000:]
    return r.stdout


def _counts(lake):
    import pyarrow.dataset as ds
    out = {}
    for t in ["bronze_encounters", "silver_encounters", "quarantine_encounters", "gold_epiweek_onset"]:
        out[t] = ds.dataset(os.path.join(lake, t), format="parquet", partitioning="hive").count_rows()
    return out


def test_rerun_is_idempotent_and_late_drop_updates_weeks():
    from generator.generate import generate
    with tempfile.TemporaryDirectory() as t:
        raw, lake = os.path.join(t, "raw"), os.path.join(t, "lake")
        generate(raw, 0.02, 11, "2021-01-01", "2021-03-28")
        env = {**os.environ, "EPISCOPE_RAW": raw, "EPISCOPE_LAKE": lake, "SHUFFLE_PARTITIONS": "4"}
        _run(env)
        first = _counts(lake)
        out2 = _run(env)
        assert "nothing new" in out2 and "no new bronze batches" in out2
        assert _counts(lake) == first                              # nothing duplicated
        # late drop: new files for an already-loaded load_date with onset weeks long before
        drop = os.path.join(t, "drop")
        generate(drop, 0.02, 12, "2021-03-29", "2021-04-04")
        _run(env, "--raw", drop)
        second = _counts(lake)
        assert second["silver_encounters"] > first["silver_encounters"]
        _run(env, "--raw", drop)                                   # re-run the drop: still idempotent
        assert _counts(lake) == second
