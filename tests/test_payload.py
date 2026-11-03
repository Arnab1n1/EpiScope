import os, json, glob
import pytest
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "dashboard/data")
pytestmark = pytest.mark.skipif(not os.path.exists(os.path.join(DATA, "series.json")), reason="run the pipeline first")


def test_payload_under_10mb():
    total = sum(os.path.getsize(f) for f in glob.glob(DATA + "/**/*.json", recursive=True))
    assert total <= 10 * 1024 * 1024, total


def test_no_small_counts_in_any_published_series():
    s = json.load(open(os.path.join(DATA, "series.json")))
    for c, dd in s["series"].items():
        for dis, arr in dd.items():
            assert all(v is None or v == 0 or v >= 5 for v in arr), (c, dis)
    dw = json.load(open(os.path.join(DATA, "division_week.json")))
    for dis, rows in dw["diseases"].items():
        for row in rows:
            assert all(v is None or v == 0 or v >= 5 for v in row)
    for f in glob.glob(DATA + "/district/*.json"):
        b = json.load(open(f))
        for dd in b["series"].values():
            for arr in dd.values():
                assert all(v is None or v == 0 or v >= 5 for v in arr)


def test_district_bundle_contains_only_its_own_detail():
    for f in glob.glob(DATA + "/district/*.json"):
        b = json.load(open(f))
        assert list(b["series"].keys()) == [b["district"]]
        assert list(b["alerts"]["c2_recent"].keys()) == [b["district"]]
        assert list(b["forecasts"].keys()) == [b["district"]]
