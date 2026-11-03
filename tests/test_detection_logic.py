import pandas as pd, numpy as np
from analytics.detection.detect import alerts_from, _cusum_pdf


def test_cusum_catches_slow_growth_that_c2_threshold_misses():
    z = np.r_[np.zeros(20), np.full(15, 0.9)]            # persistent +0.9 sd shift
    df = pd.DataFrame({"week_start": pd.date_range("2024-01-01", periods=35, freq="W-MON"), "c2": z})
    out = _cusum_pdf(df)
    assert out.cusum.max() > 4 and (df.c2 > 3).sum() == 0


def test_min_cases_blocks_tiny_count_alerts():
    d = pd.DataFrame({"c2": [5.0, 5.0], "value": [2, 9], "cusum": [0, 0], "week_start": ["2024-01-01"] * 2})
    assert len(alerts_from(d, 3.0)) == 1
