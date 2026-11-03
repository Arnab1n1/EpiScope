"""PRV-2: small-cell suppression. Primary: 1 <= n <= 4 -> suppressed. Complementary: if a row or column has exactly ONE suppressed
cell, also suppress the next-smallest cell there, repeated until stable, so a published margin cannot be used to recover a suppressed cell
by subtraction. Zeros are real zeros and are published."""
import numpy as np

THRESHOLD = 5        # counts 1..4 are suppressed


def primary_mask(M, threshold=THRESHOLD):
    M = np.asarray(M, float)
    return (M >= 1) & (M < threshold)


def complementary_mask(M, mask=None, threshold=THRESHOLD):
    M = np.asarray(M, float)
    mask = primary_mask(M, threshold) if mask is None else mask.copy()
    changed, guard = True, 0
    while changed and guard < 2000:
        guard += 1
        changed = False
        for axis in (0, 1):
            n = M.shape[1 - axis] if axis == 0 else M.shape[0]
            for i in range(M.shape[1] if axis == 0 else M.shape[0]):
                idx = (slice(None), i) if axis == 0 else (i, slice(None))
                m = mask[idx]
                if m.sum() == 1:
                    vals = np.where(m, np.inf, M[idx])
                    # next smallest NON-suppressed positive cell (prefer positive cells; fall back to zeros)
                    cand = np.where(np.isfinite(vals) & (vals > 0), vals, np.inf)
                    if not np.isfinite(cand).any():      # only zeros left: hide a zero (a zero margin-mate is otherwise a leak)
                        cand = np.where(np.isfinite(vals), 0.0, np.inf)
                    j = int(np.argmin(cand))
                    if np.isfinite(cand[j]):
                        sub = mask[idx]; sub[j] = True
                        mask[idx] = sub
                        changed = True
    return mask


def suppress_series(values, threshold=THRESHOLD):
    """1-D published series: returns list with None for suppressed cells."""
    v = np.asarray(values, float)
    m = primary_mask(v, threshold)
    return [None if mm else int(x) for x, mm in zip(v, m)]


def recoverable(M, mask):
    """Test helper: True if any suppressed cell could be recovered from published row/col margins (i.e. a margin group has exactly 1 hidden cell)."""
    for axis in (0, 1):
        k = mask.sum(axis=axis)
        if np.any(k == 1):
            return True
    return False
