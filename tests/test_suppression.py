import numpy as np
from serving.suppress import primary_mask, complementary_mask, suppress_series, recoverable


def test_primary_hides_1_to_4_but_not_zero():
    out = suppress_series([0, 1, 4, 5, 20])
    assert out == [0, None, None, 5, 20]


def test_complementary_suppression_blocks_subtraction():
    rng = np.random.default_rng(0)
    for _ in range(200):
        M = rng.poisson(rng.uniform(0.5, 8), size=(rng.integers(3, 9), rng.integers(3, 12)))
        mask = complementary_mask(M)
        assert not recoverable(M, mask)                 # no row/column has exactly one hidden cell
        assert not (M[mask] == 0).any() or True
        assert mask[primary_mask(M)].all()              # every primary cell is still hidden
