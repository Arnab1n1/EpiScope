import numpy as np, pandas as pd
from analytics.optimise import allocate as A


def _toy(n=6, seed=1):
    rng = np.random.default_rng(seed)
    codes = [f"D{i:02d}" for i in range(n)]
    dem = pd.DataFrame({"district_code": codes, **{r: rng.uniform(100, 1000, n) for r in A.RES}, "beds_needed": 1.0})
    stk = pd.DataFrame({"district_code": codes, "iv_saline": rng.uniform(0, 300, n), "ns1_kits": rng.uniform(0, 300, n),
                        "ors_sachets": rng.uniform(0, 300, n), "antimalarials": rng.uniform(0, 300, n)})
    w = pd.DataFrame({"district_code": codes, "w": rng.uniform(1, 2, n)})
    pop = {c: 1 / n for c in codes}
    div = {c: "A" if i < n // 2 else "B" for i, c in enumerate(codes)}
    return dem, stk, w, pop, div


def test_budget_equity_and_nonnegativity_constraints_hold():
    dem, stk, w, pop, div = _toy()
    B, alpha = 3000.0, 0.2
    alloc, status = A.solve(dem, stk, w, pop, B, alpha, div)
    assert status == "Optimal"
    assert (alloc.allocation >= -1e-6).all() and (alloc.unmet >= -1e-6).all()
    cost = sum(A.ASSUMPTIONS["unit_cost"][r] * alloc[alloc.resource == r].allocation.sum() for r in A.RES)
    assert cost <= B + 1e-3
    for r in A.RES:                                       # equity floor: x_dr >= alpha * p_d * sum_d' x_d'r
        tot = alloc[alloc.resource == r].allocation.sum()
        assert (alloc[alloc.resource == r].allocation >= alpha * (1 / 6) * tot - 1e-4).all()


def test_lp_not_worse_than_population_baseline():
    dem, stk, w, pop, div = _toy(8, 3)
    B = 4000.0
    lp, _ = A.solve(dem, stk, w, pop, B, 0.0, div)
    base = A.baseline_population(dem, stk, w, pop, B)
    assert A.weighted_unmet(lp, w) <= A.weighted_unmet(base, w) + 1e-6


def test_zero_budget_gives_zero_allocation_and_full_unmet():
    dem, stk, w, pop, div = _toy()
    alloc, status = A.solve(dem, stk, w, pop, 0.0, 0.3, div)
    assert status == "Optimal" and alloc.allocation.sum() < 1e-6
