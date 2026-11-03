import hashlib, os, glob, tempfile
from generator.generate import generate


def _digest(root):
    h = hashlib.sha256()
    for f in sorted(glob.glob(os.path.join(root, "s4_encounters", "*", "encounters.csv"))):
        h.update(open(f, "rb").read())
    return h.hexdigest()


def test_generator_is_deterministic_for_a_seed():
    with tempfile.TemporaryDirectory() as t:
        a = generate(os.path.join(t, "a", "raw"), 0.01, 7, "2021-03-01", "2021-03-14")
        b = generate(os.path.join(t, "b", "raw"), 0.01, 7, "2021-03-01", "2021-03-14")
        c = generate(os.path.join(t, "c", "raw"), 0.01, 8, "2021-03-01", "2021-03-14")
        assert a["rows_written"] == b["rows_written"] > 0
        assert _digest(os.path.join(t, "a", "raw")) == _digest(os.path.join(t, "b", "raw"))
        assert _digest(os.path.join(t, "a", "raw")) != _digest(os.path.join(t, "c", "raw"))
