"""ENG-2: run the full pipeline at SF in {0.25, 0.5, 1} and record runtime per stage, output sizes and hardware.
Usage: python scripts/scale_experiment.py [--sfs 0.25 0.5 1] [--seed 42]      (SF=1 needs ~45 min on a 16 GB laptop; plan for it)
Writes docs/experiments/scaling.csv. Nothing here is pre-filled: numbers come from YOUR hardware."""
import argparse, os, platform, subprocess, sys, time, shutil, json, csv
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def du(p):
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(p) for f in fs) / 1e6 if os.path.exists(p) else 0.0


def hw():
    mem = None
    try:
        mem = round(int([l for l in open("/proc/meminfo") if l.startswith("MemTotal")][0].split()[1]) / 1e6, 1)
    except Exception:
        pass
    return {"cpu": platform.processor() or platform.machine(), "cores": os.cpu_count(), "ram_gb": mem, "python": platform.python_version(),
            "pyspark": __import__("pyspark").__version__}


def timed(cmd, env):
    t = time.time(); r = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True); dt = time.time() - t
    if r.returncode: print(r.stdout[-1500:], r.stderr[-1500:]); raise SystemExit(f"failed: {cmd}")
    return dt


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--sfs", nargs="+", type=float, default=[0.25, 0.5, 1.0]); ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args(); rows = []
    for sf in a.sfs:
        base = os.path.join(ROOT, "data/scale", f"sf{sf}"); shutil.rmtree(base, ignore_errors=True); os.makedirs(base)
        raw, lake, out = f"{base}/raw", f"{base}/lake", f"{base}/dash"
        env = {**os.environ, "EPISCOPE_RAW": raw, "EPISCOPE_LAKE": lake, "EPISCOPE_OUT": out}
        py = sys.executable
        r = {"sf": sf, "hardware": json.dumps(hw())}
        r["generate_s"] = round(timed([py, "-m", "generator.generate", "--sf", str(sf), "--seed", str(a.seed), "--out", raw], env), 1)
        for st in ["bronze", "silver", "gold"]:
            r[f"{st}_s"] = round(timed([py, "-m", "pipeline.run", "--stages", st], env), 1)
        r["analytics_s"] = round(timed([py, "-m", "analytics.run_all", "--skip-backtest", "--gt", f"{base}/ground_truth"], env), 1)
        r["raw_mb"] = round(du(raw), 1); r["lake_mb"] = round(du(lake), 1); r["dash_mb"] = round(du(out), 2)
        r["total_s"] = round(sum(v for k, v in r.items() if k.endswith("_s")), 1)
        rows.append(r); print(r)
    os.makedirs(os.path.join(ROOT, "docs/experiments"), exist_ok=True)
    with open(os.path.join(ROOT, "docs/experiments/scaling.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
