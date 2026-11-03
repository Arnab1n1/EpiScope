"""S3: fetch REAL daily weather from the Open-Meteo archive API at district centroids; responses are cached (URL, timestamp, SHA-256).
Usage: python -m generator.fetch_weather --start 2021-01-01 --end 2025-12-31 --out data/reference/weather_cache.csv
Then:  python -m generator.generate --weather-cache data/reference/weather_cache.csv ...   (the generator then uses real weather)
Note: the synthetic fallback in generate.py is used when no cache exists (e.g. offline). Respect the API rate limits (sleep between calls)."""
import argparse, hashlib, json, os, time
import pandas as pd
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URL = "https://archive-api.open-meteo.com/v1/archive"


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--start", default="2021-01-01"); ap.add_argument("--end", default="2026-12-31")
    ap.add_argument("--out", default=os.path.join(ROOT, "data/reference/weather_cache.csv")); a = ap.parse_args()
    d = pd.read_csv(os.path.join(ROOT, "data/reference/districts.csv")); cache = os.path.join(ROOT, "data/reference/_weather_http_cache"); os.makedirs(cache, exist_ok=True)
    frames, log = [], []
    for r in d.itertuples():
        params = dict(latitude=r.lat, longitude=r.lon, start_date=a.start, end_date=min(a.end, pd.Timestamp.today().strftime("%Y-%m-%d")),
                      daily="precipitation_sum,temperature_2m_mean,relative_humidity_2m_mean", timezone="Asia/Dhaka")
        key = hashlib.sha1(json.dumps(params, sort_keys=True).encode()).hexdigest(); p = os.path.join(cache, key + ".json")
        if not os.path.exists(p):
            resp = requests.get(URL, params=params, timeout=60); resp.raise_for_status(); open(p, "w").write(resp.text); time.sleep(1.5)
        js = json.load(open(p)); sha = hashlib.sha256(open(p, "rb").read()).hexdigest(); log.append((r.district_code, resp.url if 'resp' in dir() else URL, sha))
        dd = js["daily"]
        frames.append(pd.DataFrame({"district_code": r.district_code, "date": dd["time"], "precip_mm": dd["precipitation_sum"],
                                    "tmean_c": dd["temperature_2m_mean"], "humidity": dd["relative_humidity_2m_mean"]}))
    out = pd.concat(frames); out.to_csv(a.out, index=False); pd.DataFrame(log, columns=["district_code", "url", "sha256"]).to_csv(a.out + ".provenance.csv", index=False)
    print("wrote", a.out, len(out), "rows")


if __name__ == "__main__":
    main()
