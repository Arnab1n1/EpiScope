# Data sources
| ID | Dataset | Status in this repo |
|---|---|---|
| S1 boundaries | geoBoundaries ADM2 (CC-BY 4.0) / HDX | **Not bundled** (no network in the build environment). Dashboard falls back to a schematic Voronoi map. Convert with `mapshaper -simplify 5% -o format=topojson districts.topo.json` and place in `dashboard/data/`. Feature property `shapeName` must equal the district name in `data/reference/districts.csv`. |
| S2 population | BBS Census 2022 / WorldPop | `data/reference/districts.csv` holds **approximate placeholder values** (thousands, rounded). Replace before reporting incidence. |
| S3 weather | Open-Meteo archive API | `generator/fetch_weather.py` fetches + caches real data. Without a cache the generator uses a deterministic **synthetic** weather fallback (clearly not real). |
| S4-S6 | Synthetic generator | `generator/generate.py` (seeded). |
| S7 OpenDengue | Optional | Not used. |
