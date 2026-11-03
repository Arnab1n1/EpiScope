# EpiScope: big-data surveillance, forecasting and response planning

**Video:** `<paste unlisted/anyone-with-link URL here>`   |   Course: CSE 4345 Big Data Analytics (CEP), Premier University   |   Tag: `v1.0`

> **All encounter data is SYNTHETIC.** Boundaries/population/weather are public sources (see `docs/DATA_SOURCES.md`).
> **Status of this repository:** a complete, runnable implementation that was executed end-to-end at **SF=0.01 on a 1-core sandbox**
> (see `docs/RESULTS_SANDBOX.md`). The scale experiment (SF 0.25/0.5/1), the 45-minute check, real Open-Meteo weather and real
> geoBoundaries geometry are **for your team to run** and fill in (Section "What you still have to do").

## Prerequisites
* Python 3.10+, **Java 17** (Spark 3.5 + Arrow work on Java 17; Java 21 may fail in `applyInPandas`), 16 GB RAM recommended
* `pip install -r requirements.txt` (versions pinned; `pulp==2.9.0` is required: PuLP 4.x changed its API)
* Optional: Docker (MinIO object store, `docker-compose.yml`), Airflow (`orchestration/episcope_dag.py`)

## One command (ENG-1)
```bash
make all SF=0.25 SEED=42     # installs deps, generates data, bronze->silver->gold, detection, forecasts, optimiser, calendar, dashboard JSON
make serve                    # http://localhost:8000   (or: cd dashboard && python -m http.server)
```
`make all` defaults to SF=0.05 (fast). Use `SF=1` for the full 25M-row run. District-officer view: `http://localhost:8000/?role=district&d=D14`.

## Hidden test drop (incremental, unannounced files)
```bash
make drop DIR=/path/to/drop    # same schema; nothing is hard-coded (dates, file names, row counts, facility lists)
```
This ingests only new/changed files (checksum manifest), quarantines defects, updates affected epi-weeks (late data up to 21 days),
re-runs detection/forecast/allocation and rewrites `dashboard/data/*.json`. Without `DIR` a synthetic 14-day drop is generated first.
Re-running the same drop changes nothing (proved by `tests/test_idempotency.py`, `make test-integration`).

## Architecture (medallion lakehouse on Spark)
`generator/` -> `data/raw` -> **bronze** (raw, immutable, +source/load_ts/batch_id/checksum) -> **silver** (typed, 12+ quality rules,
quarantine, de-identified) -> **gold** (epi-week marts, weather lags, completeness, lineage) -> `analytics/` (detection, forecast,
demand, LP, calendar) -> `serving/` (suppressed JSON, <=10 MB) -> `dashboard/` (D3 v7, 5 linked views). Details: `docs/ARCHITECTURE.md`.

Deviation from the reference architecture (justified): tables are **Parquet** (not Delta). Delta needs extra jars/network; idempotency is
implemented with a checksum manifest + anti-join on `encounter_id`; time travel (ING-7, "Could") is therefore not implemented.
The Spark SQL catalog registration is session-scoped (temp views); enable Hive support for a persistent metastore.

## Requirement traceability (ID -> file -> report section)
| ID | Pri | Where | Status |
|---|---|---|---|
| ING-1 generator + bronze metadata | M | `generator/generate.py`, `pipeline/bronze/ingest.py` | done, run at SF=0.01 |
| ING-2 Spark for heavy work | M | `pipeline/**`, `analytics/**` (pandas only inside per-group fits / final aggregates) | done |
| ING-3 partitioned Parquet + catalog | M | silver/gold partitioned by `(epi_year, division)`; `register_tables` | done (session catalog) |
| ING-4 idempotent incremental + 21-day late data | M | manifest in `bronze/ingest.py`, `silver/build.py` (anti-join), gold full recompute | done, tested |
| ING-5 object store | S | `docker-compose.yml`, `EPISCOPE_LAKE=s3a://...` | **configured, not run here** |
| ING-6 streaming path | S | - | **not implemented** (daily batch chosen, see report C4) |
| ING-7 ACID/time travel | C | - | not implemented |
| DQ-1 12+ rules + quarantine | M | `pipeline/quality/rules.py`, `silver/build.py` | done (16 rules; 12 row-level + 4 set-level) |
| DQ-2 DQ report table + dashboard | M | `gold_dq_report`, `gold_dq_run`, `dq_report.json`, header tile | done |
| DQ-3 lineage | M | `src_batch_ids` in gold marts | done |
| DQ-4 patient de-dup + precision | S | `gold/build.py::patient_dedup`; precision vs `ground_truth_duplicates.csv` | implemented; **precision not yet measured** (see TODO) |
| PRV-1 hash/age bands | M | `silver/build.py`; `tests/test_privacy_and_isolation.py` | done |
| PRV-2 small-cell suppression | M | `serving/suppress.py`, `tests/test_suppression.py`, `tests/test_payload.py` | done |
| PRV-3 role views | S | `serving/export.py` district bundles, `?role=district&d=` | done (demo districts D01/D14/D15) |
| PRV-4 k-anonymity / DP | C | - | not implemented |
| AN-1 detection in Spark | M | `analytics/detection/detect.py` (C2 window fn, CUSUM applyInPandas) | done |
| AN-2 evaluation + trade-off curve | M | `analytics/detection/evaluate.py`, `docs/experiments/fig_detection_tradeoff.png` | done |
| AN-3 two methods + artefact | S | naive vs gated C2, CUSUM; artefact alerts naive=2, gated=0 | done |
| AN-4 forecasting | M | `analytics/forecast/forecast.py` (applyInPandas per district) | done (4 diseases; backtest on 2) |
| AN-5 backtest vs seasonal naive | M | `analytics/forecast/backtest.py` | done |
| AN-6 demand conversion | M | `analytics/optimise/allocate.py::demand_from_forecast` | done (assumptions are placeholders) |
| AN-7 optimiser vs baseline | M | `analytics/optimise/allocate.py` | done |
| AN-8 efficiency-equity curve | S | grid 5 budgets x 5 alphas -> `allocation_grid.json`, view 4 | done |
| AN-9 preparedness calendar | M | `analytics/calendar/calendar.py` | done |
| AN-10 SEIR | C | - | not implemented |
| VIS-1..4 D3 dashboard | M | `dashboard/` | done; smoke-tested in jsdom, **needs your visual check in a browser** |
| VIS-5 what-if | S | sliders: budget, alpha, tau (precomputed grid + client re-threshold) | done |
| VIS-6 accessibility/honesty | S | viridis, hatching, freshness timestamp, uncertainty bands | done (no formal a11y audit) |
| VIS-7 mobile/animation | C | responsive CSS only | partial |
| ENG-1 one command | M | `Makefile` | done |
| ENG-2 scaling experiment | M | `scripts/scale_experiment.py` | **script ready; you must run it** |
| ENG-3 Spark optimisations | M | `scripts/spark_opt_experiment.py` -> `docs/experiments/spark_optimisations.csv` | script run at SF=0.01; **re-run on your hardware + take Spark UI screenshots** |
| ENG-4 tests + DAG | S | `tests/`, `orchestration/episcope_dag.py` | tests done; DAG not executed here |

## What you still have to do (honest list)
1. Run `make all SF=1` and `make scale`; record hardware and runtimes (ENG-2). Check the 45-minute limit on your machine.
2. Run `python -m generator.fetch_weather` with internet to replace the synthetic weather fallback with real Open-Meteo data; drop real
   boundaries into `dashboard/data/districts.topo.json` (see `docs/DATA_SOURCES.md`). Replace the approximate district populations in
   `data/reference/districts.csv` with BBS 2022 / WorldPop values.
3. Run `make spark-opt` on your laptop and capture Spark UI screenshots (http://localhost:4040).
4. Measure DQ-4 precision (`ground_truth_duplicates.csv` vs `silver_patient_dedup_audit`) and report it.
5. Open the dashboard in a real browser, run the usability test with 2 non-CS users, record changes.
6. Replace every number in the report with YOUR run's numbers; the figures in `docs/RESULTS_SANDBOX.md` came from SF=0.01 on a 1-core sandbox.
7. Fill `AI_USAGE.md` honestly, split commits across all three members, and be able to explain every file.
