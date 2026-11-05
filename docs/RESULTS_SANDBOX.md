# EpiScope Results: Full-Scale Run and Low-Scale Sandbox

This file separates the recorded SF=1 project run from earlier SF=0.01 sandbox experiments. The two sets of results have different data volumes and hardware conditions and must not be combined into a single performance claim.

## 1. Recorded full-scale project run

**Conditions:** SF=1, seed 42, synthetic encounter data, Windows environment, Java 17, PySpark 3.5.1. The pipeline run generated data for 2021–2025 across the 64 districts.

### 1.1 Data generation and pipeline

| Measurement | Recorded result |
|---|---:|
| Generated encounter rows | 25,452,837 |
| Silver rows reported by the pipeline | 24,953,759 |
| Quarantined rows | 569,314 |
| Resubmissions removed | 499,078 |
| Patient duplicates merged | 633,305 |
| Late-extreme records excluded | 2,094 |
| Gold onset rows | 83,840 |
| Silver + Gold pipeline runtime | 2,850.6 seconds (47.51 minutes) |

The generator exceeded the assignment minimum of 25 million encounter records. The recorded Silver and Gold runtime was approximately 47.51 minutes, so the assignment's 45-minute target was **not achieved** in this run.

The counts above are pipeline-reported categories. Do not add them together as if they were mutually exclusive partitions of the generated row count unless the pipeline's accounting definitions confirm that they are disjoint.

### 1.2 Detection

The recorded full-scale analytics run selected EARS C2 with threshold `tau=8.0`.

| Metric | Recorded result |
|---|---:|
| Sensitivity | 0.75 |
| False alerts per day | 7.27 |
| Median detection delay | 15 days |
| Naive alerts in artefact window | 2 |
| Gated alerts in artefact window | 0 |

**Metric caution:** false alerts per day is not the same measure as false alerts per district-year. Do not compare the 7.27/day value directly with the proposal's initial target of 12 false alerts per district-year. The operating point and metric should be interpreted using the final run's evaluation output.

### 1.3 Forecast backtesting

The full-scale recorded MAE comparison is:

| Disease | Horizon | Model MAE | Seasonal-naive MAE |
|---|---:|---:|---:|
| Dengue | 1 week | 39.0211 | 93.4467 |
| Dengue | 2 weeks | 54.5017 | 93.4467 |
| Dengue | 3 weeks | 58.2504 | 93.4467 |
| Dengue | 4 weeks | 59.0791 | 93.4467 |
| Diarrhoeal disease | 1 week | 39.0633 | 91.2957 |
| Diarrhoeal disease | 2 weeks | 53.3723 | 91.2957 |
| Diarrhoeal disease | 3 weeks | 51.5538 | 91.2957 |
| Diarrhoeal disease | 4 weeks | 69.7577 | 91.2957 |

The recorded 80% prediction-interval coverage varied approximately from 0.7589 to 0.8633 across the evaluated results. This is an aggregate range; horizon-specific coverage values are not reproduced here. Production forecasts were generated for Dengue, diarrhoeal disease, ILI and Malaria.

### 1.4 Analytics runtime

| Analytics stage | Runtime |
|---|---:|
| Detection | 30.3 seconds |
| Forecast backtesting | 87.6 seconds |
| Production forecasting | 19.7 seconds |
| Optimisation | 10.2 seconds |
| **Total** | **147.8 seconds** |

### 1.5 Tests and dashboard export

- Recorded test run: **16 passed, 18 warnings in 950.24 seconds**. The warnings were Pandas deprecation warnings.
- Aggregated dashboard JSON export: approximately **7.21 MB**, below the assignment's 10 MB target.

The test result applies to the code and environment used for that run. It does not replace the required instructor-style hidden-drop demonstration.

## 2. Low-scale build-sandbox experiments

**Conditions:** SF=0.01 (approximately 255,000 encounters), seed 42, one CPU core, 3 GB RAM, Java 17, Spark 3.5.1, synthetic weather and approximate population inputs.

These are low-scale engineering experiments. Detection and interval-coverage estimates are less reliable at this scale. They are retained to document early tuning and optimisation work, not as the final full-scale project metrics.

### 2.1 Sandbox detection

EARS C2 gated experiment at `tau=2.5`, with a target cap of 12 false alerts per district-year:

- Sensitivity: 0.49 (48/97 district-level outbreaks, including the national surge contribution recorded in the experiment).
- False alerts: 10.39 per district-year.
- Median detection delay: 15 days.
- Artefact-window alerts: naive 2, gated 0.
- In this sandbox comparison, gating reduced measured sensitivity from 0.53 to 0.49, corresponding to approximately three fewer detected outbreaks.
- CUSUM detected the slow-growing outbreak (1/1); C2 did not (0/1).

See the detection experiment CSV files and figure in `docs/experiments/`, if those artifacts are present in the repository.

### 2.2 Sandbox forecast backtest

Rolling-origin backtest over the last 52 weeks, refitting every four weeks:

| Disease | Horizon | Seasonal-naive MAE | Model MAE | Relative MAE | Relative WIS | 80% coverage |
|---|---:|---:|---:|---:|---:|---:|
| Dengue | 1 week | 1.4483 | 0.9476 | 0.6543 | 0.5466 | 0.9289 |
| Dengue | 2 weeks | 1.4483 | 0.9960 | 0.6877 | 0.5851 | 0.9260 |
| Dengue | 3 weeks | 1.4483 | 1.0110 | 0.6980 | 0.5993 | 0.9228 |
| Dengue | 4 weeks | 1.4483 | 0.9971 | 0.6884 | 0.5930 | 0.9199 |
| Diarrhoeal | 1 week | 1.9829 | 1.3781 | 0.6950 | 0.6286 | 0.8973 |
| Diarrhoeal | 2 weeks | 1.9829 | 1.3821 | 0.6970 | 0.6333 | 0.8957 |
| Diarrhoeal | 3 weeks | 1.9829 | 1.3899 | 0.7009 | 0.6456 | 0.8867 |
| Diarrhoeal | 4 weeks | 1.9829 | 1.4719 | 0.7423 | 0.7263 | 0.9105 |

Relative MAE below 1 indicates lower MAE than the seasonal-naive baseline. Coverage around 0.89–0.93 is above the nominal 0.80 level, indicating that the prediction intervals were too wide in this low-count sandbox experiment.

### 2.3 Sandbox allocation

The retained `allocation_summary.csv` indicates that LP weighted unmet demand was below the population-proportional baseline at each tested budget level. Increasing the equity-floor parameter increased total unmet demand, reflecting the efficiency-equity trade-off.

These allocation results are from SF=0.01. They are not full-scale allocation results. Keep the CSV and its execution conditions with any reported comparison.

### 2.4 Sandbox Spark optimisation

All timings below are from SF=0.01 on one CPU core. They are not full-scale performance measurements.

| Experiment | Before (s) | After (s) | Observation |
|---|---:|---:|---|
| Broadcast join of facility registry | 3.266 | 2.215 | Approximately 32.2% faster in this run |
| AQE coalesce for Dhaka skew test | 1.727 | 1.450 | Approximately 16.0% faster in this run |
| Two-stage salted aggregation | 1.450 | 1.514 | Approximately 4.4% slower; salting did not help at this scale |
| Partition pruning | 0.439 | 0.173 | Approximately 60.6% faster |
| File compaction | 1.368 | 0.440 | Approximately 67.8% faster |

Compaction reduced the file count from 563 to 56 and output size from 17.53 MB to 12.26 MB. Salting did not improve this test and must not be presented as a performance gain.

### 2.5 Sandbox quality and privacy

| Measurement | Sandbox result |
|---|---:|
| Input rows | 250,273 |
| Quarantined rows | 5,689 |
| Duplicate patients merged | 6,298 |
| Duplicate precision | 1.00 |
| Duplicate recall against injected duplicates | 0.89 |
| Late-extreme records excluded | 25 |

The sandbox test record states that 15 fast tests and the idempotency integration test passed. This is a separate earlier test run from the 16-pass full-scale project test run above.

## 3. Interpretation and reporting rules

1. Use Section 1 for the recorded SF=1 pipeline, analytics, forecasting, tests and dashboard-export results.
2. Use Section 2 only when explicitly identifying the SF=0.01 sandbox conditions.
3. Do not compare `false alerts/day` with `false alerts/district-year`; they are different metrics.
4. Do not present SF=0.01 allocation or Spark optimisation measurements as full-scale results.
5. Do not claim the assignment's 45-minute SF=1 pipeline target was met; the recorded runtime was 47.51 minutes.
6. Do not claim the instructor-style 14-day hidden-drop rerun has been demonstrated unless before-and-after evidence from that exact procedure is added.
7. All encounter data and resulting model outputs are synthetic educational outputs, not real-world public-health estimates.
