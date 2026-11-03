# Results from the build sandbox (NOT your team's results)
**Conditions:** SF=0.01 (~255k encounters), seed 42, 1 CPU core, 3 GB RAM, Java 17, Spark 3.5.1, *synthetic* weather, approximate populations.
Re-run everything on your hardware (SF>=0.25) and replace these numbers; counts this small make detection and coverage unreliable.

## Detection (EARS C2, gated, operating point tau=2.5, cap 12 false alerts / district-year)
sensitivity 0.49 (48/97 district-level outbreaks incl. 64 from the national surge), false alerts/district-year 10.39, median days to detection 15.
Artefact window alerts: naive = 2, gated = 0 (gate costs ~3 detected outbreaks: 0.53 naive vs 0.49 gated sensitivity at tau=2.5).
CUSUM detected the slow-growing outbreak (1/1); C2 missed it (0/1). See `docs/experiments/detection_*.csv`, `fig_detection_tradeoff.png`.

## Forecast backtest (rolling origin, last 52 weeks, refit every 4 weeks)
| disease_group   |   horizon |   mae_seasonal_naive |   mae_model |   relative_mae |   relative_wis |   coverage80 |
|:----------------|----------:|---------------------:|------------:|---------------:|---------------:|-------------:|
| DENGUE          |         1 |               1.4483 |      0.9476 |         0.6543 |         0.5466 |       0.9289 |
| DENGUE          |         2 |               1.4483 |      0.996  |         0.6877 |         0.5851 |       0.926  |
| DENGUE          |         3 |               1.4483 |      1.011  |         0.698  |         0.5993 |       0.9228 |
| DENGUE          |         4 |               1.4483 |      0.9971 |         0.6884 |         0.593  |       0.9199 |
| DIARRHOEAL      |         1 |               1.9829 |      1.3781 |         0.695  |         0.6286 |       0.8973 |
| DIARRHOEAL      |         2 |               1.9829 |      1.3821 |         0.697  |         0.6333 |       0.8957 |
| DIARRHOEAL      |         3 |               1.9829 |      1.3899 |         0.7009 |         0.6456 |       0.8867 |
| DIARRHOEAL      |         4 |               1.9829 |      1.4719 |         0.7423 |         0.7263 |       0.9105 |
Relative MAE < 1 = beats seasonal naive. 80% coverage is ~0.90-0.93: intervals are too wide (discrete, low counts) - report this honestly.

## Allocation
See `allocation_summary.csv`: LP weighted unmet demand is below the population-proportional baseline at every budget level; alpha raises unmet demand (the price of equity).

## Spark optimisations (SF=0.01, 1 core; take Spark UI screenshots on your machine)
| optimisation                                           |   before |   after | note                                                                                             |
|:-------------------------------------------------------|---------:|--------:|:-------------------------------------------------------------------------------------------------|
| broadcast join of registry                             |    3.266 |   2.215 | sort-merge join shuffles the large side; broadcast ships the ~600-row registry to every task     |
| Dhaka skew: AQE coalesce (off -> on)                   |    1.727 |   1.45  | AQE merges tiny post-shuffle partitions and splits skewed ones                                   |
| Dhaka skew: two-stage salted aggregation vs plain (on) |    1.45  |   1.514 | salting spreads the hot key; helps only when one key dominates a stage                           |
| partition pruning (epi_year=2023, division=Dhaka)      |    0.439 |   0.173 | filter on partition columns skips whole directories; see 'number of files read' in the scan node |
| file compaction (563 -> 56 files)                      |    1.368 |   0.44  | daily incremental appends create many small files; compaction cuts task and listing overhead     |
| file compaction file count                             |  563     |  56     | size 17.53 MB -> 12.26 MB                                                                        |
Salting did not help here (no dominant key at this scale) - report that, do not claim a gain.

## Quality / privacy
250,273 rows in, 5,689 quarantined, 6,298 duplicate patients merged (precision 1.00, recall 0.89 vs injected duplicates), 25 late-extreme excluded.
Tests: 15 fast tests + idempotency integration test pass.
