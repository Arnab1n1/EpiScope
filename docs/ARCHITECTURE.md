# EpiScope: Architecture and Design Decisions

This document summarises the architecture implemented in the EpiScope repository. It is a short technical reference; the final report contains the wider design rationale, evaluation and limitations.

## 1. Processing layers

- **Bronze:** source records are ingested into Parquet with load metadata, including source, load time, batch identifier and checksum. The ingestion layout uses load-date or snapshot-date partitioning where applicable.
- **Silver:** records are validated, cleaned, de-identified and deduplicated. Invalid records are written to quarantine with reasons. Silver data is partitioned by epidemiological year and division.
- **Gold:** curated epidemiological and operational marts are built from Silver data and partitioned by epidemiological year and division.

The pipeline uses Spark/PySpark 3.5.1 and Parquet. Gold outputs are rebuilt from the processed Silver data so that late-arriving records can be reflected in revised aggregates.

## 2. Gold data marts and reporting time

Two time perspectives are maintained:

- **Onset-week mart:** groups records by disease onset week. It can be revised when late reports arrive and is used for seasonality and preparedness-calendar analysis.
- **Report-week surveillance mart:** represents what had been reported by a given reporting week. It is used for detection and forecasting backtests to reduce look-ahead bias from later revisions.

This distinction is important because a retrospective onset-based total is not necessarily the information that decision-makers had available at the time.

## 3. Reporting artefact guard

The pipeline calculates facility-reporting completeness and late-report share. Weeks with suspicious reporting patterns are marked with an `artefact_suspect` flag. The gated detection path uses reports within the configured reporting window and excludes flagged weeks, while the naive path uses the un-gated series.

In the SF=0.01 sandbox experiment, the naive path raised two alerts in the artefact window and the gated path raised none. The gated path also had lower measured sensitivity in that small experiment. These are low-scale sandbox results, not the full-scale operating-point results. See [`RESULTS_SANDBOX.md`](RESULTS_SANDBOX.md).

## 4. Incremental processing and idempotency design

- Bronze checks previously loaded path and SHA-256 checksum combinations to avoid re-ingesting the same source file.
- Silver processes unseen batch identifiers and applies an anti-join on `encounter_id` to avoid adding already processed encounters.
- Gold is recomputed from the resulting Silver data so late records can revise affected aggregates.

These mechanisms provide idempotency-related safeguards. However, a successful instructor-style 14-day hidden-drop run followed by a second identical run must be demonstrated with before-and-after evidence before claiming that the complete hidden-drop requirement has been verified.

## 5. Forecasting

The forecasting implementation uses a district-disease count model. The mean is estimated with a Poisson GLM using seasonal harmonic terms, autoregressive terms, lagged weather features where available, and population/reporting-completeness offsets. NB2 dispersion is estimated separately using moments. Forecasts are produced directly for one-to-four-week horizons, and the rolling-origin backtest refits every four weeks.

Backtesting is performed for Dengue and diarrhoeal disease. Production forecast outputs are generated for Dengue, diarrhoeal disease, ILI and Malaria. The final performance values are reported in the results documentation.

## 6. Resource allocation

The allocation component uses linear programming implemented with PuLP/CBC. It evaluates budget levels and equity-floor settings for the dashboard what-if scenarios. A population-proportional allocation is used as a baseline. The equity parameter controls the minimum allocation protection and therefore exposes the trade-off between total unmet demand and distributional fairness.

The retained allocation comparison in the sandbox results was run at SF=0.01 on one CPU core. It must not be described as a full-scale allocation benchmark.

## 7. Privacy controls

- Synthetic `patient_id` values are salted and hashed with SHA-256, truncated to 128 bits.
- The salt is supplied through an environment variable or a git-ignored `.secrets/` location and must never be committed.
- Silver data does not retain the direct `patient_id` or exact `age_years` fields.
- Patient-count cells from 1 to 4 are suppressed in published patient-count series.
- District outputs are exported as separate aggregated bundles.

These controls reduce exposure in the prototype but do not constitute a formal privacy certification, a complete re-identification audit or server-side role-based access control.

## 8. Implementation boundaries and limitations

- The default workflow is batch-oriented; a Kafka/streaming refresh path is not implemented.
- MinIO-compatible configuration is present, but a completed object-store load test has not been demonstrated.
- Delta/Iceberg time travel is not implemented.
- The bundled population references are approximate for some generator uses; they should not be presented as a complete district-specific joint age-by-sex distribution.
- Weather is treated as observed only when the historical fetch has completed and the cache is available. Otherwise the generator uses synthetic weather.
- A schematic map fallback must not be described as validated official district geometry.
- The SF=1 pipeline run, detection/forecast metrics and small-scale Spark optimisation experiments have different execution conditions. Keep their results separate when reporting performance.
