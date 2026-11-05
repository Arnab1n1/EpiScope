# EpiScope: M1 Project Proposal

**Course:** CSE 4345 – Big Data Analytics  
**Academic session:** Spring 2026  
**Group:** [Insert group number]

**Team members and roles**

| Member | Student ID | Role |
|---|---|---|
| Arnab Barua | 0222220005101091 | Project Lead and Lead Systems Architect |
| Mohammed Masrafi | 0222220005101080 | Analytics Lead |
| Mohammed Iftekhar Sami | 0222220005101090 | Dashboard and Visualisation Lead |
| Foisal Abideen | 0222220005101072 | Data Quality, Privacy and Validation Lead |

> **Proposal note:** This document records the team's initial M1 plan. The instructor later approved a four-member team, so the role allocation above reflects that approved team arrangement. Implementation details and measured outcomes may differ from this initial plan and are documented in the final report and results files.

## 1. Problem restatement

EpiScope is intended to support district-level public-health surveillance and response planning in Bangladesh. The system will combine health-facility encounter records with facility, population, weather and stock reference inputs. It will provide early warning for Dengue, diarrhoeal disease, influenza-like illness (ILI) and Malaria; produce one-to-four-week case forecasts; estimate hospital and medical-supply demand; support allocation of limited resources; and present the results through an interactive national and district-level dashboard.

The solution must handle large volumes of batch data, late and incomplete reports, duplicate records, reporting artefacts and privacy constraints. Since the assignment is a teaching case, generated patient and operational records will be synthetic and outputs will not be presented as real public-health estimates.

## 2. Stakeholders and conflicting requirements

| Conflict | Initial design decision | Planned evidence |
|---|---|---|
| C1: Early detection vs. false alarms | Tune detection thresholds to balance sensitivity and false-alert burden, with a target cap of 12 false alerts per district-year for the initial experiment. | Threshold trade-off and detection evaluation |
| C2: Geographic detail vs. privacy | Publish district-level aggregates and suppress patient-count cells from 1 to 4; assess complementary suppression where totals could reveal suppressed values. | Suppression tests and export inspection |
| C3: Allocation efficiency vs. equity | Use a linear-programming allocation model with an adjustable equity floor, and compare it with a population-proportional baseline. | Efficiency-equity results across budget scenarios |
| C4: Data freshness vs. processing cost | Use a scheduled batch approach suitable for day-scale public-health decisions; consider streaming only if time and infrastructure permit. | Processing cadence and runtime discussion |
| C5: Forecast accuracy vs. interpretability | Start with an interpretable statistical count model and compare forecasts with a seasonal-naive baseline. | Rolling-origin MAE and interval-coverage results |

The initial C1 target is a planning constraint, not a claim that the final experiment achieved that exact operating point. The final report must state the actual threshold and measured false-alert metric used in the completed run.

## 3. Planned architecture and tools

The proposed workflow uses a Spark 3.5 local environment and Parquet storage with Bronze, Silver and Gold layers. Bronze retains ingested source records and load metadata. Silver applies validation, cleaning, de-identification, deduplication and quarantine rules. Gold provides curated epidemiological and operational data marts.

The analytics plan includes EARS C2 outbreak detection with CUSUM as a comparator, one-to-four-week forecasting for Dengue and diarrhoeal disease, production forecasts for all four disease groups, demand conversion, linear-programming resource allocation using PuLP/CBC, and a seasonal preparedness calendar. The dashboard is planned with D3.js v7.

Further architecture decisions and implementation details are maintained in [`ARCHITECTURE.md`](ARCHITECTURE.md).

## 4. Initial risk register

| # | Risk | Planned mitigation |
|---|---|---|
| 1 | Full-scale generation and Spark processing may exceed laptop resources or runtime targets. | Use deterministic seeded generation, test at smaller scale first, monitor stage runtimes and investigate Spark partitioning and join strategies. |
| 2 | An incremental or hidden test drop may introduce duplicates or late records. | Avoid hard-coded dates and file names; test repeat processing and verify duplicate handling and revised aggregates. |
| 3 | Late changes to Gold schemas may break analytics and dashboard code. | Define and review data contracts before integrating downstream modules. |
| 4 | Weather or boundary API access may fail. | Cache successful downloads and clearly label any synthetic fallback data. |
| 5 | Uneven team participation may make responsibilities difficult to verify. | Assign lead responsibilities, review contributions regularly and maintain an agreed contribution matrix. |
| 6 | Java and Spark version incompatibility may prevent local execution. | Use and document a compatible Java 17 and Spark 3.5 environment. |

## 5. Expected deliverables

The planned deliverables are a reproducible Spark data pipeline, documented quality and privacy controls, quantitatively evaluated detection and forecasting, resource-allocation scenarios, a coordinated D3.js dashboard, a technical report with a non-technical policy brief, and a recorded team presentation.

The final implementation, limitations and measured results are reported separately. This proposal describes the intended approach at the start of the project and should not be read as evidence that every proposed feature was completed.
