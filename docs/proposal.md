# EpiScope: M1 proposal (max 2 pages; edit to your team's own decisions)
**Group:** __  **Members/roles:** M1 Data Engineer, M2 Analytics Engineer, M3 Visualisation Engineer

## 1. Problem restatement
The Ministry needs per-district early warning (dengue, diarrhoeal, ILI, malaria), 1-4 week forecasts of cases and hospital/supply demand,
an allocation of a fixed national stock, and a seasonal calendar, on an interactive dashboard that explains itself, tolerates late and incomplete
data and never exposes identifiable patients.

## 2. Stakeholders and conflicts (resolved with evidence in the report)
| Conflict | Our resolution (operating point) | Evidence we will produce |
|---|---|---|
| C1 sensitivity vs false alarms | max sensitivity s.t. <= 12 false alerts per district-year | threshold trade-off curve |
| C2 granularity vs privacy | district level; counts 1-4 suppressed; complementary suppression for margins | suppression tests |
| C3 efficiency vs equity | LP with equity floor alpha; report the curve over 5 budgets | efficiency-equity curve |
| C4 freshness vs cost | daily batch (decision latency is days); streaming only if time allows | cadence argument |
| C5 accuracy vs interpretability | interpretable NB-GLM; compare to a boosted model if time | backtest MAE/WIS |

## 3. Architecture and tools
Spark 3.5 (local), Parquet medallion lakehouse (bronze/silver/gold), PuLP/CBC, statsmodels, D3.js v7. Diseases: dengue + diarrhoeal for the backtest,
all four in production. Detection: EARS C2 (+CUSUM comparator). Justifications in `docs/ARCHITECTURE.md`.

## 4. Risk register
| # | Risk | Mitigation |
|---|---|---|
| 1 | SF=1 too slow/too big for laptops | per-partition seeded generation; test at SF=0.25 first; Spark tuning |
| 2 | Hidden drop breaks pipeline | no hard-coded dates/files; own mock drop test from Day 9 |
| 3 | Gold schema changes late | freeze schema contract by Day 6 |
| 4 | API/network failure for weather/boundaries | cache; offline synthetic fallback |
| 5 | Uneven commits across members | daily standup; review matrix at Day 7 |
| 6 | Java/Arrow incompatibility (Java 21) | use Java 17 (documented) |
