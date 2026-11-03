# One command from a clean clone:  make all      (ENG-1)
PY ?= python
SF ?= 0.05
SEED ?= 42

.PHONY: all setup generate pipeline analytics serve drop test test-integration scale spark-opt clean
all: setup generate pipeline analytics
	@echo "Done. Run 'make serve' and open http://localhost:8000"

setup:
	$(PY) -m pip install -r requirements.txt
	@test -f .env || cp .env.example .env

generate:
	$(PY) -m generator.generate --sf $(SF) --seed $(SEED) --out data/raw

pipeline:
	$(PY) -m pipeline.run

analytics:
	$(PY) -m analytics.run_all

# Hidden-test-style incremental drop: DIR defaults to a freshly generated 14-day drop (never hard-coded dates).
DIR ?= data/drop
drop:
	@test -d $(DIR) || $(PY) -m generator.generate --drop --sf $(SF) --seed 777 --out $(DIR)
	$(PY) -m pipeline.run --raw $(DIR)
	$(PY) -m analytics.run_all --skip-backtest
	@echo "Drop ingested; alerts, allocation plan and dashboard data regenerated."

serve:
	cd dashboard && $(PY) -m http.server 8000

test:
	$(PY) -m pytest -q -m "not integration"

test-integration:
	$(PY) -m pytest -q -m integration

scale:
	$(PY) scripts/scale_experiment.py

spark-opt:
	$(PY) scripts/spark_opt_experiment.py

clean:
	rm -rf data/raw data/drop data/lake data/lake_manifests data/ground_truth* dashboard/data/*.json dashboard/data/district
