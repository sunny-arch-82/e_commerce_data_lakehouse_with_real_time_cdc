-include .env
export

.PHONY: help build build-dbt build-jupyter up down ps run-stream replay-quarantine

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "  up-core       Start core services (postgres, minio, kafka, spark, trino, hms)"
	@echo "  up-explore    Start JupyterLab for exploration"
	@echo "  up-datagen    Start data generator"
	@echo "  up-airflow    Start Airflow + dbt"
	@echo "  up-jupyter   Start core + JupyterLab"
	@echo "  up-all        Start everything (core + datagen + explore + airflow)"
	@echo "  down          Stop all containers"
	@echo "  ps             Show running containers"
	@echo ""
	@echo "  build         Build all Docker images"
	@echo "  build-dbt     Build dbt Docker image"
	@echo "  build-jupyter Build JupyterLab image only"
	@echo "  run-stream    Run Spark CDC processor with quarantine/monitoring"
	@echo "  replay-quarantine Preview or replay quarantined CDC records"
	@echo ""
	@echo "Examples:"
	@echo "  make up-core          # Core services only"
	@echo "  make up-all          # Everything"
	@echo "  docker compose --profile core up -d"
	@echo "  docker compose --profile core --profile datagen --profile explore --profile airflow up -d"

# ─── Build ────────────────────────────────────────────────────
build:
	docker compose --profile core --profile datagen --profile explore --profile airflow build

# ─── Up ─────────────────────────────────────────────────────
up-core:
	docker compose --profile core up -d

up-explore:
	docker compose --profile core --profile explore up -d

up-datagen:
	docker compose --profile core --profile datagen up -d

up-airflow:
	docker compose --profile core --profile airflow up -d

up-all:
	docker compose --profile core --profile datagen --profile explore --profile airflow up -d

up-jupyter:
	docker compose --profile core --profile explore build
	docker compose --profile core --profile explore up -d

build-jupyter:
	docker compose --profile explore build

up:
	docker compose --profile core up -d

# ─── Down ─────────────────────────────────────────────────────
down:
	docker compose --profile "*" down

# ─── Status ───────────────────────────────────────────────────
ps:
	@docker compose ps

# ─── Test ─────────────────────────────────────────────────────
test:
	docker compose exec airflow-scheduler /opt/dbt_venv/bin/dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# ─── CDC reliability commands ───────────────────────────────
run-stream:
	docker compose exec jupyter-lab python /home/jovyan/work/stream_processor.py

# Dry-run by default. Set EXECUTE=1 to republish records to Kafka.
# Example: make replay-quarantine TABLE=orders LIMIT=25 EXECUTE=1
replay-quarantine:
	docker compose exec jupyter-lab python /home/jovyan/work/replay_quarantine.py $(if $(TABLE),--table $(TABLE),) $(if $(REASON),--reason $(REASON),) --limit $(or $(LIMIT),100) $(if $(filter 1,$(EXECUTE)),--execute,)
