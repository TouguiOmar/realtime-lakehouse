# ─────────────────────────────────────────────────────────────────
#  Makefile  —  Real-Time Lakehouse
#  Usage: make <target>
#  Requires: Docker Desktop, GNU Make (install via choco install make)
# ─────────────────────────────────────────────────────────────────

-include .env
export

PACKAGES = org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262

PACKAGES_NO_KAFKA = org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262

SPARK_MASTER   = docker exec lakehouse-spark-master /opt/spark/bin/spark-submit
SPARK_CLUSTER  = --master spark://spark-master:7077 --conf spark.cores.max=2
SPARK_LOCAL    = --master local[2]

.PHONY: up down restart status register seed bronze silver gold pipeline demo logs clean help

## ── Stack ────────────────────────────────────────────────────────

up: ## Start the full stack
	docker compose up -d
	@echo ""
	@echo "Waiting for services to be healthy..."
	@sleep 30
	@$(MAKE) status

down: ## Stop and wipe all volumes
	docker compose down -v
	@echo "Stack stopped and volumes removed."

restart: down up ## Full restart from scratch

status: ## Show health of all services
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

logs: ## Tail logs for a service (usage: make logs s=lakehouse-connect)
	docker compose logs -f $(s)

clean: ## Remove all generated spark/ivy2 jars cache
	@echo "Cleaning ivy2 cache..."
	@rm -rf spark/ivy2
	@echo "Done."

## ── Pipeline ─────────────────────────────────────────────────────

register: ## Register the Debezium CDC connector
	@echo "Registering Debezium connector..."
	@STATUS=$$(docker exec lakehouse-connect curl -s -o /dev/null -w "%{http_code}" \
		-X GET http://localhost:8083/connectors/orders-cdc/status); \
	if [ "$$STATUS" = "200" ]; then \
		echo "Connector already running — skipping registration."; \
	else \
		docker exec lakehouse-connect curl -s -X POST http://localhost:8083/connectors \
			-H "Content-Type: application/json" \
			-d @/debezium/register-connector.json; \
		echo "Connector registered."; \
	fi

seed: ## Insert sample data into Postgres
	docker exec lakehouse-postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "\
		INSERT INTO orders (customer_id, status, total_usd) VALUES \
		(1, 'pending',   99.99), \
		(2, 'completed', 149.50), \
		(3, 'pending',   49.00), \
		(4, 'completed', 200.00), \
		(5, 'pending',   75.00) \
		ON CONFLICT DO NOTHING;"
	@echo "Data seeded."

bronze: ## Start the Bronze Structured Streaming writer (blocking)
	$(SPARK_MASTER) $(SPARK_CLUSTER) \
		--packages $(PACKAGES) \
		/opt/spark-apps/bronze_writer.py

silver: ## Run the Silver MERGE writer (batch)
	$(SPARK_MASTER) $(SPARK_LOCAL) \
		--packages $(PACKAGES_NO_KAFKA) \
		/opt/spark-apps/silver_writer.py

gold: ## Run the Gold aggregations (batch)
	$(SPARK_MASTER) $(SPARK_LOCAL) \
		--packages $(PACKAGES_NO_KAFKA) \
		/opt/spark-apps/gold_writer.py

pipeline: silver gold ## Run Silver then Gold in sequence

## ── Demo ─────────────────────────────────────────────────────────

demo: ## Full demo: start stack, register, seed, run pipeline
	@$(MAKE) up
	@sleep 60
	@$(MAKE) register
	@$(MAKE) seed
	@echo ""
	@echo "Start the Bronze writer in a separate terminal with: make bronze"
	@echo "Then run: make pipeline"

## ── Verify ───────────────────────────────────────────────────────

verify-cdc: ## Consume 5 CDC events from Kafka to verify Debezium is working
	docker exec lakehouse-kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic cdc.public.orders \
		--from-beginning \
		--max-messages 5

connector-status: ## Check Debezium connector status
	docker exec lakehouse-connect curl -s \
		http://localhost:8083/connectors/orders-cdc/status | python3 -m json.tool

## ── Help ─────────────────────────────────────────────────────────

help: ## Show this help message
	@echo ""
	@echo "Real-Time Lakehouse — available commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-20s %s\n", $$1, $$2}'
	@echo ""

.DEFAULT_GOAL := help