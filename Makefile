.DEFAULT_GOAL := build
build:
	docker build -t ppi_airflow_img_dev:1.0 .

up: celery-up
down:
	docker compose -f docker-compose-CeleryExecutor.yml down
	docker compose -f docker-compose-LocalExecutor.yml down

celery-up:
	docker compose -f docker-compose-CeleryExecutor.yml up
celery-down:
	docker compose -f docker-compose-CeleryExecutor.yml down

local-up:
	docker compose -f docker-compose-LocalExecutor.yml up
local-down:
	docker compose -f docker-compose-LocalExecutor.yml down
