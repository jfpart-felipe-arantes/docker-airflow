.DEFAULT_GOAL := build
build:
	docker build -t ppi_airflow_img_dev:1.0 .
up: celery-up
celery-up:
	docker compose -f docker-compose-CeleryExecutor.yml up -d

celery-down:
	docker compose -f docker-compose-CeleryExecutor.yml down

down:
	docker compose -f docker-compose-CeleryExecutor.yml down
	docker compose -f docker-compose-LocalExecutor.yml down
