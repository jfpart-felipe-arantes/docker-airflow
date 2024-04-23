.DEFAULT_GOAL := build
build:
	nerdctl build -t ppi_airflow_img_dev:1.0 .
up: celery-up

celery-up:
	nerdctl compose -f docker-compose-CeleryExecutor.yml up -d

celery-down:
	nerdctl compose -f docker-compose-CeleryExecutor.yml down

down:
	nerdctl compose -f docker-compose-CeleryExecutor.yml down
	nerdctl compose -f docker-compose-LocalExecutor.yml down
