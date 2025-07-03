
.DEFAULT_GOAL := help
include .env

define HELP

Available commands:

- run: This will call core.

- stop: Stop the project.
- start: Start a stopped project.
- down: Tear down the project, clean directories.
- ps: Show all running containers.

- logs: Show/tail logs.
- logsf: Stream logs.
- watch: Watch logs.

- fsql: Open Flink Jobmanager and start sql-client.
- jm: Open a bash terminal inside Jobmanager.

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help


run: 
	docker compose -p pipeline up -d \
		jobmanager taskmanager \
		 --remove-orphans 

stop:
	docker compose stop

start:
	docker compose start

down:	
	docker compose down
	cd data/; rm -rf flink

ps:
	docker compose ps

logs:
	docker compose logs

logsf:
	docker compose logs -f

watch:
	watch docker compose ps

fsql:
	docker compose exec jobmanager /opt/flink/bin/sql-client.sh

jm:
	docker compose exec jobmanager /bin/bash
