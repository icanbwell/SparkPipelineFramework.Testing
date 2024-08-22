LANG=en_US.utf-8

export LANG

Pipfile.lock: Pipfile
	docker compose run --rm --name spf_test dev sh -c "rm -f Pipfile.lock && pipenv lock --dev"

.PHONY: install_types
install_types: Pipfile
	docker compose run --rm --name spf_test dev pipenv run mypy --install-types --non-interactive

.PHONY:devdocker
devdocker: ## Builds the docker for dev
	docker compose build

.PHONY:shell
shell:devdocker ## Brings up the bash shell in dev docker
	docker compose run --rm --name helix_shell dev /bin/bash

.PHONY:init
init: devdocker up setup-pre-commit  ## Initializes the local developer environment

.PHONY: up
up: Pipfile.lock
	docker compose up --build -d --remove-orphans
	@echo MockServer dashboard: http://localhost:1080/mockserver/dashboard
	@echo Fhir server dashboard http://localhost:3000/

.PHONY: down
down:
	docker compose down --remove-orphans && \
	docker system prune -f && \
	docker volume prune --filter label=mlflow -f

.PHONY:clean-pre-commit
clean-pre-commit: ## removes pre-commit hook
	rm -f .git/hooks/pre-commit

.PHONY:setup-pre-commit
setup-pre-commit: Pipfile.lock
	cp ./pre-commit-hook ./.git/hooks/pre-commit

.PHONY:run-pre-commit
run-pre-commit: setup-pre-commit
	./.git/hooks/pre-commit

.PHONY:update
update: down Pipfile.lock setup-pre-commit  ## Updates all the packages using Pipfile
	make devdocker && \
	make pipenv-setup && \
	make up

.PHONY:tests
tests:
	docker compose run --rm --name spf_tests dev pytest tests spf_test

.PHONY:proxies
proxies:
	docker compose run --rm --name spf_proxies dev python /SparkpipelineFramework/spf_test/proxy_generator/generate_proxies.py

.PHONY: sphinx-html
sphinx-html:
	docker compose run --rm --name spf_test dev make -C docsrc html
	@echo "copy html to docs... why? https://github.com/sphinx-doc/sphinx/issues/3382#issuecomment-470772316"
	@rm -rf docs/*
	@touch docs/.nojekyll
	cp -a docsrc/_build/html/. docs

.PHONY:pipenv-setup
pipenv-setup:devdocker ## Run pipenv-setup to update setup.py with latest dependencies
	docker compose run --rm --name spf_test dev sh -c "pipenv run pipenv install --skip-lock --categories \"pipenvsetup\" && pipenv run pipenv-setup sync --pipfile" && \
	make run-pre-commit

.PHONY: clean_data
clean_data: down ## Cleans all the local docker setup
ifneq ($(shell docker volume ls | grep "sparkpipelineframework"| awk '{print $$2}'),)
	docker volume ls | grep "sparkpipelineframework" | awk '{print $$2}' | xargs docker volume rm
endif

.PHONY:show_dependency_graph
show_dependency_graph:
	docker compose run --rm --name spf_test dev sh -c "pipenv install --skip-lock -d && pipenv graph --reverse"
	docker compose run --rm --name spf_test dev sh -c "pipenv install -d && pipenv graph"

.PHONY:build
build: ## Builds the docker for dev
	docker compose build --progress=plain --parallel

.PHONY:clean
clean: down
	find . -type d -name "__pycache__" | xargs rm -r
	find . -type d -name "metastore_db" | xargs rm -r
	find . -type f -name "derby.log" | xargs rm -r
	find . -type d -name "temp" | xargs rm -r
