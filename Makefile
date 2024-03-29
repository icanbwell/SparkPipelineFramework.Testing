LANG=en_US.utf-8

export LANG

BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
VERSION=$(shell cat VERSION)
VENV_NAME=venv
GIT_HASH=${CIRCLE_SHA1}
SPARK_VER=3.1.1
HADOOP_VER=3.2
PACKAGES_FOLDER=/usr/local/lib/python3.7/dist-packages
SPF_BASE=${PACKAGES_FOLDER}

Pipfile.lock: Pipfile
	docker-compose run --rm --name spftest_pip dev sh -c "rm -f Pipfile.lock && pipenv lock --dev"

.PHONY:devdocker
devdocker: ## Builds the docker for dev
	docker-compose build

.PHONY:shell
shell:devdocker ## Brings up the bash shell in dev docker
	docker-compose run --rm --name spf_test_shell dev /bin/bash

.PHONY:init
init: devdocker up setup-pre-commit  ## Initializes the local developer environment

.PHONY: up
up: Pipfile.lock
	docker-compose up --build -d --remove-orphans
	@echo MockServer dashboard: http://localhost:1080/mockserver/dashboard
	@echo Fhir server dashboard http://localhost:3000/

.PHONY: down
down:
	docker-compose down --remove-orphans && \
	docker system prune -f

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
	docker-compose run --rm --name spftest_pipenv dev pipenv sync --dev && \
	make devdocker


.PHONY:tests
tests: up
	docker-compose run --rm --name spftest_tests dev pytest tests library

.PHONY:proxies
proxies:devdocker ## Generates proxies for all the library transformers, auto mappers and pipelines
	docker-compose run --rm --name helix_proxies dev \
	python ${SPF_BASE}/spark_pipeline_framework/proxy_generator/generate_proxies.py

.PHONY:continuous_integration
continuous_integration: venv
	. $(VENV_NAME)/bin/activate && \
	pip install --upgrade pip && \
    pip install --upgrade -r requirements.txt && \
    pip install --upgrade -r requirements-test.txt && \
    python setup.py install && \
    pre-commit run --all-files && \
    pytest tests

.PHONY: sphinx-html
sphinx-html:
	docker-compose run --rm --name sam_fhir dev make -C docsrc html
	@echo "copy html to docs... why? https://github.com/sphinx-doc/sphinx/issues/3382#issuecomment-470772316"
	@rm -rf docs
	@mkdir docs
	@touch docs/.nojekyll
	cp -a docsrc/_build/html/. docs

.DEFAULT_GOAL := help
.PHONY: help
help: ## Show this help.
	# from https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: clean_data
clean_data: down ## Cleans all the local docker setup
ifneq ($(shell docker volume ls | grep "sparkpipelineframeworktesting"| awk '{print $$2}'),)
	docker volume ls | grep "sparkpipelineframeworktesting" | awk '{print $$2}' | xargs docker volume rm
endif

.PHONY:pipenv-setup
pipenv-setup:devdocker ## Brings up the bash shell in dev docker
	docker-compose run --rm --name spf_test dev pipenv-setup sync --pipfile

.PHONY:show_dependency_graph
show_dependency_graph:
	docker-compose run --rm --name spf_test dev sh -c "pipenv install --skip-lock && pipenv graph --reverse"
	docker-compose run --rm --name spf_test dev sh -c "pipenv install -d && pipenv graph"

.PHONY:qodana
qodana:
	docker run --rm -it --name qodana --mount type=bind,source="${PWD}",target=/data/project -p 8080:8080 jetbrains/qodana-python:2022.3-eap --show-report
