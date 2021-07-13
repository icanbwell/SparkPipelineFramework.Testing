LANG=en_US.utf-8

export LANG

BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
VERSION=$(shell cat VERSION)
VENV_NAME=venv
GIT_HASH=${CIRCLE_SHA1}
SPARK_VER=3.1.1
HADOOP_VER=3.2
PACKAGES_FOLDER=venv/lib/python3.6/site-packages
SPF_BASE=${PACKAGES_FOLDER}

Pipfile.lock: Pipfile
	docker-compose run --rm --name spftest_pip dev pipenv lock --dev

.PHONY:devdocker
devdocker: ## Builds the docker for dev
	docker-compose build

.PHONY:init
init: devdocker up setup-pre-commit  ## Initializes the local developer environment

.PHONY: up
up: Pipfile.lock
	docker-compose up --build -d --remove-orphans

.PHONY: down
down:
	docker-compose down

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
tests:
	docker-compose run --rm --name spftest_tests dev pytest tests

.PHONY:proxies
proxies:
	. ${VENV_NAME}/bin/activate && \
	python3 ${PACKAGES_FOLDER}/spark_pipeline_framework/proxy_generator/generate_proxies.py

.PHONY:continuous_integration
continuous_integration: venv
	. $(VENV_NAME)/bin/activate && \
	pip install --upgrade pip && \
    pip install --upgrade -r requirements.txt && \
    pip install --upgrade -r requirements-test.txt && \
    python setup.py install && \
    pre-commit run --all-files && \
    pytest tests
