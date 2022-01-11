SHELL=/bin/bash

help:
	@echo  'Options:'
	@echo  '  clean           		- Clean auxiliary files.'
	@echo  '  setup           		- Set up local virtual env for development.'
	@echo  '  build           		- Build and package the application and its dependencies.'
	@echo  '  test            		- Run unit tests.'
	@echo  '  execute-local       	- Run a task locally.'
	@echo  '  execute-cluster     	- Run a task on a cluster.'

clean:
	poetry run rm -rf deps/ .pytest_cache .mypy_cache *.xml .coverage dist/

setup:
	pip install poetry
	poetry config virtualenvs.in-project true --local
	poetry install

test:
	poetry run pytest -ra

build:
	rm -rf deps && mkdir deps && \
 	python -m venv .venv_build && source .venv_build/bin/activate && \
	pytest -ra && \
	pip install venv-pack && venv-pack -o deps/environment.tar.gz && \
	cp cooking_etl/main.py deps && rm -r .venv_build

execute-local:
	poetry run spark-submit --master local cooking_etl/main.py --step ${step} --input ${input} --output ${output}

execute-cluster:
	PYSPARK_PYTHON=./environment/bin/python/spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--driver-memory <value>g \
    --executor-memory <value>g \
    --executor-cores <number of cores> \
	--archives <s3 path>/environment.tar.gz#environment \
	s3://<path>/main.py \
	--step ${step} --input ${input} --output ${output}