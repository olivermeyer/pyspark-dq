.PHONY: help

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

setup-dev: venv deps dev-deps  ## Create a virtualenv with all requirements

clean-dev:  ## Clean the virtualenv
	rm -r venv/

venv:
	virtualenv -p python3.7 venv

deps:
	venv/bin/pip install -r requirements.txt

dev-deps:
	venv/bin/pip install -r test-requirements.txt

test: test-unit test-integration  ## Run the test suite

test-unit:
	@echo "Running unit tests"
	pytest tests/unit/ --disable-warnings

test-integration:
	@echo "Running integration tests"
	pytest tests/integration/ --disable-warnings

build:  ## Package and push to PyPi
	python setup.py sdist bdist_wheel
	twine upload dist/*

clean-build:  ## Clean distribution directories
	rm -rf dist/
	rm -rf build/
