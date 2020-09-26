setup-dev: venv deps dev-deps

clean-dev:
	rm -r venv/

venv:
	virtualenv -p python3.7 venv

deps:
	venv/bin/pip install -r requirements.txt

dev-deps:
	venv/bin/pip install -r test-requirements.txt

test: test-unit test-integration

test-unit:
	@echo "Running unit tests"
	pytest tests/unit/ --disable-warnings

test-integration:
	@echo "Running integration tests"
	pytest tests/integration/ --disable-warnings

build:
	python setup.py sdist bdist_wheel
	twine upload dist/*

clean-build:
	rm -rf dist/
	rm -rf build/
