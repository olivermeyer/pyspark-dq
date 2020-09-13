setup: clean venv deps dev-deps

clean:
	rm -r venv/

venv:
	virtualenv -p python3.7 venv

deps:
	venv/bin/pip install -r requirements.txt

dev-deps:
	venv/bin/pip install -r test-requirements.txt

test:
	pytest tests/

dist:
	python setup.py sdist bdist_wheel
	twine upload --repository pypi dist/*
