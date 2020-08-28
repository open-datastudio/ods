init:
	pip install -r requirements.txt

test:
	python -m unittest

dist:
	python setup.py sdist

publish:
	twine upload dist/*

.PHONY: init test
