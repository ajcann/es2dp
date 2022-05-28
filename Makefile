sources = es2dp

.PHONY: test format lint unittest

test: format lint unittest

format:
	poetry run isort $(sources) tests
	poetry run black $(sources) tests

lint:
	poetry run flake8 $(sources) tests
	poetry run mypy $(sources) tests

unittest:
	poetry run pytest
