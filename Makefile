.PHONY: check format lint type-check test

setup:
	bash scripts/setup.sh

install-pre-commit:
	pre-commit install

check: format lint type-check test

format:
	black .

lint:
	ruff check .

type-check:
	mypy . --ignore-missing-imports

test:
	pytest
