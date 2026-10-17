PY ?= python
export PYTHONPATH := src

.PHONY: install test lint demo

install:
	$(PY) -m pip install -r requirements.txt

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check src tests
	$(PY) -m ruff format --check src tests

demo:
	$(PY) -m lakehouse demo
