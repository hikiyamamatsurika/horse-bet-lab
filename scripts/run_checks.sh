#!/usr/bin/env bash

set -eu

.venv/bin/python -m ruff check .
.venv/bin/python -m mypy .
PYTHONPATH=src .venv/bin/python -m pytest
