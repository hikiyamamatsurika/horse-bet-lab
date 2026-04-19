.PHONY: test lint typecheck run ingest dataset model model-compare evaluate place-backtest check

test:
	PYTHONPATH=src .venv/bin/python -m pytest

lint:
	.venv/bin/python -m ruff check .

typecheck:
	.venv/bin/python -m mypy .

run:
	PYTHONPATH=src .venv/bin/python -m horse_bet_lab.runner --config configs/default.toml

ingest:
	PYTHONPATH=src .venv/bin/python -m horse_bet_lab.ingest.cli

dataset:
	PYTHONPATH=src .venv/bin/python -m horse_bet_lab.dataset.cli --config configs/dataset_minimal.toml

model:
	PYTHONPATH=src .venv/bin/python -m horse_bet_lab.model.cli --config configs/model_odds_only_logreg_is_place.toml

model-compare:
	PYTHONPATH=src .venv/bin/python -m horse_bet_lab.model.comparison_cli --config configs/model_market_feature_comparison_2024_2025.toml

evaluate:
	PYTHONPATH=src .venv/bin/python -m horse_bet_lab.evaluation.cli --config configs/bet_eval_odds_log1p_plus_popularity.toml

place-backtest:
	PYTHONPATH=src .venv/bin/python -m horse_bet_lab.evaluation.place_cli --config configs/place_backtest_odds_log1p_plus_popularity.toml

check:
	PYTHONPATH=src .venv/bin/python -m pytest
	.venv/bin/python -m ruff check .
	.venv/bin/python -m mypy .
