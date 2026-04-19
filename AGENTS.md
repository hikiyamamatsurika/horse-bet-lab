# AGENTS.md

## Project purpose
This repository is a long-term horse racing prediction and backtesting lab.

The goal is not to build the strongest model first.
The primary goal is to build a reproducible experiment platform where model / feature / strategy / period can be switched and compared under identical conditions.

## Role split
- ChatGPT: requirements clarification, design discussion, prioritization, review, and interpretation of experiment results
- Codex: implementation within the scope of the current issue
- Subagents: may be used only for narrow tasks such as investigation, impact analysis, or test perspective drafting

## Core principles
- Work on one issue at a time
- Keep changes small and reviewable
- Do not expand scope without stating it explicitly
- Prioritize reproducibility over cleverness
- Prevent future-data leakage
- Keep prediction models and betting strategies separate
- Build comparison infrastructure before pursuing stronger models

## Current priorities
1. Reproducible development environment
2. JRDB ingestion
3. Dataset generation
4. Baseline models
5. Strategy engine
6. Backtest runner
7. Metrics and reporting

## Working rules for Codex
Before implementation, always provide:
1. assumptions
2. files to change
3. short plan

After implementation, always provide:
1. what changed
2. tests run
3. unresolved risks

## Subagent policy
Subagents may be used when helpful, but only for limited tasks.
Examples:
- repository/file investigation
- impact analysis
- test perspective drafting

Rules:
- state why the subagent is being used
- keep the parent agent responsible for the final output
- do not use subagents unnecessarily for small tasks
- do not delegate design decisions or scope expansion to subagents
