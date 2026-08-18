"""Pytest suite for the whole platform: three suites, two verification models.

Root ``tests/*.py`` cover Component A (the analytical backbone) and are verified
by assertion (R5). ``tests/sentinel/`` covers Component B (the Sentinel crew),
verified by scoring against the I4 ground-truth oracle. ``tests/harness/`` covers
the C4h peak-load harness, the AC-1 isolation gate. See ``tests/README.md`` for
the per-module run matrix, gating flags, and the offline stub-LLM vs live-eval
ladder.
"""
