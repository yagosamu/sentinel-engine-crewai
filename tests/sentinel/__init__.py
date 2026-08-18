"""Tests for Component B (the Sentinel crew), verified by scoring vs the I4 oracle.

These never assert that a diagnosis is "correct"; they score the crew's output
against the ``injected_incidents`` ground truth (R5). The suite is layered offline
first: pure/mocked -> StubLLM wiring (plumbing only) -> live-LLM scorecard (the
only API-key-gated module). See ``tests/README.md`` for the run matrix.
"""
