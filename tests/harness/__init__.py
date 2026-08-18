"""Tests for the C4h peak-load harness, the AC-1 isolation gate.

Verified by assertion against the harness verdict: the pure decision rule
(``test_verdict``) plus one live true-positive that a tagged ingest session
holding a lock on the OLTP path is flagged analytics-attributable
(``test_lockwait_detection``). See ``tests/README.md`` for the run matrix.
"""

