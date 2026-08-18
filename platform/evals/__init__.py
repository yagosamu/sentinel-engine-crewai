"""Runnable evals (bash + python).

Assert each injected defect was CAUGHT: present in ``silver.<entity>_rejects``
WHERE reject_rule = '<failure_key>' and ABSENT from gold, joined to the
ground-truth ``injected_incidents.failure_key`` in Postgres.
"""
