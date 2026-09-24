import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.decision_lab.models import Evidence, Dataset, Session, Experiment, canonical, digest


T = datetime(2026, 3, 3, 20, tzinfo=timezone.utc)


def ev(kind, payload, *, at=T, available=None, quality="POINT_IN_TIME_SAFE", owner=None, rid=None, revision=None):
    return Evidence(kind=kind, record_id=rid or digest([kind, payload, at.isoformat()]),
                    effective_at=at, available_at=available or at, revision_at=revision,
                    source="fixture", quality=quality, owner=owner, payload_json=canonical(payload))


def test_evidence_deep_freeze_and_hash():
    e = ev("MACRO", {"vix": 17})
    original = e.content_hash
    e.payload["vix"] = 99
    assert e.payload["vix"] == 17 and e.content_hash == original
    with pytest.raises(ValueError):
        e.source = "changed"
    with pytest.raises(ValueError):
        ev("MACRO", {"vix": float("nan")})


def test_timezone_and_revision_availability():
    with pytest.raises(ValueError):
        ev("MACRO", {}, at=T.replace(tzinfo=None))
    e = ev("MACRO", {}, revision=T+timedelta(days=1))
    assert not e.known_at(T)


def test_duplicate_evidence_versions_fail_instead_of_last_write_wins():
    e = ev("MACRO", {})
    with pytest.raises(ValueError, match="duplicate evidence"):
        Dataset(records=(e,e), sessions=(), calendar_version="fixture")


def test_preregistration_cannot_follow_holdout_or_overlap_training():
    with pytest.raises(ValueError, match="preregistered"):
        Experiment(experiment_id="e", registered_at=T, confirmatory=True, family=("DVA20",), evaluation_start=T)
    with pytest.raises(ValueError, match="strictly ordered"):
        Experiment(experiment_id="e", registered_at=T, train_end=T, evaluation_start=T)
