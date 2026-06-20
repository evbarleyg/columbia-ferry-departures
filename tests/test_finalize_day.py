"""Tests for finalizing a captured JSONL track into the season JSON."""
import pytest

import finalize_day as fd


def _pt(t, sog=8.0):
    return {"t": t, "lat": 48.7, "lon": -122.5, "sog": sog, "name": "COLUMBIA", "call": "WYR2092"}


def test_clean_points_sorts_then_downsamples():
    pts = [
        _pt("2026-06-19T21:45:30-07:00"),  # 30s after the 21:45:00 point -> dropped
        _pt("2026-06-19T21:45:00-07:00"),  # earliest, out of order in input
        _pt("2026-06-19T21:46:05-07:00"),  # 65s after 21:45:00 -> kept
    ]
    assert [p["t"] for p in fd.clean_points(pts)] == [
        "2026-06-19T21:45:00-07:00",
        "2026-06-19T21:46:05-07:00",
    ]


def test_clean_points_empty():
    assert fd.clean_points([]) == []


def test_merge_day_flips_status_and_sets_points():
    season = {"days": [
        {"friday_pt": "2026-06-19 00:00:00-07:00", "vessel": "COLUMBIA", "status": "scheduled", "points": []},
        {"friday_pt": "2026-06-26 00:00:00-07:00", "vessel": "COLUMBIA", "status": "scheduled", "points": []},
    ]}
    pts = [_pt("2026-06-19T21:45:00-07:00")]
    day = fd.merge_day(season, "2026-06-19", pts)
    assert day["status"] == "captured"
    assert day["points"] == pts
    assert season["days"][1]["status"] == "scheduled"  # neighbouring day untouched


def test_merge_day_unknown_date_raises():
    season = {"days": [
        {"friday_pt": "2026-06-19 00:00:00-07:00", "vessel": "COLUMBIA", "status": "scheduled", "points": []},
    ]}
    with pytest.raises(KeyError):
        fd.merge_day(season, "2026-07-04", [])


def test_merge_day_is_idempotent_replacing_not_appending():
    season = {"days": [
        {"friday_pt": "2026-06-19 00:00:00-07:00", "vessel": "COLUMBIA", "status": "scheduled", "points": []},
    ]}
    fd.merge_day(season, "2026-06-19", [_pt("2026-06-19T21:45:00-07:00")])
    second = [_pt("2026-06-19T21:50:00-07:00")]
    fd.merge_day(season, "2026-06-19", second)
    assert season["days"][0]["points"] == second
