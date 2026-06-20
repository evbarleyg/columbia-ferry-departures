"""Tests for the season-skeleton generator."""
import build_season_skeleton as sk


def test_skeleton_has_13_summer_fridays_in_order():
    days = sk.build_skeleton()["days"]
    assert len(days) == 13
    assert days[0]["friday_pt"].startswith("2026-06-05")
    assert days[-1]["friday_pt"].startswith("2026-08-28")


def test_kennicott_relief_days_are_pending():
    by_date = {d["friday_pt"][:10]: d for d in sk.build_skeleton()["days"]}
    for day in ("2026-06-05", "2026-06-12"):
        assert by_date[day]["vessel"] == "KENNICOTT"
        assert by_date[day]["status"] == "pending"


def test_columbia_days_are_scheduled():
    by_date = {d["friday_pt"][:10]: d for d in sk.build_skeleton()["days"]}
    assert by_date["2026-06-19"]["vessel"] == "COLUMBIA"
    assert by_date["2026-06-19"]["status"] == "scheduled"
    assert by_date["2026-08-28"]["vessel"] == "COLUMBIA"
    assert by_date["2026-08-28"]["status"] == "scheduled"


def test_meta_lists_both_vessels_and_geofence():
    meta = sk.build_skeleton()["meta"]
    assert {v["name"] for v in meta["vessels"]} == {"COLUMBIA", "KENNICOTT"}
    columbia = next(v for v in meta["vessels"] if v["name"] == "COLUMBIA")
    assert columbia["mmsi"] == 367144000 and columbia["role"] == "primary"
    kennicott = next(v for v in meta["vessels"] if v["name"] == "KENNICOTT")
    assert kennicott["mmsi"] is None and kennicott["role"] == "relief"
    assert meta["lat_min"] == 48.68 and meta["lon_max"] == -122.45


def test_all_skeleton_days_start_empty():
    assert all(d["points"] == [] for d in sk.build_skeleton()["days"])
