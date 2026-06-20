"""Tests for the poll-position file logic (geofence, live update, season append)."""
import poll_position as pp


def _pt(lat, lon, t="2026-06-19T14:00:00-07:00", sog=10.0):
    return {"t": t, "lat": lat, "lon": lon, "sog": sog, "name": "COLUMBIA", "call": "WYR2092"}


def test_inside_box():
    assert pp.inside_box(_pt(48.72, -122.51)) is True          # terminal, inside
    assert pp.inside_box(_pt(48.64, -122.66)) is False         # SW of the box (approach)
    assert pp.inside_box(_pt(48.68, -122.62)) is True          # exactly on the SW corner


def test_update_live_sets_latest_and_appends_trail():
    live = {"latest": None, "in_box": False, "trail": []}
    pp.update_live(live, _pt(48.7, -122.5))
    assert live["latest"]["lat"] == 48.7
    assert live["updated_at"] == "2026-06-19T14:00:00-07:00"
    assert live["trail"] == [[48.7, -122.5]]


def test_update_live_caps_trail():
    live = {"trail": [[0, 0]] * pp.TRAIL_MAX}
    pp.update_live(live, _pt(48.7, -122.5))
    assert len(live["trail"]) == pp.TRAIL_MAX
    assert live["trail"][-1] == [48.7, -122.5]   # newest kept


def test_append_inbox_marks_captured():
    season = {"days": [
        {"friday_pt": "2026-06-19 00:00:00-07:00", "vessel": "COLUMBIA", "status": "scheduled", "points": []},
        {"friday_pt": "2026-06-26 00:00:00-07:00", "vessel": "COLUMBIA", "status": "scheduled", "points": []},
    ]}
    assert pp.append_inbox(season, _pt(48.72, -122.51)) is True
    assert season["days"][0]["status"] == "captured"
    assert len(season["days"][0]["points"]) == 1
    assert season["days"][1]["status"] == "scheduled"          # other day untouched


def test_append_inbox_no_matching_friday():
    season = {"days": [
        {"friday_pt": "2026-06-19 00:00:00-07:00", "vessel": "COLUMBIA", "status": "scheduled", "points": []},
    ]}
    # A Saturday point (ship mid-route) has no Friday to attach to.
    assert pp.append_inbox(season, _pt(48.72, -122.51, t="2026-06-20T09:00:00-07:00")) is False
    assert season["days"][0]["points"] == []
