"""Unit tests for the pure logic in capture_columbia.

The websocket I/O is integration-tested live (--locate); here we lock down the
parts where a silent bug would quietly corrupt a capture: timestamp conversion,
schema mapping, and downsampling.
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import capture_columbia as cap

PT = ZoneInfo("America/Los_Angeles")


def test_parse_time_utc_converts_to_pacific():
    dt = cap.parse_time_utc("2026-06-19 14:46:10.123456789 +0000 UTC")
    # 14:46 UTC is 07:46 PDT (UTC-7 in summer).
    assert dt.isoformat() == "2026-06-19T07:46:10-07:00"


def test_message_to_point_maps_2025_schema():
    msg = {
        "MessageType": "PositionReport",
        "MetaData": {
            "MMSI": 367144000,
            "ShipName": "COLUMBIA            ",  # AIS pads names with spaces
            "time_utc": "2026-06-19 02:05:52.0 +0000 UTC",
        },
        "Message": {"PositionReport": {"Latitude": 48.68098, "Longitude": -122.55444, "Sog": 13.5}},
    }
    assert cap.message_to_point(msg) == {
        "t": "2026-06-18T19:05:52-07:00",  # 02:05 UTC -> 19:05 previous-day PDT
        "lat": 48.68098,
        "lon": -122.55444,
        "sog": 13.5,
        "name": "COLUMBIA",
        "call": "WYR2092",
    }


def test_message_to_point_ignores_non_position_messages():
    assert cap.message_to_point({"MessageType": "ShipStaticData", "MetaData": {}, "Message": {}}) is None


def test_message_to_point_ignores_report_without_coords():
    msg = {"MessageType": "PositionReport", "MetaData": {"time_utc": "2026-06-19 02:05:52 +0000 UTC"},
           "Message": {"PositionReport": {"Sog": 0.0}}}
    assert cap.message_to_point(msg) is None


def test_downsampler_keeps_one_per_minute():
    ds = cap.Downsampler(seconds=60)
    t0 = datetime(2026, 6, 19, 21, 45, 0, tzinfo=PT)
    assert ds.keep(t0) is True              # first point always kept
    assert ds.keep(t0 + timedelta(seconds=30)) is False   # too soon
    assert ds.keep(t0 + timedelta(seconds=61)) is True     # past the interval


def test_build_subscription_shape():
    sub = cap.build_subscription("KEY123", cap.BELLINGHAM_BBOX)
    assert sub["APIKey"] == "KEY123"
    assert sub["FiltersShipMMSI"] == ["367144000"]
    assert sub["FilterMessageTypes"] == ["PositionReport"]
    assert sub["BoundingBoxes"] == [[[48.68, -122.62], [48.78, -122.45]]]
