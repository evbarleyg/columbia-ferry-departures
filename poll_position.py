#!/usr/bin/env python3
"""Poll the Columbia's current position once and update the live-data files.

Run by the 5-minute GitHub Action. Operates on two files in --data-dir (carried
across runs on the rolling `live-data` branch):
  - live_position.json  : {updated_at, in_box, latest, trail[]}  -> the live page
  - columbia_tracks_summer_2026.json : season tracks             -> the explorer

Each run does ONE aisstream locate, then:
  - updates the live position + trail,
  - if the point is inside the Bellingham box, appends it to that Friday's track
    (status -> captured) — the same geofenced path the source recorded, at 5-min grain,
  - on box ENTRY, pings ntfy (if NTFY_TOPIC is set).

Stateless across runs — all state lives in the two files. The aisstream key comes
from $AISSTREAM_API_KEY (a GitHub secret) and never reaches a browser.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import urllib.request
from pathlib import Path

import websockets

from build_season_skeleton import build_skeleton
from capture_columbia import (
    COLUMBIA_MMSI, SSL_CONTEXT, WS_URL, WIDE_BBOX, build_subscription, message_to_point,
)

# Bellingham geofence — matches the 2025/2026 meta box.
LAT_MIN, LAT_MAX = 48.68, 48.78
LON_MIN, LON_MAX = -122.62, -122.45
TRAIL_MAX = 300
LOCATE_TIMEOUT = 45


def inside_box(point: dict) -> bool:
    return LAT_MIN <= point["lat"] <= LAT_MAX and LON_MIN <= point["lon"] <= LON_MAX


def update_live(live: dict, point: dict) -> dict:
    live["latest"] = point
    live["updated_at"] = point["t"]
    trail = live.setdefault("trail", [])
    trail.append([point["lat"], point["lon"]])
    if len(trail) > TRAIL_MAX:
        del trail[:-TRAIL_MAX]
    return live


def append_inbox(season: dict, point: dict) -> bool:
    """Append the point to its Friday's track (status -> captured). Returns True if
    a matching Friday existed."""
    friday = point["t"][:10]
    for day in season["days"]:
        if day["friday_pt"].startswith(friday):
            day["points"].append({k: point[k] for k in ("t", "lat", "lon", "sog", "name", "call")})
            day["status"] = "captured"
            return True
    return False


def ntfy(text: str) -> None:
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        return
    try:
        req = urllib.request.Request(
            f"https://ntfy.sh/{topic}", data=text.encode(),
            headers={"Title": "MV Columbia", "Tags": "ship"})
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:  # a failed ping must never fail the run
        print("ntfy failed:", e, flush=True)


async def locate_once(api_key: str, timeout: int = LOCATE_TIMEOUT) -> dict | None:
    """Connect, return the first Columbia PositionReport, then disconnect."""
    sub = build_subscription(api_key, WIDE_BBOX)

    async def first_point(ws):
        async for raw in ws:
            point = message_to_point(json.loads(raw))
            if point:
                return point

    try:
        async with websockets.connect(WS_URL, ssl=SSL_CONTEXT, ping_interval=20) as ws:
            await ws.send(json.dumps(sub))
            return await asyncio.wait_for(first_point(ws), timeout=timeout)
    except (asyncio.TimeoutError, OSError, websockets.WebSocketException) as e:
        print("locate failed/timed out:", e, flush=True)
        return None


def load_json(path: Path, default):
    return json.loads(path.read_text()) if path.exists() else default


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data")
    args = ap.parse_args()

    api_key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    if not api_key:
        sys.exit("AISSTREAM_API_KEY not set")

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    live_path = data_dir / "live_position.json"
    season_path = data_dir / "columbia_tracks_summer_2026.json"
    live = load_json(live_path, {"latest": None, "in_box": False, "trail": []})
    season = load_json(season_path, build_skeleton())

    point = asyncio.run(locate_once(api_key))
    if not point:
        print("no position this run; leaving files unchanged", flush=True)
        return

    was_in_box = bool(live.get("in_box"))
    here = inside_box(point)
    update_live(live, point)
    live["in_box"] = here
    if here:
        append_inbox(season, point)
    if here and not was_in_box:
        ntfy(f"Columbia entered the Bellingham box at {point['t'][11:16]} PT ({point['sog']} kn).")

    live_path.write_text(json.dumps(live, indent=2) + "\n")
    season_path.write_text(json.dumps(season, indent=2) + "\n")
    print(f"{point['t']} {point['lat']},{point['lon']} sog={point['sog']} in_box={here}", flush=True)


if __name__ == "__main__":
    main()
