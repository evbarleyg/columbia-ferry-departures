#!/usr/bin/env python3
"""Generate the season-skeleton columbia_tracks_summer_2026.json.

One-time bootstrap: lays out every summer-2026 Friday on the Bellingham mainline
slot with its scheduled vessel and an initial status. Capture + finalize_day.py
later flip 'scheduled' days to 'captured' and fill points; the deferred NOAA
backfill flips the 'pending' Kennicott days. Refuses to overwrite an existing
file unless --force, so it can never clobber captured data.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

SEASON_START = date(2026, 6, 5)
SEASON_END = date(2026, 8, 28)
PT_OFFSET = "-07:00"  # PDT — in effect for the whole summer-2026 season.

# Relief vessel (Kennicott) Fridays — the two already-passed runs awaiting NOAA.
KENNICOTT_FRIDAYS = {date(2026, 6, 5), date(2026, 6, 12)}

VESSELS = [
    {"name": "COLUMBIA", "mmsi": 367144000, "call": "WYR2092", "role": "primary"},
    {"name": "KENNICOTT", "mmsi": None, "call": None, "role": "relief"},
]

OUT_PATH = "columbia_tracks_summer_2026.json"


def fridays(start: date, end: date) -> list[date]:
    """Every Friday in [start, end] inclusive."""
    d = start + timedelta(days=(4 - start.weekday()) % 7)  # first Friday on/after start
    out = []
    while d <= end:
        out.append(d)
        d += timedelta(days=7)
    return out


def build_skeleton() -> dict:
    days = []
    for d in fridays(SEASON_START, SEASON_END):
        relief = d in KENNICOTT_FRIDAYS
        days.append({
            "friday_pt": f"{d.isoformat()} 00:00:00{PT_OFFSET}",
            "vessel": "KENNICOTT" if relief else "COLUMBIA",
            "status": "pending" if relief else "scheduled",
            "points": [],
        })
    return {
        "meta": {
            "season": 2026,
            "vessels": VESSELS,
            "lat_min": 48.68,
            "lat_max": 48.78,
            "lon_min": -122.62,
            "lon_max": -122.45,
            "timezone": "America/Los_Angeles",
            "note": "AIS points for the Bellingham mainline Friday slot, summer 2026; downsampled ~1/min.",
        },
        "days": days,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=OUT_PATH)
    ap.add_argument("--force", action="store_true",
                    help="overwrite even if the file exists (DANGER: wipes captured data)")
    args = ap.parse_args()
    path = Path(args.out)
    if path.exists() and not args.force:
        sys.exit(f"{path} already exists; refusing to overwrite (would wipe captured data). "
                 "Use --force to override.")
    path.write_text(json.dumps(build_skeleton(), indent=2) + "\n")
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
