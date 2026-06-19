#!/usr/bin/env python3
"""Merge a captured JSONL track into the season JSON for one Friday.

Reads captures/columbia_<friday>.jsonl, cleans it (sort by time, re-downsample to
~1/min to smooth reconnect artifacts), and writes it into the matching day of
columbia_tracks_summer_2026.json, flipping that day's status to 'captured'.
Idempotent: re-running replaces the day's points rather than appending.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Slightly under 60s so genuine ~1/min points survive while reconnect-duplicate
# bursts (the capture downsampler resets its clock on every reconnect) are dropped.
DOWNSAMPLE_SECONDS = 50


def read_jsonl(path: Path) -> list[dict]:
    points = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            points.append(json.loads(line))
    return points


def clean_points(points: list[dict]) -> list[dict]:
    """Sort by timestamp and drop points within DOWNSAMPLE_SECONDS of the previous
    kept one."""
    ordered = sorted(points, key=lambda p: p["t"])
    kept: list[dict] = []
    last: datetime | None = None
    for p in ordered:
        t = datetime.fromisoformat(p["t"])
        if last is None or (t - last).total_seconds() >= DOWNSAMPLE_SECONDS:
            kept.append(p)
            last = t
    return kept


def merge_day(season: dict, friday: str, points: list[dict]) -> dict:
    """Set the matching day's points and status='captured'. friday = 'YYYY-MM-DD'.

    Replaces points (not append) so finalizing twice is idempotent.
    """
    for day in season["days"]:
        if day["friday_pt"].startswith(friday):
            day["points"] = points
            day["status"] = "captured"
            return day
    raise KeyError(f"no day in season JSON matches {friday}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--friday", required=True, help="YYYY-MM-DD of the sailing to finalize")
    ap.add_argument("--capture", default=None,
                    help="JSONL path (default: captures/columbia_<friday>.jsonl)")
    ap.add_argument("--season", default="columbia_tracks_summer_2026.json")
    args = ap.parse_args()

    capture_path = Path(args.capture or f"captures/columbia_{args.friday}.jsonl")
    if not capture_path.exists():
        sys.exit(f"capture file not found: {capture_path}")
    season_path = Path(args.season)
    season = json.loads(season_path.read_text())

    raw = read_jsonl(capture_path)
    points = clean_points(raw)
    if not points:
        sys.exit(f"{capture_path} has no points; nothing to finalize")
    day = merge_day(season, args.friday, points)
    season_path.write_text(json.dumps(season, indent=2) + "\n")
    print(f"finalized {args.friday}: {len(points)} points (from {len(raw)} raw) "
          f"-> {day['vessel']} captured")


if __name__ == "__main__":
    main()
