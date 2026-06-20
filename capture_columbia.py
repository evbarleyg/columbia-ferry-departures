#!/usr/bin/env python3
"""Capture the MV Columbia's AIS track from aisstream.io into a crash-safe JSONL file.

Capture mode (default):
    Subscribe to the Bellingham terminal geofence, filtered to Columbia's MMSI,
    and append one downsampled point (~1/min) per line to
    captures/columbia_<friday>.jsonl as it streams. Append-only + fsync, so a
    crash or disconnect loses nothing. Reconnects automatically.

Locate mode (--locate):
    Subscribe to a wide West-Coast/Alaska box for Columbia's MMSI, print the
    first few live positions, and exit. Use it to validate the API key and to
    see where the Columbia is *right now*.

The API key is read from $AISSTREAM_API_KEY, or from a gitignored key file
(default: aisstream_key.txt). Never pass the key on the command line or in chat.

Each captured point matches the 2025 dataset schema exactly:
    {"t": ISO8601 Pacific, "lat", "lon", "sog", "name", "call"}
The JSONL is the raw capture; finalize_day.py (separate task) downsamples/merges
it into the season JSON.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import ssl
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import certifi
import websockets

WS_URL = "wss://stream.aisstream.io/v0/stream"
PT = ZoneInfo("America/Los_Angeles")

# python.org's framework Python ships no CA bundle, so the stdlib ssl module
# can't verify any TLS cert. Point TLS at certifi's bundle — self-contained,
# no changes to the system Python.
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

# MV Columbia — AMHS flagship. MMSI + call sign taken from the 2025 dataset.
COLUMBIA_MMSI = "367144000"
COLUMBIA_CALL = "WYR2092"

# Bellingham terminal geofence — reused verbatim from the 2025 dataset meta.
# aisstream box format: [[[lat, lon], [lat, lon]]]  (two opposite corners).
BELLINGHAM_BBOX = [[[48.68, -122.62], [48.78, -122.45]]]
# Wide box for --locate: entire US West Coast + Alaska, so we find the ship
# wherever it is. (MMSI filter means only Columbia's messages arrive anyway.)
WIDE_BBOX = [[[40.0, -150.0], [62.0, -118.0]]]

DOWNSAMPLE_SECONDS = 60
LOCATE_POINTS = 1
LOCATE_TIMEOUT_SECONDS = 45


def log(message: str) -> None:
    """Progress to stderr, stamped in Pacific so it lines up with the data."""
    stamp = datetime.now(PT).strftime("%H:%M:%S")
    print(f"[{stamp} PT] {message}", file=sys.stderr, flush=True)


def load_api_key(key_file: str) -> str:
    key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    if key:
        return key
    path = Path(key_file)
    if path.exists():
        key = path.read_text().strip()
        if key:
            return key
    sys.exit(
        f"No API key found. Set $AISSTREAM_API_KEY or put your key in "
        f"{key_file!r} (one line, no quotes)."
    )


def parse_time_utc(time_utc: str) -> datetime:
    """aisstream MetaData.time_utc -> aware Pacific datetime.

    The field looks like '2026-06-19 14:46:10.123456789 +0000 UTC'; we take the
    leading 'YYYY-MM-DD HH:MM:SS' and treat it as UTC.
    """
    base = time_utc[:19]
    dt = datetime.strptime(base, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.astimezone(PT)


def message_to_point(msg: dict) -> dict | None:
    """Convert an aisstream PositionReport envelope to a 2025-schema track point.

    Returns None for non-position messages or reports without coordinates.
    """
    if msg.get("MessageType") != "PositionReport":
        return None
    meta = msg.get("MetaData", {}) or {}
    report = (msg.get("Message", {}) or {}).get("PositionReport", {}) or {}
    lat = report.get("Latitude", meta.get("latitude"))
    lon = report.get("Longitude", meta.get("longitude"))
    if lat is None or lon is None:
        return None
    sog = report.get("Sog")
    name = (meta.get("ShipName") or "COLUMBIA").strip()
    return {
        "t": parse_time_utc(meta.get("time_utc", "")).isoformat(),
        "lat": round(float(lat), 5),
        "lon": round(float(lon), 5),
        "sog": float(sog) if sog is not None else None,
        "name": name,
        "call": COLUMBIA_CALL,
    }


class Downsampler:
    """Keep at most one point per `seconds`, judged by the point's own timestamp."""

    def __init__(self, seconds: int = DOWNSAMPLE_SECONDS):
        self.seconds = seconds
        self.last_kept: datetime | None = None

    def keep(self, dt: datetime) -> bool:
        if self.last_kept is None or (dt - self.last_kept).total_seconds() >= self.seconds:
            self.last_kept = dt
            return True
        return False


def append_jsonl(path: Path, point: dict) -> None:
    """Append one point as a JSON line, flushed + fsynced so a crash loses nothing."""
    with path.open("a") as f:
        f.write(json.dumps(point) + "\n")
        f.flush()
        os.fsync(f.fileno())


def build_subscription(api_key: str, bbox: list) -> dict:
    return {
        "APIKey": api_key,
        "BoundingBoxes": bbox,
        "FiltersShipMMSI": [COLUMBIA_MMSI],
        "FilterMessageTypes": ["PositionReport"],
    }


async def run(api_key: str, bbox: list, out_path: Path | None, locate: bool) -> None:
    sub = build_subscription(api_key, bbox)
    downsampler = Downsampler()
    seen = kept = 0
    mode = "locate" if locate else "capture"

    async for ws in websockets.connect(WS_URL, ssl=SSL_CONTEXT, ping_interval=20, ping_timeout=20):
        try:
            await ws.send(json.dumps(sub))
            log(f"connected + subscribed ({mode}); waiting for Columbia ({COLUMBIA_MMSI})...")
            async for raw in ws:
                point = message_to_point(json.loads(raw))
                if point is None:
                    continue
                seen += 1
                if locate:
                    print(json.dumps(point, indent=2))
                    if seen >= LOCATE_POINTS:
                        log(f"got {seen} position(s); done.")
                        return
                    continue
                if downsampler.keep(datetime.fromisoformat(point["t"])):
                    append_jsonl(out_path, point)
                    kept += 1
                    log(
                        f"kept {kept} (seen {seen})  {point['t']}  "
                        f"sog={point['sog']}kn @ {point['lat']},{point['lon']}"
                    )
        except websockets.ConnectionClosed:
            log("connection closed; reconnecting...")
            continue


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--locate", action="store_true",
                        help="find Columbia's current position anywhere and exit (validation)")
    parser.add_argument("--key-file", default="aisstream_key.txt",
                        help="gitignored file holding the API key (default: aisstream_key.txt)")
    parser.add_argument("--out-dir", default="captures", help="where JSONL captures are written")
    parser.add_argument("--friday", default=None,
                        help="YYYY-MM-DD label for the output file (default: today in Pacific)")
    args = parser.parse_args()

    api_key = load_api_key(args.key_file)

    if args.locate:
        try:
            asyncio.run(asyncio.wait_for(run(api_key, WIDE_BBOX, None, locate=True),
                                         timeout=LOCATE_TIMEOUT_SECONDS))
        except asyncio.TimeoutError:
            log(f"No Columbia position in {LOCATE_TIMEOUT_SECONDS}s. It may be outside "
                "terrestrial AIS range (open Gulf of Alaska) or its transponder is off. "
                "If you saw 'connected + subscribed', the key and connection are fine.")
        return

    friday = args.friday or datetime.now(PT).strftime("%Y-%m-%d")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"columbia_{friday}.jsonl"
    log(f"capturing Bellingham box -> {out_path}   (Ctrl-C to stop)")
    try:
        asyncio.run(run(api_key, BELLINGHAM_BBOX, out_path, locate=False))
    except KeyboardInterrupt:
        log("stopped.")


if __name__ == "__main__":
    main()
