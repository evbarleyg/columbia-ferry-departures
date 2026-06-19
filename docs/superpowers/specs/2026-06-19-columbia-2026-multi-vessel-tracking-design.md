# Columbia Ferry Explorer — 2026 Multi-Vessel Tracking

**Date:** 2026-06-19
**Status:** Approved design, pre-implementation

## Overview

The existing tool (`index.html` + `columbia_tracks_summer_fridays_2025.json`) is a static
Leaflet explorer of the MV Columbia's summer-2025 **Friday departures** from the Bellingham,
WA terminal. It assumes **one vessel, one finished season**: every Friday is a completed
Columbia departure, colored by departure time.

Summer 2026 breaks both assumptions:

1. **Multiple vessels** cover the Bellingham mainline Friday slot — the **Kennicott** runs
   early June, then the **Columbia** takes over from Jun 19.
2. **The season is in progress.** Today (Jun 19) is the Columbia's *first* 2026 Friday;
   most of its sailings are still in the future. The dataset must **accumulate** rather than
   ship complete.

This design generalizes the data model and detection logic from "one ship, finished season"
to "**primary + relief vessels, season filling in as it goes**," while keeping the UI a simple
static one-shot load.

## Goals

- Track the Columbia (and the Kennicott on its relief days) over the 2026 summer season.
- Keep the explorer a static-load page — no live socket, no backend (user chose accumulate-only).
- Stand up a **lightweight** going-forward capture replacing the heavy NOAA national-dump process.
- Lay out the full season up front so the tool answers "where is the Columbia over this summer."

## Non-Goals (explicitly deferred or cut)

- **No live "where is it right now" layer** — accumulate-only. Today's sailing appears once
  recorded and appended, not in real time.
- **No going-forward Kennicott capture** — the Kennicott finishes its stint Jun 12; everything
  from Jun 19 on is Columbia. Live capture is Columbia-only.
- **NOAA backfill is not built now** — see Deferred Work.

## Decisions (resolved during brainstorming)

| Question | Decision |
|---|---|
| Which vessels to track | **Columbia primary + Kennicott relief** (relief shown only on days it covers) |
| Data pipeline | **NOAA backfill** for the 2 past Kennicott Fridays + **aisstream.io live capture** going forward |
| Backfill timing | **Defer (free)** — Jun 5 & 12 are "pending" until NOAA posts 2026 (~early 2027) |
| Liveness | **Accumulate-only** — static load, like 2025 |
| Capture script | **Python** standalone script |

### Why NOAA can't backfill now

NOAA Marine Cadastre posts cleaned AIS in **delayed yearly batches**. As of 2026-06-19:
`AISDataHandler/2025/` is complete through `ais-2025-12-31.csv.zst` (~81.5 GB/yr), but
`AISDataHandler/2026/` returns **HTTP 404** — it doesn't exist. Following the cadence that just
delivered a complete 2025, June 2026 should appear when the 2026 batch lands, ~early 2027.

## Season Scope

Bellingham mainline Friday slot, decoded from the AMHS schedule screenshots
(BLI ⇄ Ketchikan "KTN"). **Working scope = Jun 5 → Aug 28.** May / September are out of scope
unless confirmed.

| Friday | Vessel | Arrive BLI | Depart north | Initial status |
|---|---|---|---|---|
| Jun 5 | Kennicott | 8:00a | 6:00p | `pending` (NOAA) |
| Jun 12 | Kennicott | 8:00a | ~3p (screenshot cut off) | `pending` (NOAA) |
| Jun 19 | Columbia | 4:45p | **9:45p** | `captured` if caught live tonight, else `pending` |
| Jun 26 | Columbia | 8:00a | 6:00p | `scheduled` |
| Jul 3 · 10 · 17 · 24 · 31 | Columbia | 8:00a | 6:00p | `scheduled` |
| Aug 7 · 14 · 21 · 28 | Columbia | 8:00a | 6:00p | `scheduled` |

## Components

### 1. Data schema v2 — `columbia_tracks_summer_2026.json`

Extends the v1 shape. Geofence box reused from v1 (`lat_min 48.68 / lat_max 48.78 /
lon_min -122.62 / lon_max -122.45`, tz `America/Los_Angeles`).

```jsonc
{
  "meta": {
    "season": 2026,
    "vessels": [
      { "name": "COLUMBIA",  "mmsi": 367144000, "call": "WYR2092", "role": "primary" },
      { "name": "KENNICOTT", "mmsi": null,      "call": null,      "role": "relief"  }
    ],
    "lat_min": 48.68, "lat_max": 48.78,
    "lon_min": -122.62, "lon_max": -122.45,
    "timezone": "America/Los_Angeles",
    "note": "AIS points for the Bellingham mainline Friday slot, summer 2026; downsampled ~1/min."
  },
  "days": [
    { "friday_pt": "2026-06-05 00:00:00-07:00", "vessel": "KENNICOTT", "status": "pending",   "points": [] },
    { "friday_pt": "2026-06-19 00:00:00-07:00", "vessel": "COLUMBIA",  "status": "captured",  "points": [ /* {t,lat,lon,sog,name,call} */ ] },
    { "friday_pt": "2026-06-26 00:00:00-07:00", "vessel": "COLUMBIA",  "status": "scheduled", "points": [] }
    // ...full season skeleton
  ]
}
```

- **`meta.vessels`** replaces v1's single `meta.mmsi`. Kennicott `mmsi`/`call` are `null` until
  resolved at backfill time.
- **`day.vessel`** — which ship sailed.
- **`day.status`** — `captured` | `pending` | `scheduled`.
- **Points unchanged** — `{ t, lat, lon, sog, name, call }`.
- File is **pre-seeded with the full season skeleton**; the capture script flips `scheduled`→
  `captured` and fills `points`.

### 2. Tool logic + UI — `index.html`

**Departure detection (fix).** v1 `detectDeparture` returns the *first* sustained ≥8 kn run after
14:00 — on an arrival+departure day (e.g. Jun 19) that latches onto the incoming arrival. New rule:
among sustained ≥8 kn runs (`sustainN = 3`), choose the **last** run of the day that is **outbound**
(net displacement away from the terminal centroid / exiting the box). This yields the northbound
departure on both normal days and arrival+departure days.

**Color window (fix).** v1 `departureColor` maps 16:00–20:30. Widen to **16:00–22:00** so the
9:45pm sailing isn't clamped to max-red; re-spread the gradient stops across the new range.

**Vessel identity (new).** Line color still encodes departure time (legend unchanged). Add a
**vessel name chip** to each route card and the spotlight; the relief vessel gets a distinct accent
so Kennicott days read differently from Columbia days at a glance.

**Pending / scheduled states (new).** Cards for non-`captured` days render greyed with no map line:
`pending` → "Kennicott · data pending (NOAA 2026)"; `scheduled` → "Columbia · scheduled". Stats
(`renderStats`) and selection helpers skip days without points.

**Copy.** 2025 → 2026; hero mentions the two vessels.

### 3. Capture pipeline — `capture_columbia.py` (new)

Standalone Python script, run by the user (manually or via cron) on Friday sailing windows.

- Connects to **aisstream.io** (free WebSocket) with the user's API key.
- Subscribes to the Bellingham bounding box, filters to **Columbia only** (`367144000`).
- Records positions, downsamples to ~1/min into `{ t, lat, lon, sog, name, call }`.
- When the sailing window ends, writes a `captured` day into `columbia_tracks_summer_2026.json`
  (flipping that Friday's `scheduled` entry).

**Prerequisite:** user registers a free aisstream.io API key.
**Deadline:** to catch *tonight's* 9:45pm departure live, the script must be running before then;
otherwise Jun 19 becomes a NOAA-backfill (`pending`) day and the first live catch is Jun 26.

## Deferred Work (documented, not built now)

- **NOAA backfill of Jun 5 & 12.** When `AISDataHandler/2026/` posts (~early 2027): download the
  two daily `.csv.zst` files, filter to the **Kennicott MMSI** within the geofence box, downsample
  ~1/min, and replace the two `pending` day objects with `captured` data. Requires resolving the
  Kennicott's MMSI at that time.

## Prerequisites & Open Items

- **aisstream.io API key** — user-provided, before the capture script can run.
- **Kennicott MMSI** — needed only at backfill time (~2027); `null` in the schema until then.
- **Season boundaries** — Jun 5 → Aug 28 assumed; confirm whether May or September Fridays apply.

## Build Sequence (high level — detailed plan via writing-plans)

1. Author schema v2 and pre-seed `columbia_tracks_summer_2026.json` with the season skeleton.
2. Update `index.html`: detection fix, color window, vessel chips, pending/scheduled states, copy,
   point it at the 2026 file.
3. Write `capture_columbia.py` against aisstream.io (Columbia-only).
4. Document the deferred NOAA backfill procedure.
