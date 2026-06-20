# Backfill runbook — Kennicott, Jun 5 & 12 2026

Two Friday sailings on the Bellingham mainline slot were run by the **Kennicott**
(relief vessel) and had already passed by the time tracking started, with no live
capture. They sit in `columbia_tracks_summer_2026.json` as `status: "pending"`
and render as greyed "data pending · NOAA 2026" cards.

They are recoverable for **free** from NOAA Marine Cadastre — just not yet.

## When to run this

NOAA publishes cleaned AIS in delayed **annual** batches. As of 2026-06-19:

- `https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025/` — complete (Jan 1–Dec 31 2025)
- `https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2026/` — **HTTP 404, not posted**

Following the cadence that delivered a complete 2025 by mid-2026, the **2026**
directory should appear **~early 2027**. Check periodically; run this once it exists.

## Prerequisite: the Kennicott's MMSI

The schema has `mmsi: null` for the Kennicott (`meta.vessels`). Resolve it first
(MarineTraffic/VesselFinder, search "Kennicott" AMHS, US flag) and confirm against
the AMHS fleet. Update `meta.vessels` with the real `mmsi` and `call` sign.

## Steps

1. **Download** the two daily files from the 2026 directory:
   - `ais-2026-06-05.csv.zst`
   - `ais-2026-06-12.csv.zst`

2. **Filter** each to the Kennicott's MMSI *and* the Bellingham geofence box
   (same box as `meta`): lat `48.68`–`48.78`, lon `-122.62`–`-122.45`.

3. **Shape** the surviving rows into the point schema, one JSONL line each:
   `{ "t": <ISO8601 Pacific>, "lat", "lon", "sog", "name": "KENNICOTT", "call": <Kennicott call> }`.
   Convert timestamps from UTC to `America/Los_Angeles`. Downsample to ~1/min.
   Write to `captures/kennicott_2026-06-05.jsonl` and `..._06-12.jsonl`.

   > The NOAA CSV columns differ from the aisstream feed, so this needs a small
   > one-off extract script — not `capture_columbia.py` (which is aisstream-shaped
   > and Columbia-pinned). Model the output on the 2025 point schema.

4. **Merge** each into the season JSON with the existing finalize step (it is
   vessel-agnostic — it just fills the matching day's points and flips status):

   ```bash
   .venv/bin/python finalize_day.py --friday 2026-06-05 --capture captures/kennicott_2026-06-05.jsonl
   .venv/bin/python finalize_day.py --friday 2026-06-12 --capture captures/kennicott_2026-06-12.jsonl
   ```

5. **Verify** in the browser: both cards turn interactive, show a `KENNICOTT`
   chip and a detected departure, and draw a track. The detector picks the last
   sustained outbound run, same as for the Columbia.

## Notes

- `finalize_day.py` is idempotent — safe to re-run if a first extract looks wrong.
- These were ~6:00pm departures (Jun 12's exact time was cut off in the schedule
  screenshot; the captured track is the source of truth once backfilled).
