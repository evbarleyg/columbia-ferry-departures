# Deploy: Vercel site + 5-minute GitHub Action poller

No always-on machine. A scheduled GitHub Action polls the position every ~5 min and
force-pushes the data to a rolling `live-data` branch; Vercel serves the static
explorer + live page and reads the data from that branch via rewrites.

```
GitHub Action (*/5)  ──locate──►  poll_position.py  ──force-push──►  live-data branch
   key = repo secret                                                  (1 rolling commit:
                                                                       live_position.json
                                                                       columbia_tracks_*.json)
                                                                            │ raw URL
Vercel (static, from main)  ── /data/* rewrite ────────────────────────────┘
   /          -> explorer (reads /data/columbia.json)
   /tracker   -> live map (polls /data/live_position.json every ~60s)
```

## What's in the repo

- `poll_position.py` — one aisstream locate → updates live position + trail, appends
  in-geofence points to the season path, pings ntfy on box entry. Stateless; all
  state is the two files on `live-data`.
- `.github/workflows/poll.yml` — the `*/5` cron + the rolling-branch publish.
- `tracker.html` — the live map (polls `/data/live_position.json`).
- `index.html` — the explorer; reads `/data/columbia.json` (live), falls back to the
  committed file locally.
- `vercel.json` — `cleanUrls` + `/data/*` rewrites to the `live-data` raw URLs.

## One-time setup (your steps)

1. **GitHub secret** (the poller's key — you set it, I don't touch keys):
   ```bash
   gh secret set AISSTREAM_API_KEY < aisstream_key.txt
   gh secret set NTFY_TOPIC --body "columbia-<random-suffix>"   # optional, arrival pings
   ```
2. **Merge this branch to `main`** (so the workflow + site code are live).
3. **Kick the poller once** to create the `live-data` branch:
   `gh workflow run poll.yml` — then confirm a `live-data` branch appears with the two files.
4. **Connect Vercel** to the repo (Production Branch = `main`):
   - Dashboard → New Project → import `columbia-ferry-departures`, or `vercel link` + `vercel --prod`.
   - No build step (static); `vercel.json` handles routing.
5. **Pings:** install the **ntfy** app, subscribe to your `NTFY_TOPIC`.

Open the Vercel URL on your phone → `/` for the explorer, `/tracker` for live.

## Retire the local capture

Once the poller is live, the Mac-side capture is redundant — they'd both hit the
one key. Stop it: `pkill -f capture_columbia.py`. (Tonight it stays the source of
truth until the first finalize.)

## Notes / knobs

- **Cron isn't punctual** — `*/5` can lag/skip under load. Fine at this grain.
- **5-min path** is coarser than the source's 1-min; if departure detection misses
  a sparse run, drop `sustainN` from 3 to 2 in `index.html`'s `detectDeparture`.
- **CORS:** if a browser ever blocks the raw URL directly, the Vercel `/data/*`
  rewrite proxies it same-origin (already the default path the pages use).
- History stays tiny: `live-data` is force-pushed to a single commit each run.
