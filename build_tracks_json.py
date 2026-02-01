import glob, json
import pandas as pd

MMSI = 367144000

# "Harbor + a little bigger" box (you can tweak later)
LAT_MIN, LAT_MAX = 48.68, 48.78
LON_MIN, LON_MAX = -122.62, -122.45

usecols = ["mmsi", "base_date_time", "latitude", "longitude", "sog", "vessel_name", "call_sign"]

files = sorted(glob.glob("ais-2025-*.csv"))
if not files:
    raise SystemExit("No ais-2025-*.csv files found. Are you in the folder with the decompressed CSVs?")

rows = []
for f in files:
    print("Reading", f)
    for chunk in pd.read_csv(f, usecols=usecols, chunksize=1_000_000):
        c = chunk[chunk["mmsi"] == MMSI]
        if c.empty:
            continue

        t_utc = pd.to_datetime(c["base_date_time"], utc=True, errors="coerce")
        c = c.assign(t_pt=t_utc.dt.tz_convert("America/Los_Angeles"))
        c = c.dropna(subset=["t_pt", "latitude", "longitude", "sog"])

        # keep only local Fridays
        c = c[c["t_pt"].dt.dayofweek == 4]
        if c.empty:
            continue

        # keep within the broad area
        c = c[
            c["latitude"].between(LAT_MIN, LAT_MAX) &
            c["longitude"].between(LON_MIN, LON_MAX)
        ]
        if c.empty:
            continue

        rows.append(c[["t_pt", "latitude", "longitude", "sog", "vessel_name", "call_sign"]])

if not rows:
    raise SystemExit("No Columbia Friday rows found. (Box too tight? Missing CSVs?)")

df = pd.concat(rows, ignore_index=True).sort_values("t_pt")
df["friday_pt"] = df["t_pt"].dt.floor("D")

# Light de-dupe: same lat/lon/time repeats happen
df = df.drop_duplicates(subset=["t_pt", "latitude", "longitude", "sog"])

# Downsample to keep file small:
# keep at most one point per 60 seconds (approx)
df["t_sec"] = (df["t_pt"].astype("int64") // 10**9)
df["t_bucket"] = (df["t_sec"] // 60)
df = df.sort_values("t_pt").groupby(["friday_pt", "t_bucket"], as_index=False).tail(1)

out = {
    "meta": {
        "mmsi": MMSI,
        "lat_min": LAT_MIN, "lat_max": LAT_MAX,
        "lon_min": LON_MIN, "lon_max": LON_MAX,
        "timezone": "America/Los_Angeles",
        "note": "AIS points for Columbia on local Fridays in summer 2025; downsampled to ~1/min."
    },
    "days": []
}

for day, g in df.groupby("friday_pt"):
    g = g.sort_values("t_pt")
    out["days"].append({
        "friday_pt": str(day),
        "points": [
            {
                "t": ts.isoformat(),
                "lat": float(lat),
                "lon": float(lon),
                "sog": float(sog),
                "name": (name if isinstance(name, str) else ""),
                "call": (call if isinstance(call, str) else ""),
            }
            for ts, lat, lon, sog, name, call in zip(g["t_pt"], g["latitude"], g["longitude"], g["sog"], g["vessel_name"], g["call_sign"])
        ]
    })

with open("columbia_tracks_summer_fridays_2025.json", "w") as f:
    json.dump(out, f)

print("Wrote: columbia_tracks_summer_fridays_2025.json")
print("Days:", len(out["days"]))
