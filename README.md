# arc_tz_line

Combined line graph dashboard for the ARC ecovillage near Mkuranga, Tanzania. All temperature/humidity loggers from `arc_tz_temp_humid` plus weather station variables (wind, solar, rainfall) from `arc_tz_weather`, overlaid on a single line graph with toggleable checkboxes.

Built from `arc_tz_temp_humid` with:
- Only the line-graph view (histogram, adaptive comfort, periodic averages, density plots removed from the UI).
- Weather station data (Omnisense sensor `30B40014`) added as per-variable checkboxes with stacked right-side y-axes.

## Build

```bash
pip install pandas pytz requests openpyxl
python build.py           # full build from sensor_snapshot.json + fetched CSVs
python build.py --auto    # CI-style incremental build
```

Output: `index.html` (overwritten on every run).

## Live site

GitHub Pages: https://actionresearchprojects.github.io/arc_tz_line/

## Data pipeline

GitHub Action `update-dashboard-data.yml` runs daily at 04:00 UTC, fetching fresh Omnisense/Open-Meteo/ENSO/IOD/MJO data and rebuilding the dashboard.
