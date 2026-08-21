# TidaLine Power BI Dashboard

## What this covers
Gold-layer **ports** and **earthquakes** only (no vessels).

The semantic model already in the `.pbix` is used as-is.

## Pages (maps are required)

### 1. Overview
- KPI cards: ports, earthquakes, classified ports, average magnitude
- **Worldwide ports map (blue)**
- **Earthquakes map (orange)**
- Risk classification donut, `supplies_rate`, `comm_rate`

### 2. Ports Analysis
Worldwide port map plus the attributes the business asked for:

- **Map** of all ports (blue). Click a port to filter the table.
- Slicers: country, harbor size, harbor type, supplies_rate, comm_rate, shelter
- Table: country, depths, harbor size & type, shelter, `supplies_rate`, `comm_rate`, coordinates

`supplies_rate` / `comm_rate` come from Gold (Excellent / Good / Limited / Unavailable).

### 3. Risk Analysis
Relationship between seismic events and nearby ports:

- **Earthquakes map (orange)** — click an event to filter nearby ports
- **Ports map (blue)** — click a port to filter nearby earthquakes
- Classification slicer for Health & Safety: **safe / cautionary / dangerous**
- Distance flags: `within_50km`, `within_100km`
- `date_key` slicer for recent vs historical events
- Detail table: port name, port coordinates, `unid`, `event_time`, `flynn_region`, event lat/lon, `mag`, `magtype`, `depth_km`, `distance_km`, classification

Risk KPI (Gold):
- **dangerous** — nearest event ≤ 50 km
- **cautionary** — nearest event 50–100 km
- **safe** — nearest event > 100 km

Vessels headed to dangerous ports are **not** in this dashboard (vessel stream is out of scope).

## How to open
1. Close Power BI Desktop if `Dashboard.pbix` is locked.
2. Open `powerbi/Dashboard.pbix` (or `Dashboard_complete.pbix` if the builder could not overwrite the locked file).
3. If Desktop asks to recover the file, accept.
4. Open **Ports Analysis** and **Risk Analysis** and confirm the maps render (Bing/Azure map in Power BI needs an internet connection).

## Rebuild
```bash
python powerbi/build_dashboard.py
```
