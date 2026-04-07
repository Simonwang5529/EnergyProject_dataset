# Interactive Map Layer Guide

This note records the seven views currently included in the NYC outage themes interactive map.

## Layer list

| Button | Full title | Geography | What it shows | Main value |
| --- | --- | --- | --- | --- |
| `P00 Annual Trends` | Annual Temporal Outages and Severe Weather | Tracts | Year-by-year tract map with a metric switcher and year slider. | Lets you compare annual outage occurrence, mean outage duration, and severe-weather exposure from 2014 to 2023. |
| `P04 SW Significance` | Severe Weather Association Significance | Tracts | Statistical significance view for the severe weather versus outage-occurrence relationship. | Highlights where the tract-level association passes `95%`, `99%`, or the combined `95% + 99%` threshold. |
| `P06 Seasonality Clusters` | Tract Outage Seasonality Clusters | Tracts | Categorical cluster map. | Groups tracts by similar outage seasonality patterns. |
| `P07 Overall CVI` | Overall Climate Vulnerability Index | Tracts | Continuous choropleth of the overall CVI score. | Shows the structural vulnerability pattern across NYC tracts. |
| `P07 Priority Overlap` | Priority Overlap: Vulnerability + Outage Burden | Tracts | Rule-based priority flag map. | Identifies tracts where high vulnerability overlaps with higher outage burden. |
| `P08 County Vulnerability` | County Vulnerability Concentration | Counties | County choropleth of the share of high-CVI tracts. | Summarizes where vulnerability is most concentrated at the county level. |
| `P10 Duration Risk` | County Duration Risk (2023) | Counties | County map of long-outage severity. | Compares 2023 county-level outage duration risk using the 90th-percentile duration and related long-outage indicators. |

## How to read them

### `P00 Annual Trends`
- Use the `Metric` dropdown to switch between outage occurrence, mean outage duration, and severe-weather exposure.
- Use the year slider or `Play` button to see how the tract pattern changes over time.
- This is the only temporal view in the current map.

### `P04 SW Significance`
- Use the `Significance` dropdown to switch between `95%`, `99%`, and `95% + 99%`.
- The combined view separates tracts that pass the stricter `99%` threshold from tracts that are significant only at `95%`.
- This view is intended for statistical interpretation rather than magnitude comparison.

### `P06 Seasonality Clusters`
- Each tract is assigned to a discrete seasonality class.
- The legend shows how many tracts fall into each cluster.
- Use this layer to compare pattern types, not raw outage totals.

### `P07 Overall CVI`
- Colors represent equal-interval bins of the overall CVI score.
- This is the main tract-scale vulnerability surface in the map.
- Use it together with `P07 Priority Overlap` to compare vulnerability alone versus vulnerability plus outage burden.

### `P07 Priority Overlap`
- This layer simplifies the tract view into flagged and non-flagged priority areas.
- It is meant to support intervention or screening conversations.
- Tooltip values provide the supporting outage and CVI context behind the flag.

### `P08 County Vulnerability`
- This county view aggregates tract-level vulnerability into a county share.
- It is helpful for high-level presentation because it compresses the tract pattern into a borough/county summary.
- A static PNG for this layer is also exported with the build.

### `P10 Duration Risk`
- This view focuses on long outages rather than outage counts.
- The main mapped value is county `p90` duration in hours, with tooltip context for long outages over `8h` and `24h`.
- Use it when the story is resilience and recovery time, not outage frequency.

## Data sources used by the build

- `NYC_Outage_Themes.gpkg`
  - Main geometry and attribute source for all seven views.
- `EDA_Outage_Calculated_CSVs/part_00_foundation/annual_panel.csv`
  - Annual tract panel used for `P00`.
- `EDA_Outage_Calculated_CSVs/part_04/tract_sw.csv`
  - Significance source synced into the GeoPackage for `P04`.

