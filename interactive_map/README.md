# Interactive Map Handoff

This folder turns the existing `NYC_Outage_Themes.gpkg` file into a browser-ready interactive map for the updated spatial tasks.

## Deployed website

https://simonwang5529.github.io/EnergyProject_dataset/

## What it builds

- `output/nyc_outage_updated_layers.html`
  - Self-contained interactive map for the updated part 06 to part 10 views:
    - Part 06: tract seasonality clusters
    - Part 07: tract CVI overall
    - Part 07: tract priority overlap
    - Part 08: county high-CVI tract share
    - Part 10: county duration risk

- `output/part_08_county_vulnerability_concentration.png`
  - Static county map for the part 08 vulnerability-concentration layer.

- `output/nyc_outage_updated_layers_preview.png`
  - Slide-ready preview montage you can paste into your summary document or slideshow.

## Rebuild

Run:

```bash
python3 interactive_map/build_interactive_map.py
```


