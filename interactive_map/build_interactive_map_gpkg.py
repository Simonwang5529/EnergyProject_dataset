#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

import build_interactive_map as base


PROJECT_DIR = Path(__file__).resolve().parent
ROOT = PROJECT_DIR.parents[0]
DEFAULT_GPKG_PATH = ROOT / "NYC_Outage_Themes.gpkg"
DEFAULT_SIG_CSV_PATH = ROOT / "EDA_Outage_Calculated_CSVs" / "part_04" / "tract_sw.csv"
DEFAULT_ANNUAL_CSV_PATH = ROOT / "EDA_Outage_Calculated_CSVs" / "part_00_foundation" / "annual_panel.csv"
DEFAULT_HTML_PATH = ROOT / "nyc_outage_updated_layers.html"
DEFAULT_DOCS_INDEX_PATH = ROOT / "docs" / "index.html"
DEFAULT_COUNTY_PNG_PATH = ROOT / "part_08_county_vulnerability_concentration.png"
DEFAULT_PREVIEW_PNG_PATH = ROOT / "nyc_outage_updated_layers_preview.png"
SPATIALITE_CANDIDATES = [
    Path("/opt/homebrew/lib/mod_spatialite.dylib"),
    Path("/usr/local/lib/mod_spatialite.dylib"),
]


def resolve_existing_file(candidates: list[Path]) -> Path:
    for candidate in candidates:
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate
    checked = "\n".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(f"Could not find a usable file. Checked:\n{checked}")


def maybe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync significance columns into a GeoPackage and build the interactive map."
    )
    parser.add_argument("--gpkg", type=Path, default=DEFAULT_GPKG_PATH, help="Input GeoPackage.")
    parser.add_argument(
        "--significance-csv",
        type=Path,
        default=DEFAULT_SIG_CSV_PATH,
        help="CSV containing 95%% and 99%% significance columns.",
    )
    parser.add_argument(
        "--html-path",
        type=Path,
        default=DEFAULT_HTML_PATH,
        help="Where to write the primary self-contained HTML map.",
    )
    parser.add_argument(
        "--docs-index-path",
        type=Path,
        default=DEFAULT_DOCS_INDEX_PATH,
        help="Where to write the GitHub Pages index.html.",
    )
    parser.add_argument(
        "--county-png-path",
        type=Path,
        default=DEFAULT_COUNTY_PNG_PATH,
        help="Where to write the county vulnerability static PNG.",
    )
    parser.add_argument(
        "--preview-png-path",
        type=Path,
        default=DEFAULT_PREVIEW_PNG_PATH,
        help="Where to write the preview montage PNG.",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Build from the GeoPackage as-is without writing CSV significance columns into it first.",
    )
    parser.add_argument(
        "--annual-csv",
        type=Path,
        default=DEFAULT_ANNUAL_CSV_PATH,
        help="Annual panel CSV for the Part 00 temporal choropleth.",
    )
    return parser.parse_args()


def load_spatialite(connection: sqlite3.Connection) -> None:
    connection.enable_load_extension(True)
    for candidate in SPATIALITE_CANDIDATES:
        if candidate.exists():
            connection.load_extension(str(candidate))
            return
    checked = ", ".join(str(path) for path in SPATIALITE_CANDIDATES)
    raise FileNotFoundError(f"Could not find mod_spatialite. Checked: {checked}")


def load_significance_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def load_annual_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def normalize_gpkg_value(column: str, value: str | None) -> int | float | None:
    if value in ("", None):
        return None
    if column.startswith(("occ_sig_", "dur_sig_")):
        return 1 if str(value).strip().lower() == "true" else 0
    return float(value)


def sync_significance_to_gpkg(gpkg_path: Path, csv_path: Path) -> None:
    rows = load_significance_rows(csv_path)
    column_defs = {
        "n_years": "REAL",
        "occ_spearman_rho": "REAL",
        "occ_spearman_p": "REAL",
        "dur_spearman_rho": "REAL",
        "dur_spearman_p": "REAL",
        "occ_spearman_q": "REAL",
        "dur_spearman_q": "REAL",
        "occ_sig_05": "INTEGER",
        "occ_sig_01": "INTEGER",
        "dur_sig_05": "INTEGER",
        "dur_sig_01": "INTEGER",
    }

    connection = sqlite3.connect(gpkg_path)
    try:
        load_spatialite(connection)
        cursor = connection.cursor()
        for table in ("tract_clusters", "tract_trend_sig"):
            existing = {row[1] for row in cursor.execute(f"PRAGMA table_info('{table}')")}
            for column, column_type in column_defs.items():
                if column not in existing:
                    cursor.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {column_type}')
            assignments = ", ".join(f'"{column}" = ?' for column in column_defs)
            sql = f'UPDATE "{table}" SET {assignments} WHERE tract_fips = ?'
            for row in rows:
                values = [normalize_gpkg_value(column, row.get(column)) for column in column_defs]
                values.append(row["tract_fips"])
                cursor.execute(sql, values)
        connection.commit()
    finally:
        connection.close()


def load_tract_geometry(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        select tract_fips, county_name, geom
        from tract_clusters
        where tract_fips is not null
        order by tract_fips
        """
    ).fetchall()
    return [
        {
            "id": f"tract-{row['tract_fips']}",
            "label": row["tract_fips"],
            "county_name": row["county_name"],
            "geometry": base.parse_gpkg_geometry(row["geom"]),
        }
        for row in rows
    ]


def load_county_geometry(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return base.load_county_geometry(connection)


def add_legend_meta(layer: dict[str, Any], title: str, note: str) -> dict[str, Any]:
    layer["legendTitle"] = title
    layer["legendNote"] = note
    return layer


def add_button_meta(layer: dict[str, Any], short_title: str, short_part: str | None = None) -> dict[str, Any]:
    layer["buttonTitle"] = short_title
    if short_part:
        layer["buttonPart"] = short_part
    return layer


def build_cluster_layer(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        select
            tract_fips,
            county_name,
            cluster,
            "Merged_NYC_outage_unique_events_2020_2023" as total_events,
            "Merged_NYC_Overall CVI Score" as cvi_overall
        from tract_clusters
        where tract_fips is not null
        order by tract_fips
        """
    ).fetchall()

    color_map = {
        "No cluster": "#d9d2c3",
        "Cluster 0": "#4e79a7",
        "Cluster 1": "#f28e2b",
        "Cluster 2": "#59a14f",
        "Cluster 3": "#e15759",
    }
    styles: dict[str, dict[str, str]] = {}
    counts: Counter[str] = Counter()

    for row in rows:
        label = "No cluster" if row["cluster"] is None else f"Cluster {int(row['cluster'])}"
        counts[label] += 1
        styles[f"tract-{row['tract_fips']}"] = {
            "fill": color_map[label],
            "tooltip": (
                f"<strong>Tract {row['tract_fips']}</strong><br>"
                f"County: {row['county_name']}<br>"
                f"Seasonality type: {label}<br>"
                f"Unique outage events (2020-2023): {int(row['total_events']) if row['total_events'] is not None else 'No data'}<br>"
                f"Overall CVI: {base.format_float(row['cvi_overall'])}"
            ),
        }

    summary = (
        f"{counts.get('Cluster 0', 0) + counts.get('Cluster 1', 0) + counts.get('Cluster 2', 0) + counts.get('Cluster 3', 0)} "
        f"tracts are assigned to four seasonality clusters, with {counts.get('No cluster', 0)} tracts left unclassified. To be clear, the boroughs ended up cleanly organizing into different clusters."
    )
    return add_button_meta(
        add_legend_meta(
            {
                "id": "part06_clusters",
                "part": "Part 06",
                "title": "Tract Outage Seasonality Clusters",
                "description": "Categorical cluster map showing where outage seasonality patterns differ across NYC tracts.",
                "summary": summary,
                "group": "tracts",
                "legend": base.discrete_legend(
                    counts,
                    color_map,
                    ["Cluster 0", "Cluster 1", "Cluster 2", "Cluster 3", "No cluster"],
                ),
                "styles": styles,
            },
            "Seasonality categories",
            "Counts in parentheses show how many tracts fall in each cluster.",
        ),
        "Seasonality Clusters",
        "P06",
    )


def build_cvi_layer(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        select
            tract_fips,
            county_name,
            cvi_overall,
            cvi_baseline_social_econ,
            cvi_climate_extreme_events
        from tract_cvi
        where tract_fips is not null
        order by tract_fips
        """
    ).fetchall()

    palette = ["#f7fbff", "#c6dbef", "#6baed6", "#3182bd", "#08519c"]
    values = [float(row["cvi_overall"]) for row in rows if row["cvi_overall"] is not None]
    edges = base.equal_interval_bins(values)
    styles: dict[str, dict[str, str]] = {}
    counts: Counter[str] = Counter()

    for row in rows:
        dom_id = f"tract-{row['tract_fips']}"
        if row["cvi_overall"] is None:
            label = "No data"
            fill = "#d9d2c3"
        else:
            index = base.bin_index(float(row["cvi_overall"]), edges)
            label = base.bin_label(edges, index, digits=3)
            fill = palette[index]
        counts[label] += 1
        styles[dom_id] = {
            "fill": fill,
            "tooltip": (
                f"<strong>Tract {row['tract_fips']}</strong><br>"
                f"County: {row['county_name']}<br>"
                f"Overall CVI: {base.format_float(row['cvi_overall'])}<br>"
                f"Baseline social/economic: {base.format_float(row['cvi_baseline_social_econ'])}<br>"
                f"Climate extreme events: {base.format_float(row['cvi_climate_extreme_events'])}"
            ),
        }

    legend = [
        {
            "label": f"{base.bin_label(edges, index, digits=3)} ({counts[base.bin_label(edges, index, digits=3)]})",
            "color": palette[index],
        }
        for index in range(len(edges) - 1)
    ]
    return add_button_meta(
        add_legend_meta(
            {
                "id": "part07_cvi",
                "part": "Part 07",
                "title": "Overall Climate Vulnerability Index",
                "description": "Tract-level choropleth for the overall CVI score, highlighting the structural vulnerability landscape.",
                "summary": f"Overall CVI ranges from {min(values):.3f} to {max(values):.3f} across {len(rows):,} tracts. While there are certainly some areas that are particularly vulnerable, and others that are much less, CVI seems to be almost evenly distributed across the city.",
                "group": "tracts",
                "legend": legend,
                "styles": styles,
            },
            "Equal-interval CVI bins",
            "Each swatch shows a score range; counts in parentheses show how many tracts land in that range.",
        ),
        "Overall CVI",
        "P07",
    )


def build_priority_layer(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        select
            tract_fips,
            county_name,
            priority_flag,
            occ_slope_per_year,
            cvi_overall,
            avg_outage_occurrence,
            total_outages
        from tract_priority
        where tract_fips is not null
        order by tract_fips
        """
    ).fetchall()

    color_map = {
        "Priority tract": "#b2182b",
        "Not flagged": "#ef8a62",
        "No data": "#d9d2c3",
    }
    styles: dict[str, dict[str, str]] = {}
    counts: Counter[str] = Counter()

    for row in rows:
        dom_id = f"tract-{row['tract_fips']}"
        if row["priority_flag"] is None:
            label = "No data"
        elif int(row["priority_flag"]) == 1:
            label = "Priority tract"
        else:
            label = "Not flagged"
        counts[label] += 1
        styles[dom_id] = {
            "fill": color_map[label],
            "tooltip": (
                f"<strong>Tract {row['tract_fips']}</strong><br>"
                f"County: {row['county_name']}<br>"
                f"Priority status: {label}<br>"
                f"Occurrence slope per year: {base.format_float(row['occ_slope_per_year'])}<br>"
                f"Average outage occurrence: {base.format_float(row['avg_outage_occurrence'])}<br>"
                f"Overall CVI: {base.format_float(row['cvi_overall'])}<br>"
                f"Total outages: {base.format_float(row['total_outages'], digits=0)}"
            ),
        }

    return add_button_meta(
        add_legend_meta(
            {
                "id": "part07_priority",
                "part": "Part 07",
                "title": "Priority Overlap: Vulnerability + Outage Burden",
                "description": "Rule-based priority map highlighting tracts where high vulnerability overlaps with elevated outage burden.",
                "summary": f"{counts.get('Priority tract', 0)} tracts are flagged as overlap priorities where vulnerability and outage burden are jointly elevated. These areas would require the most attention going forward.",
                "group": "tracts",
                "legend": base.discrete_legend(counts, color_map, ["Priority tract", "Not flagged", "No data"]),
                "styles": styles,
            },
            "Priority status",
            "Priority tracts are the overlap areas where vulnerability and outage burden are both elevated.",
        ),
        "Priority Overlap",
        "P07",
    )


def build_sw_significance_layer(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        select
            tract_fips,
            county_name,
            n_years,
            occ_spearman_rho,
            occ_spearman_q,
            dur_spearman_rho,
            dur_spearman_q,
            occ_sig_05,
            occ_sig_01,
            dur_sig_05,
            dur_sig_01
        from tract_clusters
        where tract_fips is not null
        order by tract_fips
        """
    ).fetchall()

    variant_meta = {
        "both": {
            "label": "95% & 99%",
            "color_map": {
                "99% significant": "#8c2d04",
                "95% significant only": "#ec7014",
                "Not significant": "#fdd49e",
                "No data": "#d9d2c3",
            },
            "order": ["99% significant", "95% significant only", "Not significant", "No data"],
            "description": "Tract-level severe-weather versus outage-occurrence association using FDR-adjusted significance. This combined view separates tracts significant at 99% from those significant only at 95%.",
            "legend_title": "FDR-adjusted significance",
            "legend_note": "Dark red marks q < 0.01. Orange marks 0.01 <= q < 0.05. Pale sand means the tract did not pass either threshold.",
        },
        "sig_05": {
            "label": "95%",
            "color_map": {
                "Significant at 95%": "#ec7014",
                "Not significant": "#fdd49e",
                "No data": "#d9d2c3",
            },
            "order": ["Significant at 95%", "Not significant", "No data"],
            "description": "Tract-level severe-weather versus outage-occurrence association using the 95% threshold (FDR-adjusted q < 0.05).",
            "legend_title": "95% significance",
            "legend_note": "Orange includes all tracts with q < 0.05, including the stricter 99% cases.",
        },
        "sig_01": {
            "label": "99%",
            "color_map": {
                "Significant at 99%": "#8c2d04",
                "Not significant": "#fdd49e",
                "No data": "#d9d2c3",
            },
            "order": ["Significant at 99%", "Not significant", "No data"],
            "description": "Tract-level severe-weather versus outage-occurrence association using the 99% threshold (FDR-adjusted q < 0.01).",
            "legend_title": "99% significance",
            "legend_note": "Only the darkest red tracts meet the stricter q < 0.01 threshold.",
        },
    }

    variant_counts = {variant_id: Counter() for variant_id in variant_meta}
    variant_styles: dict[str, dict[str, dict[str, str]]] = {variant_id: {} for variant_id in variant_meta}

    for row in rows:
        occ_sig_05 = base.truthy(row["occ_sig_05"])
        occ_sig_01 = base.truthy(row["occ_sig_01"])
        dur_sig_05 = base.truthy(row["dur_sig_05"])
        dur_sig_01 = base.truthy(row["dur_sig_01"])
        occ_q = maybe_float(row["occ_spearman_q"])
        dur_q = maybe_float(row["dur_spearman_q"])
        dom_id = f"tract-{row['tract_fips']}"

        tooltip = (
            f"<strong>Tract {row['tract_fips']}</strong><br>"
            f"County: {row['county_name']}<br>"
            f"Occurrence rho: {base.format_float(row['occ_spearman_rho'])}<br>"
            f"Occurrence q-value: {base.format_float(occ_q)}<br>"
            f"95% significant: {'Yes' if occ_sig_05 else 'No'}<br>"
            f"99% significant: {'Yes' if occ_sig_01 else 'No'}<br>"
            f"Duration rho: {base.format_float(row['dur_spearman_rho'])}<br>"
            f"Duration q-value: {base.format_float(dur_q)}<br>"
            f"Duration 95% significant: {'Yes' if dur_sig_05 else 'No'}<br>"
            f"Duration 99% significant: {'Yes' if dur_sig_01 else 'No'}<br>"
            f"Years observed: {base.format_float(row['n_years'], digits=0)}"
        )

        if occ_q is None and dur_q is None:
            labels = {"both": "No data", "sig_05": "No data", "sig_01": "No data"}
        else:
            labels = {
                "both": "99% significant" if occ_sig_01 else "95% significant only" if occ_sig_05 else "Not significant",
                "sig_05": "Significant at 95%" if occ_sig_05 else "Not significant",
                "sig_01": "Significant at 99%" if occ_sig_01 else "Not significant",
            }

        for variant_id, label in labels.items():
            variant_counts[variant_id][label] += 1
            variant_styles[variant_id][dom_id] = {
                "fill": variant_meta[variant_id]["color_map"][label],
                "tooltip": tooltip,
            }

    variants = {}
    both_counts = variant_counts["both"]
    for variant_id, meta in variant_meta.items():
        counts = variant_counts[variant_id]
        if variant_id == "both":
            summary = (
                f"{both_counts.get('99% significant', 0)} tracts meet the 99% threshold and "
                f"{both_counts.get('95% significant only', 0)} additional tracts meet the 95% threshold only. These areas tend to feel the strongest effects of weather-induced outages."
            )
        elif variant_id == "sig_05":
            summary = f"{counts.get('Significant at 95%', 0)} tracts meet the 95% significance threshold for the severe-weather versus outage-occurrence association. These areas tend to feel the strongest effects of weather-induced outages."
        else:
            summary = f"{counts.get('Significant at 99%', 0)} tracts meet the 99% significance threshold for the severe-weather versus outage-occurrence association. These areas tend to feel the strongest effects of weather-induced outages."

        variants[variant_id] = {
            "label": meta["label"],
            "description": meta["description"],
            "summary": summary,
            "legend": base.discrete_legend(counts, meta["color_map"], meta["order"]),
            "legendTitle": meta["legend_title"],
            "legendNote": meta["legend_note"],
            "styles": variant_styles[variant_id],
        }

    return add_button_meta(
        {
            "id": "part04_sw_significance",
            "part": "Part 04",
            "title": "Severe Weather Association Significance",
            "description": variants["both"]["description"],
            "summary": variants["both"]["summary"],
            "group": "tracts",
            "legend": variants["both"]["legend"],
            "legendTitle": variants["both"]["legendTitle"],
            "legendNote": variants["both"]["legendNote"],
            "styles": variants["both"]["styles"],
            "defaultVariant": "both",
            "variants": variants,
        },
        "SW Significance",
        "P04",
    )


def build_high_cvi_share_layer(connection: sqlite3.Connection) -> dict[str, Any]:
    return add_button_meta(
        add_legend_meta(
            base.build_high_cvi_share_layer(connection),
            "Equal-interval county bins",
            "Each range shows the share of high-CVI tracts within a county; counts in parentheses show county totals per bin.",
        ),
        "County Vulnerability",
        "P08",
    )


def build_duration_layer(connection: sqlite3.Connection) -> dict[str, Any]:
    return add_button_meta(
        add_legend_meta(
            base.build_duration_layer(connection),
            "Equal-interval duration bins",
            "Ranges are based on county p90 outage duration in hours; counts in parentheses show county totals per bin.",
        ),
        "Duration Risk",
        "P10",
    )


def build_annual_temporal_layer(annual_csv_path: Path) -> dict[str, Any]:
    rows = load_annual_rows(annual_csv_path)
    values_by_feature: dict[str, dict[str, dict[str, float | None]]] = {}
    years_set: set[int] = set()

    metrics = {
        "outage_occurrence": {
            "label": "Outage occurrence",
            "description": "Annual count of outage events per tract.",
            "unit": "events",
            "digits": 0,
            "palette": ["#fff5f0", "#fcbba1", "#fc9272", "#fb6a4a", "#cb181d"],
        },
        "outage_duration_mean": {
            "label": "Outage duration (mean)",
            "description": "Mean outage duration in hours per tract-year.",
            "unit": "hours",
            "digits": 2,
            "palette": ["#f7fcfd", "#ccece6", "#66c2a4", "#2ca25f", "#006d2c"],
        },
        "severe_weather_count": {
            "label": "Severe-weather exposure",
            "description": "Annual count of severe-weather-linked outages per tract.",
            "unit": "events",
            "digits": 0,
            "palette": ["#f7fbff", "#c6dbef", "#6baed6", "#3182bd", "#08519c"],
        },
    }

    metric_values: dict[str, list[float]] = {key: [] for key in metrics}

    for row in rows:
        tract_fips = str(row.get("tract_fips", "")).strip().zfill(11)
        if not tract_fips:
            continue
        feature_id = f"tract-{tract_fips}"
        year = str(int(row["year"]))
        years_set.add(int(year))

        feature_year = values_by_feature.setdefault(feature_id, {})
        entry = {
            "outage_occurrence": maybe_float(row.get("outage_occurrence")),
            "outage_duration_mean": maybe_float(row.get("outage_duration_mean")),
            "severe_weather_count": maybe_float(row.get("severe_weather_count")),
        }
        feature_year[year] = entry
        for metric_key, metric_value in entry.items():
            if metric_value is not None:
                metric_values[metric_key].append(metric_value)

    metric_options: dict[str, dict[str, Any]] = {}
    for metric_key, meta in metrics.items():
        bins = base.equal_interval_bins(metric_values[metric_key]) if metric_values[metric_key] else [0.0, 1.0]
        metric_options[metric_key] = {**meta, "bins": bins}

    years = sorted(years_set)
    default_year = str(years[0]) if years else "2014"

    return {
        "id": "part00_annual_temporal",
        "part": "Part 00",
        "buttonPart": "P00",
        "buttonTitle": "Annual Trends",
        "title": "Annual Temporal Outages and Severe Weather",
        "description": "Year-by-year choropleths with playback for outage occurrence, mean outage duration, and severe-weather exposure.",
        "summary": "Use the timeline controls to animate annual changes and compare spatial patterns across metrics.",
        "group": "tracts",
        "legendTitle": "Equal-interval metric bins",
        "legendNote": "Bins are fixed across all years for each metric so year-to-year color changes are directly comparable.",
        "legend": [],
        "styles": {},
        "temporal": {
            "years": [str(year) for year in years],
            "defaultYear": default_year,
            "metricOptions": metric_options,
            "defaultMetric": "outage_occurrence",
            "values": values_by_feature,
        },
    }


def write_html(
    tract_geometry: list[dict[str, Any]],
    county_geometry: list[dict[str, Any]],
    layers: list[dict[str, Any]],
    _map_width: int,
    _map_height: int,
    output_path: Path,
) -> None:
    geometry_payload = {
        "tracts": base.make_geojson_payload(tract_geometry),
        "counties": base.make_geojson_payload(county_geometry),
    }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NYC Outage Themes Interactive Map</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    :root {{
      --paper: #f6f2ea;
      --panel: #18333b;
      --panel-soft: #254852;
      --ink: #1f2a2e;
      --muted: #5f6c72;
      --line: #d9d1c2;
      --card: #fffdfa;
      --accent: #dc6d32;
      --shadow: 0 18px 48px rgba(16, 31, 36, 0.16);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", Avenir, "Trebuchet MS", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(220, 109, 50, 0.13), transparent 28%),
        linear-gradient(180deg, #fcfaf5 0%, var(--paper) 100%);
    }}
    .shell {{
      display: grid;
      grid-template-columns: minmax(280px, 350px) 1fr;
      gap: 18px;
      padding: 18px;
      min-height: 100vh;
    }}
    .sidebar {{
      background: linear-gradient(180deg, var(--panel) 0%, var(--panel-soft) 100%);
      color: #fdf8f1;
      border-radius: 24px;
      padding: 20px;
      box-shadow: var(--shadow);
      display: flex;
      flex-direction: column;
      gap: 16px;
    }}
    .eyebrow {{
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 0.72rem;
      color: #f3c89b;
      margin-bottom: 10px;
    }}
    h1 {{
      margin: 0;
      font-size: 1.8rem;
      line-height: 1.08;
    }}
    .lede {{
      margin: 8px 0 0 0;
      color: #d4e0e4;
      line-height: 1.42;
      font-size: 0.92rem;
    }}
    .layer-buttons {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
    }}
    .layer-button {{
      width: 100%;
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.06);
      color: #fdf8f1;
      text-align: left;
      padding: 10px 12px;
      cursor: pointer;
    }}
    .layer-button:hover {{
      transform: translateY(-1px);
      border-color: rgba(255, 255, 255, 0.28);
    }}
    .layer-button.active {{
      background: rgba(243, 200, 155, 0.16);
      border-color: rgba(243, 200, 155, 0.8);
    }}
    .layer-part {{
      display: block;
      font-size: 0.66rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #f3c89b;
      margin-bottom: 3px;
    }}
    .layer-title {{
      display: block;
      font-weight: 700;
      font-size: 0.88rem;
      line-height: 1.2;
    }}
    .card {{
      background: rgba(255, 255, 255, 0.07);
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: 18px;
      padding: 14px;
    }}
    .card h2 {{
      margin: 0 0 8px 0;
      font-size: 1rem;
    }}
    .card p {{
      margin: 0;
      color: #e6eef1;
      line-height: 1.42;
      font-size: 0.9rem;
    }}
    .legend-title {{
      margin-top: 12px;
      color: #f7efe3;
      font-size: 0.76rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .legend {{
      display: grid;
      gap: 8px;
      margin-top: 10px;
    }}
    .legend-item {{
      display: flex;
      align-items: center;
      gap: 10px;
      color: #f7efe3;
      font-size: 0.88rem;
    }}
    .legend-swatch {{
      width: 16px;
      height: 16px;
      border-radius: 5px;
      border: 1px solid rgba(255, 255, 255, 0.22);
      flex: 0 0 auto;
    }}
    .card p.legend-note {{
      margin-top: 10px;
      color: #d4e0e4;
      line-height: 1.45;
      font-size: 0.88rem;
    }}
    .map-card {{
      background: var(--card);
      border-radius: 24px;
      box-shadow: var(--shadow);
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
      min-width: 0;
    }}
    .map-toolbar {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      flex-wrap: wrap;
      flex-shrink: 0;
    }}
    .map-toolbar h2 {{
      margin: 0;
      font-size: 1.28rem;
    }}
    .map-toolbar p {{
      margin: 4px 0 0 0;
      color: var(--muted);
      line-height: 1.4;
      max-width: 720px;
      font-size: 0.94rem;
    }}
    .toolbar-actions {{
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }}
    .toolbar-actions button {{
      border: 1px solid var(--line);
      background: #fffdfa;
      border-radius: 999px;
      padding: 8px 12px;
      color: var(--ink);
      cursor: pointer;
      font: inherit;
    }}
    .variant-control {{
      display: none;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--line);
      background: #fffdfa;
      border-radius: 999px;
      padding: 7px 12px;
      color: var(--ink);
      font-size: 0.9rem;
    }}
    .variant-control span {{
      color: var(--muted);
      white-space: nowrap;
    }}
    .variant-control select {{
      border: 0;
      background: transparent;
      color: var(--ink);
      font: inherit;
      padding-right: 6px;
    }}
    .map-summary {{
      margin: 0;
      padding: 10px 14px;
      background: rgba(220, 109, 50, 0.07);
      border-left: 3px solid var(--accent);
      border-radius: 0 8px 8px 0;
      color: var(--muted);
      font-size: 0.9rem;
      line-height: 1.5;
      flex-shrink: 0;
    }}
    .temporal-control {{
      display: none;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--line);
      background: #fffdfa;
      border-radius: 999px;
      padding: 7px 12px;
      color: var(--ink);
      font-size: 0.9rem;
    }}
    .temporal-control span {{
      color: var(--muted);
      white-space: nowrap;
    }}
    .temporal-control select {{
      border: 0;
      background: transparent;
      color: var(--ink);
      font: inherit;
      padding-right: 6px;
    }}
    .year-control {{
      display: none;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--line);
      background: #fffdfa;
      border-radius: 999px;
      padding: 7px 12px;
      color: var(--ink);
      font-size: 0.86rem;
    }}
    .year-control input[type="range"] {{
      width: 150px;
    }}
    .year-control strong {{
      min-width: 46px;
      text-align: right;
    }}
    .map-frame {{
      position: relative;
      border-radius: 24px;
      border: 1px solid #e4dccd;
      overflow: hidden;
      flex: 1;
      min-height: 0;
    }}
    #map {{
      width: 100%;
      height: 100%;
      border-radius: 24px;
    }}
    .tooltip {{
      position: absolute;
      pointer-events: none;
      background: rgba(18, 29, 34, 0.94);
      color: #fffdf7;
      padding: 12px 14px;
      border-radius: 14px;
      font-size: 0.9rem;
      line-height: 1.4;
      max-width: 290px;
      box-shadow: 0 12px 32px rgba(18, 29, 34, 0.22);
      opacity: 0;
      transform: translate(14px, 14px);
      transition: opacity 120ms ease;
      z-index: 1001;
    }}
    .footer-note {{
      color: var(--muted);
      font-size: 0.94rem;
      line-height: 1.45;
      flex-shrink: 0;
    }}
    .leaflet-control-zoom a {{
      border-radius: 10px !important;
    }}
    @media (max-width: 1120px) {{
      .shell {{ grid-template-columns: 1fr; height: auto; }}
      .layer-buttons {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .map-frame {{ min-height: 58vh; }}
    }}
    @media (max-width: 720px) {{
      .shell {{ padding: 12px; gap: 12px; }}
      .sidebar {{ padding: 16px; }}
      .layer-buttons {{ grid-template-columns: 1fr; }}
      .map-card {{ padding: 14px; }}
      .map-frame {{ min-height: 44vh; }}
      .year-control input[type="range"] {{ width: 120px; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <aside class="sidebar">
      <div>
        <div class="eyebrow">Presentation Build</div>
        <h1>NYC Outage Themes</h1>
        <p class="lede">Interactive map built directly from <code>NYC_Outage_Themes.gpkg</code>. Use the layer buttons to switch views, hover for details, and click a tract or county to zoom in.</p>
      </div>
      <div class="layer-buttons" id="layerButtons"></div>
      <section class="card">
        <h2 id="infoTitle"></h2>
        <p id="infoDescription"></p>
        <div class="legend-title" id="legendTitle"></div>
        <div class="legend" id="legend"></div>
        <p class="legend-note" id="legendNote"></p>
      </section>
    </aside>
    <main class="map-card">
      <div class="map-toolbar">
        <div>
          <div class="eyebrow" id="mapPart"></div>
          <h2 id="mapTitle"></h2>
          <p id="mapDescription"></p>
        </div>
        <div class="toolbar-actions">
          <label class="variant-control" id="variantControl">
            <span>Significance</span>
            <select id="variantSelect" aria-label="Select significance level"></select>
          </label>
          <label class="temporal-control" id="temporalMetricControl">
            <span>Metric</span>
            <select id="temporalMetricSelect" aria-label="Select temporal metric"></select>
          </label>
          <label class="year-control" id="temporalYearControl">
            <input id="temporalYearRange" type="range" min="0" max="0" step="1" value="0" aria-label="Select year">
            <strong id="temporalYearLabel"></strong>
          </label>
          <button type="button" id="temporalPlayPause" style="display:none;">Play</button>
          <button type="button" id="resetView">Reset view</button>
        </div>
      </div>
      <p class="map-summary" id="mapSummary"></p>
      <div class="map-frame">
        <div id="map"></div>
        <div class="tooltip" id="tooltip"></div>
      </div>
      <div class="footer-note">
        This file is fully self-contained. The same build also updates the repository root HTML and the GitHub Pages <code>docs/index.html</code>.
      </div>
    </main>
  </div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const GEOMETRY = {json.dumps(geometry_payload, separators=(",", ":"))};
    const LAYERS = {json.dumps(layers, separators=(",", ":"))};

    const map = L.map("map", {{ minZoom: 10 }});
    L.tileLayer("https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
      attribution: "&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a> contributors",
      maxZoom: 19,
    }}).addTo(map);
    const layerButtons = document.getElementById("layerButtons");
    const temporalMetricControl = document.getElementById("temporalMetricControl");
    const temporalMetricSelect = document.getElementById("temporalMetricSelect");
    const temporalYearControl = document.getElementById("temporalYearControl");
    const temporalYearRange = document.getElementById("temporalYearRange");
    const temporalYearLabel = document.getElementById("temporalYearLabel");
    const temporalPlayPause = document.getElementById("temporalPlayPause");
    const tractById = Object.fromEntries(GEOMETRY.tracts.features.map((f) => [f.properties.id, f.properties]));
    let temporalTimer = null;

    const tooltip = document.getElementById("tooltip");
    const featureToLayer = {{}};
    let currentLayer = null;
    let hoveredLayer = null;

    function darkenHex(hex, factor) {{
      const h = (hex || "#d9d2c3").replace(/^#/, "");
      if (h.length !== 6) return hex;
      const r = Math.round(parseInt(h.slice(0, 2), 16) * factor);
      const g = Math.round(parseInt(h.slice(2, 4), 16) * factor);
      const b = Math.round(parseInt(h.slice(4, 6), 16) * factor);
      return "rgb(" + r + "," + g + "," + b + ")";
    }}

    function setupFeatureEvents(feature, layer) {{
      const id = feature.properties.id;
      featureToLayer[id] = layer;
      layer._tooltip_html = "";
      layer._original_fill = "#d9d2c3";
      layer.on("mouseover", () => {{
        if (hoveredLayer && hoveredLayer !== layer) {{
          hoveredLayer.setStyle({{ fillColor: hoveredLayer._original_fill, fillOpacity: 0.75 }});
        }}
        hoveredLayer = layer;
        const isCvi = currentLayer && currentLayer.id === "part07_cvi";
        const hoverFill = isCvi ? "#000000" : darkenHex(layer._original_fill, 0.70);
        layer.setStyle({{ fillColor: hoverFill, fillOpacity: 0.95 }});
        layer.bringToFront();
      }});
      layer.on("mousemove", (e) => {{
        if (!layer._tooltip_html) return;
        tooltip.innerHTML = layer._tooltip_html;
        tooltip.style.opacity = "1";
        tooltip.style.left = (e.containerPoint.x + 18) + "px";
        tooltip.style.top = (e.containerPoint.y + 18) + "px";
      }});
      layer.on("mouseout", () => {{
        layer.setStyle({{ fillColor: layer._original_fill, fillOpacity: 0.75 }});
        if (hoveredLayer === layer) hoveredLayer = null;
        tooltip.style.opacity = "0";
      }});
      layer.on("click", (e) => {{ map.fitBounds(e.target.getBounds(), {{ padding: [20, 20] }}); }});
    }}

    const tractGeoLayer = L.geoJSON(GEOMETRY.tracts, {{
      style: () => ({{ fillColor: "#d9d2c3", fillOpacity: 0.75, color: "rgba(34,46,51,0.52)", weight: 0.42 }}),
      onEachFeature: setupFeatureEvents,
    }});
    const countyGeoLayer = L.geoJSON(GEOMETRY.counties, {{
      style: () => ({{ fillColor: "#d9d2c3", fillOpacity: 0.75, color: "rgba(18,29,34,0.85)", weight: 1.35 }}),
      onEachFeature: setupFeatureEvents,
    }});

    tractGeoLayer.addTo(map);
    map.setView([40.655, -73.94], 10);

    function applyStyles(group, styles) {{
      const geoLayer = group === "tracts" ? tractGeoLayer : countyGeoLayer;
      geoLayer.eachLayer((l) => {{
        const id = l.feature.properties.id;
        const style = styles[id] || {{}};
        const fill = style.fill || "#d9d2c3";
        l.setStyle({{ fillColor: fill }});
        l._original_fill = fill;
        l._tooltip_html = style.tooltip || "";
      }});
    }}

    function toggleGroups(group) {{
      if (group === "tracts") {{
        if (!map.hasLayer(tractGeoLayer)) map.addLayer(tractGeoLayer);
        if (map.hasLayer(countyGeoLayer)) map.removeLayer(countyGeoLayer);
      }} else {{
        if (!map.hasLayer(countyGeoLayer)) map.addLayer(countyGeoLayer);
        if (map.hasLayer(tractGeoLayer)) map.removeLayer(tractGeoLayer);
      }}
    }}

    function getActiveLayerView(layer) {{
      if (!layer.variants) return layer;
      const variantId = variantSelect.value || layer.defaultVariant || Object.keys(layer.variants)[0];
      return {{ ...layer, ...(layer.variants[variantId] || {{}}) }};
    }}

    function updateVariantControl(layer) {{
      if (!layer.variants) {{
        variantControl.style.display = "none";
        variantSelect.innerHTML = "";
        return;
      }}
      variantControl.style.display = "inline-flex";
      const selectedValue = variantSelect.value || layer.defaultVariant || Object.keys(layer.variants)[0];
      variantSelect.innerHTML = "";
      Object.entries(layer.variants).forEach(([variantId, variant]) => {{
        const option = document.createElement("option");
        option.value = variantId;
        option.textContent = variant.label || variantId;
        option.selected = variantId === selectedValue;
        variantSelect.appendChild(option);
      }});
      if (!layer.variants[variantSelect.value]) {{
        variantSelect.value = layer.defaultVariant || Object.keys(layer.variants)[0];
      }}
    }}

    function updateLegend(items) {{
      legend.innerHTML = "";
      items.forEach((item) => {{
        const row = document.createElement("div");
        row.className = "legend-item";
        row.innerHTML = `<span class="legend-swatch" style="background:${{item.color}}"></span><span>${{item.label}}</span>`;
        legend.appendChild(row);
      }});
    }}

    function updateLegendMeta(layer) {{
      legendTitle.textContent = layer.legendTitle || "Legend";
      legendNote.textContent = layer.legendNote || "";
      legendNote.style.display = layer.legendNote ? "block" : "none";
    }}

    function stopTemporalPlayback() {{
      if (temporalTimer) {{
        clearInterval(temporalTimer);
        temporalTimer = null;
      }}
      temporalPlayPause.textContent = "Play";
    }}

    function updateTemporalControl(layer) {{
      const isTemporal = Boolean(layer.temporal);
      temporalMetricControl.style.display = isTemporal ? "inline-flex" : "none";
      temporalYearControl.style.display = isTemporal ? "inline-flex" : "none";
      temporalPlayPause.style.display = isTemporal ? "inline-block" : "none";
      if (!isTemporal) {{
        stopTemporalPlayback();
        return;
      }}

      const temporal = layer.temporal;
      const selectedMetric = temporalMetricSelect.value || temporal.defaultMetric || Object.keys(temporal.metricOptions)[0];
      temporalMetricSelect.innerHTML = "";
      Object.entries(temporal.metricOptions).forEach(([metricKey, metricMeta]) => {{
        const option = document.createElement("option");
        option.value = metricKey;
        option.textContent = metricMeta.label || metricKey;
        option.selected = metricKey === selectedMetric;
        temporalMetricSelect.appendChild(option);
      }});
      if (!temporal.metricOptions[temporalMetricSelect.value]) {{
        temporalMetricSelect.value = temporal.defaultMetric || Object.keys(temporal.metricOptions)[0];
      }}

      temporalYearRange.min = "0";
      temporalYearRange.max = String(Math.max(0, temporal.years.length - 1));
      const selectedYear = temporalYearLabel.dataset.year || temporal.defaultYear || temporal.years[0];
      const idx = Math.max(0, temporal.years.indexOf(String(selectedYear)));
      temporalYearRange.value = String(idx);
      temporalYearLabel.textContent = temporal.years[idx] || "";
      temporalYearLabel.dataset.year = temporalYearLabel.textContent;
    }}

    function buildTemporalLegend(metricMeta) {{
      if (!metricMeta || !metricMeta.bins || metricMeta.bins.length < 2) return [];
      const rows = [];
      for (let index = 0; index < metricMeta.bins.length - 1; index += 1) {{
        rows.push({{
          label: `${{metricMeta.bins[index].toFixed(metricMeta.digits)}} to ${{metricMeta.bins[index + 1].toFixed(metricMeta.digits)}}`,
          color: metricMeta.palette[index],
        }});
      }}
      rows.push({{ label: "No data", color: "#d9d2c3" }});
      return rows;
    }}

    function getBinIndex(value, bins) {{
      if (value === null || value === undefined || !bins || bins.length < 2) return -1;
      for (let index = 0; index < bins.length - 1; index += 1) {{
        if (value <= bins[index + 1] || index === bins.length - 2) {{
          return index;
        }}
      }}
      return bins.length - 2;
    }}

    function applyTemporalLayer(layer) {{
      const temporal = layer.temporal;
      const metricKey = temporalMetricSelect.value;
      const metricMeta = temporal.metricOptions[metricKey];
      const year = temporal.years[Number(temporalYearRange.value)] || temporal.defaultYear;
      temporalYearLabel.textContent = year;
      temporalYearLabel.dataset.year = year;

      let observedCount = 0;
      let totalValue = 0;
      tractGeoLayer.eachLayer((l) => {{
        const featureId = l.feature.properties.id;
        const yearEntry = ((temporal.values[featureId] || {{}})[year] || {{}});
        const value = yearEntry[metricKey];
        let fill = "#d9d2c3";
        if (value !== null && value !== undefined) {{
          const binIndex = getBinIndex(Number(value), metricMeta.bins);
          fill = metricMeta.palette[binIndex];
          observedCount += 1;
          totalValue += Number(value);
        }}
        l.setStyle({{ fillColor: fill }});
        l._original_fill = fill;
        const props = l.feature.properties;
        const formatted = value === null || value === undefined ? "No data" : Number(value).toFixed(metricMeta.digits);
        l._tooltip_html = `<strong>Tract ${{props.label || featureId.replace("tract-", "")}}</strong><br>County: ${{props.countyName || "No data"}}<br>Year: ${{year}}<br>${{metricMeta.label}}: ${{formatted}} ${{metricMeta.unit}}`;
      }});

      updateLegend(buildTemporalLegend(metricMeta));
      updateLegendMeta({{
        legendTitle: layer.legendTitle,
        legendNote: `${{layer.legendNote}} Selected metric: ${{metricMeta.label}}.`,
      }});
      infoTitle.textContent = currentLayer.title;
      infoDescription.textContent = `${{currentLayer.description}} Selected metric: ${{metricMeta.label}}.`;
      mapSummary.textContent = `${{observedCount}} tracts with data in ${{year}}. Citywide total: ${{totalValue.toFixed(metricMeta.digits)}} ${{metricMeta.unit}}.`;
      mapPart.textContent = currentLayer.part;
      mapTitle.textContent = `${{currentLayer.title}} (${{year}})`;
      mapDescription.textContent = metricMeta.description;
    }}


    function renderCurrentLayer() {{
      if (!currentLayer) return;
      if (currentLayer.temporal) {{
        toggleGroups("tracts");
        applyTemporalLayer(currentLayer);
        resetView();
        return;
      }}
      const layer = getActiveLayerView(currentLayer);
      toggleGroups(layer.group);
      applyStyles(layer.group, layer.styles);
      updateLegend(layer.legend);
      updateLegendMeta(layer);
      infoTitle.textContent = currentLayer.title;
      infoDescription.textContent = layer.description || currentLayer.description;
      mapPart.textContent = currentLayer.part;
      mapTitle.textContent = currentLayer.title;
      mapDescription.textContent = layer.description || currentLayer.description;
      mapSummary.textContent = layer.summary || currentLayer.summary || "";
    }}

    function buildSelect() {{
      LAYERS.forEach((layer) => {{
        const button = document.createElement("button");
        button.type = "button";
        button.className = "layer-button";
        button.dataset.layerId = layer.id;
        button.innerHTML = `<span class="layer-part">${{layer.buttonPart || layer.part}}</span><span class="layer-title">${{layer.buttonTitle || layer.title}}</span>`;
        button.addEventListener("click", () => setLayer(layer.id));
        layerButtons.appendChild(button);
      }});
    }}

    function setLayer(layerId) {{
      const layer = LAYERS.find((candidate) => candidate.id === layerId);
      if (!layer) return;
      stopTemporalPlayback();
      currentLayer = layer;
      document.querySelectorAll(".layer-button").forEach((button) => {{
        button.classList.toggle("active", button.dataset.layerId === layerId);
      }});
      updateVariantControl(layer);
      updateTemporalControl(layer);
      renderCurrentLayer();
    }}

    const legend = document.getElementById("legend");
    const legendTitle = document.getElementById("legendTitle");
    const legendNote = document.getElementById("legendNote");
    const variantControl = document.getElementById("variantControl");
    const variantSelect = document.getElementById("variantSelect");
    const infoTitle = document.getElementById("infoTitle");
    const infoDescription = document.getElementById("infoDescription");
    const mapPart = document.getElementById("mapPart");
    const mapTitle = document.getElementById("mapTitle");
    const mapDescription = document.getElementById("mapDescription");
    const mapSummary = document.getElementById("mapSummary");

    function resetView() {{ map.setView([40.655, -73.94], 10); }}

    buildSelect();
    variantSelect.addEventListener("change", renderCurrentLayer);
    temporalMetricSelect.addEventListener("change", renderCurrentLayer);
    temporalYearRange.addEventListener("input", () => {{
      stopTemporalPlayback();
      renderCurrentLayer();
    }});
    temporalPlayPause.addEventListener("click", () => {{
      if (!currentLayer || !currentLayer.temporal) return;
      if (temporalTimer) {{
        stopTemporalPlayback();
        return;
      }}
      temporalPlayPause.textContent = "Pause";
      temporalTimer = setInterval(() => {{
        const next = Number(temporalYearRange.value) + 1;
        const max = Number(temporalYearRange.max);
        temporalYearRange.value = String(next > max ? 0 : next);
        renderCurrentLayer();
      }}, 1100);
    }});
    document.getElementById("resetView").addEventListener("click", resetView);
    setLayer((LAYERS.find((layer) => layer.id === "part00_annual_temporal") || LAYERS[0]).id);
  </script>
</body>
</html>
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")


def main() -> None:
    args = parse_args()
    gpkg_path = resolve_existing_file([args.gpkg])
    csv_path = resolve_existing_file([args.significance_csv])
    annual_csv_path = resolve_existing_file([args.annual_csv])

    if not args.skip_sync:
        sync_significance_to_gpkg(gpkg_path, csv_path)

    connection = sqlite3.connect(gpkg_path)
    connection.row_factory = sqlite3.Row
    try:
        tract_records = load_tract_geometry(connection)
        county_records = load_county_geometry(connection)
        bounds = base.compute_bounds(tract_records + county_records)
        map_width, map_height = base.enrich_geometry(tract_records + county_records, bounds)

        layers = [
            build_annual_temporal_layer(annual_csv_path),
            build_sw_significance_layer(connection),
            build_cluster_layer(connection),
            build_cvi_layer(connection),
            build_priority_layer(connection),
            build_high_cvi_share_layer(connection),
            build_duration_layer(connection),
        ]
    finally:
        connection.close()

    county_layer = next(layer for layer in layers if layer["id"] == "part08_high_cvi_share")
    write_html(tract_records, county_records, layers, map_width, map_height, args.html_path)
    write_html(tract_records, county_records, layers, map_width, map_height, args.docs_index_path)
    base.render_county_static_map(county_records, county_layer, args.county_png_path)
    base.build_preview_collage(args.preview_png_path, args.county_png_path, args.html_path)

    print(f"Wrote {args.html_path}")
    print(f"Wrote {args.docs_index_path}")
    print(f"Wrote {args.county_png_path}")
    print(f"Wrote {args.preview_png_path}")


if __name__ == "__main__":
    main()
