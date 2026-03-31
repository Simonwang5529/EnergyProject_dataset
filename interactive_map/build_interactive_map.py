#!/usr/bin/env python3

from __future__ import annotations

import json
import math
import sqlite3
import struct
from collections import Counter
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
GPKG_PATH = ROOT / "NYC_Outage_Themes.gpkg"


def parse_wkb(data: bytes, offset: int = 0) -> tuple[dict[str, Any], int]:
    little_endian = data[offset] == 1
    endian = "<" if little_endian else ">"
    offset += 1
    geom_type = struct.unpack_from(f"{endian}I", data, offset)[0]
    offset += 4

    base_type = geom_type % 1000

    if base_type == 3:
        ring_count = struct.unpack_from(f"{endian}I", data, offset)[0]
        offset += 4
        rings = []
        for _ in range(ring_count):
            point_count = struct.unpack_from(f"{endian}I", data, offset)[0]
            offset += 4
            ring = []
            for _ in range(point_count):
                x, y = struct.unpack_from(f"{endian}dd", data, offset)
                offset += 16
                ring.append((x, y))
            rings.append(ring)
        return {"type": "Polygon", "coordinates": rings}, offset

    if base_type == 6:
        polygon_count = struct.unpack_from(f"{endian}I", data, offset)[0]
        offset += 4
        polygons = []
        for _ in range(polygon_count):
            polygon, offset = parse_wkb(data, offset)
            polygons.append(polygon["coordinates"])
        return {"type": "MultiPolygon", "coordinates": polygons}, offset

    raise ValueError(f"Unsupported WKB geometry type: {geom_type}")


def parse_gpkg_geometry(blob: bytes) -> dict[str, Any]:
    if blob[:2] != b"GP":
        raise ValueError("Invalid GeoPackage geometry header.")

    flags = blob[3]
    envelope_code = (flags >> 1) & 0b111
    header_size = 8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(envelope_code, 0)
    geometry, _ = parse_wkb(blob[header_size:])
    return geometry


def iter_polygons(geometry: dict[str, Any]) -> list[list[list[tuple[float, float]]]]:
    if geometry["type"] == "Polygon":
        return [geometry["coordinates"]]
    if geometry["type"] == "MultiPolygon":
        return geometry["coordinates"]
    raise ValueError(f"Unsupported geometry type: {geometry['type']}")


def format_float(value: Any, digits: int = 3) -> str:
    if value is None:
        return "No data"
    return f"{float(value):.{digits}f}"


def format_pct(value: Any, digits: int = 1) -> str:
    if value is None:
        return "No data"
    return f"{float(value) * 100:.{digits}f}%"


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def compute_bounds(records: list[dict[str, Any]]) -> tuple[float, float, float, float]:
    xs: list[float] = []
    ys: list[float] = []
    for record in records:
        for polygon in iter_polygons(record["geometry"]):
            for ring in polygon:
                for x, y in ring:
                    xs.append(x)
                    ys.append(y)
    return min(xs), min(ys), max(xs), max(ys)


def enrich_geometry(
    records: list[dict[str, Any]],
    bounds: tuple[float, float, float, float],
    map_width: int = 1080,
    map_height: int = 900,
    padding: int = 28,
) -> tuple[int, int]:
    min_x, min_y, max_x, max_y = bounds
    span_x = max_x - min_x
    span_y = max_y - min_y
    scale = min((map_width - 2 * padding) / span_x, (map_height - 2 * padding) / span_y)

    def project(point: tuple[float, float]) -> tuple[float, float]:
        x, y = point
        px = padding + (x - min_x) * scale
        py = padding + (max_y - y) * scale
        return round(px, 1), round(py, 1)

    for record in records:
        path_parts: list[str] = []
        projected_polygons: list[list[list[tuple[float, float]]]] = []
        xs: list[float] = []
        ys: list[float] = []

        for polygon in iter_polygons(record["geometry"]):
            projected_rings = []
            for ring in polygon:
                projected_ring = [project(point) for point in ring]
                if projected_ring:
                    xs.extend(point[0] for point in projected_ring)
                    ys.extend(point[1] for point in projected_ring)
                    move = f"M {projected_ring[0][0]} {projected_ring[0][1]}"
                    lines = " ".join(f"L {x} {y}" for x, y in projected_ring[1:])
                    path_parts.append(f"{move} {lines} Z")
                projected_rings.append(projected_ring)
            projected_polygons.append(projected_rings)

        record["path"] = " ".join(path_parts)
        record["projected_polygons"] = projected_polygons
        record["bbox"] = [
            round(min(xs), 1),
            round(min(ys), 1),
            round(max(xs) - min(xs), 1),
            round(max(ys) - min(ys), 1),
        ]

    return map_width, map_height


def equal_interval_bins(values: list[float], count: int = 5) -> list[float]:
    minimum = min(values)
    maximum = max(values)
    if math.isclose(minimum, maximum):
        return [minimum, maximum]
    step = (maximum - minimum) / count
    edges = [minimum]
    for index in range(1, count):
        edges.append(minimum + step * index)
    edges.append(maximum)
    return edges


def bin_index(value: float, edges: list[float]) -> int:
    if len(edges) <= 2:
        return 0
    for index in range(len(edges) - 1):
        upper = edges[index + 1]
        if value <= upper or index == len(edges) - 2:
            return index
    return len(edges) - 2


def bin_label(edges: list[float], index: int, digits: int = 2) -> str:
    start = edges[index]
    end = edges[index + 1]
    return f"{start:.{digits}f} to {end:.{digits}f}"


def discrete_legend(counter: Counter[str], color_map: dict[str, str], order: list[str]) -> list[dict[str, str]]:
    items = []
    for label in order:
        if label in counter:
            items.append({"label": f"{label} ({counter[label]})", "color": color_map[label]})
    return items


def load_tract_geometry(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        select tract_fips, county_name, geom
        from tract_clusters
        order by tract_fips
        """
    ).fetchall()
    records = []
    for row in rows:
        tract_fips = row["tract_fips"]
        records.append(
            {
                "id": f"tract-{tract_fips}",
                "label": tract_fips,
                "county_name": row["county_name"],
                "geometry": parse_gpkg_geometry(row["geom"]),
            }
        )
    return records


def load_county_geometry(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        select county_fips, county_name, geom
        from county_duration_2023
        order by county_fips
        """
    ).fetchall()
    records = []
    for row in rows:
        county_fips = row["county_fips"]
        records.append(
            {
                "id": f"county-{county_fips}",
                "label": row["county_name"],
                "county_name": row["county_name"],
                "geometry": parse_gpkg_geometry(row["geom"]),
            }
        )
    return records


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
        raw_cluster = row["cluster"]
        if raw_cluster is None:
            label = "No cluster"
        else:
            label = f"Cluster {int(raw_cluster)}"
        counts[label] += 1
        dom_id = f"tract-{row['tract_fips']}"
        tooltip = (
            f"<strong>Tract {row['tract_fips']}</strong><br>"
            f"County: {row['county_name']}<br>"
            f"Seasonality type: {label}<br>"
            f"Unique outage events (2020-2023): {int(row['total_events']) if row['total_events'] is not None else 'No data'}<br>"
            f"Overall CVI: {format_float(row['cvi_overall'])}"
        )
        styles[dom_id] = {"fill": color_map[label], "tooltip": tooltip}

    summary = (
        f"{counts.get('Cluster 0', 0) + counts.get('Cluster 1', 0) + counts.get('Cluster 2', 0) + counts.get('Cluster 3', 0)} "
        f"tracts are assigned to four seasonality clusters, with {counts.get('No cluster', 0)} tracts left unclassified."
    )

    return {
        "id": "part06_clusters",
        "part": "Part 06",
        "title": "Tract Outage Seasonality Clusters",
        "description": "Categorical cluster map showing where outage seasonality patterns differ across NYC tracts.",
        "summary": summary,
        "group": "tracts",
        "legend": discrete_legend(counts, color_map, ["Cluster 0", "Cluster 1", "Cluster 2", "Cluster 3", "No cluster"]),
        "styles": styles,
    }


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
        order by tract_fips
        """
    ).fetchall()

    palette = ["#f7fbff", "#c6dbef", "#6baed6", "#3182bd", "#08519c"]
    values = [float(row["cvi_overall"]) for row in rows if row["cvi_overall"] is not None]
    edges = equal_interval_bins(values)

    styles: dict[str, dict[str, str]] = {}
    counts: Counter[str] = Counter()

    for row in rows:
        dom_id = f"tract-{row['tract_fips']}"
        if row["cvi_overall"] is None:
            label = "No data"
            fill = "#d9d2c3"
        else:
            index = bin_index(float(row["cvi_overall"]), edges)
            label = bin_label(edges, index, digits=3)
            fill = palette[index]
        counts[label] += 1
        tooltip = (
            f"<strong>Tract {row['tract_fips']}</strong><br>"
            f"County: {row['county_name']}<br>"
            f"Overall CVI: {format_float(row['cvi_overall'])}<br>"
            f"Baseline social/economic: {format_float(row['cvi_baseline_social_econ'])}<br>"
            f"Climate extreme events: {format_float(row['cvi_climate_extreme_events'])}"
        )
        styles[dom_id] = {"fill": fill, "tooltip": tooltip}

    legend = [{"label": f"{bin_label(edges, index, digits=3)} ({counts[bin_label(edges, index, digits=3)]})", "color": palette[index]} for index in range(len(edges) - 1)]

    return {
        "id": "part07_cvi",
        "part": "Part 07",
        "title": "Overall Climate Vulnerability Index",
        "description": "Tract-level choropleth for the overall CVI score, highlighting the structural vulnerability landscape.",
        "summary": f"Overall CVI ranges from {min(values):.3f} to {max(values):.3f} across 2,325 tracts.",
        "group": "tracts",
        "legend": legend,
        "styles": styles,
    }


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
        order by tract_fips
        """
    ).fetchall()

    color_map = {
        "Priority tract": "#b2182b",
        "Not flagged": "#ef8a62",
        "No data": "#d9d2c3",
    }
    counts: Counter[str] = Counter()
    styles: dict[str, dict[str, str]] = {}

    for row in rows:
        dom_id = f"tract-{row['tract_fips']}"
        if row["priority_flag"] is None:
            label = "No data"
        elif int(row["priority_flag"]) == 1:
            label = "Priority tract"
        else:
            label = "Not flagged"
        counts[label] += 1
        tooltip = (
            f"<strong>Tract {row['tract_fips']}</strong><br>"
            f"County: {row['county_name']}<br>"
            f"Priority status: {label}<br>"
            f"Occurrence slope per year: {format_float(row['occ_slope_per_year'])}<br>"
            f"Average outage occurrence: {format_float(row['avg_outage_occurrence'])}<br>"
            f"Overall CVI: {format_float(row['cvi_overall'])}<br>"
            f"Total outages: {format_float(row['total_outages'], digits=0)}"
        )
        styles[dom_id] = {"fill": color_map[label], "tooltip": tooltip}

    summary = f"{counts.get('Priority tract', 0)} tracts are flagged as overlap priorities where vulnerability and outage burden are jointly elevated."

    return {
        "id": "part07_priority",
        "part": "Part 07",
        "title": "Priority Overlap: Vulnerability + Outage Burden",
        "description": "Rule-based priority map highlighting tracts where high vulnerability overlaps with elevated outage burden.",
        "summary": summary,
        "group": "tracts",
        "legend": discrete_legend(counts, color_map, ["Priority tract", "Not flagged", "No data"]),
        "styles": styles,
    }


def build_high_cvi_share_layer(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        select
            county_fips,
            county_name,
            high_cvi_share,
            cvi_overall_mean,
            cvi_climate_extreme_events_mean
        from county_duration_2023
        order by county_fips
        """
    ).fetchall()

    palette = ["#fff5eb", "#fdc87e", "#fc8d59", "#d7301f", "#7f0000"]
    values = [float(row["high_cvi_share"]) for row in rows if row["high_cvi_share"] is not None]
    edges = equal_interval_bins(values)
    counts: Counter[str] = Counter()
    styles: dict[str, dict[str, str]] = {}

    for row in rows:
        dom_id = f"county-{row['county_fips']}"
        if row["high_cvi_share"] is None:
            fill = "#d9d2c3"
            label = "No data"
        else:
            index = bin_index(float(row["high_cvi_share"]), edges)
            fill = palette[index]
            label = bin_label(edges, index, digits=2)
        counts[label] += 1
        tooltip = (
            f"<strong>{row['county_name']} County</strong><br>"
            f"High-CVI tract share: {format_pct(row['high_cvi_share'])}<br>"
            f"Mean overall CVI: {format_float(row['cvi_overall_mean'])}<br>"
            f"Mean climate extreme-events CVI: {format_float(row['cvi_climate_extreme_events_mean'])}"
        )
        styles[dom_id] = {"fill": fill, "tooltip": tooltip}

    top_county = max(rows, key=lambda row: row["high_cvi_share"])

    return {
        "id": "part08_high_cvi_share",
        "part": "Part 08",
        "title": "County Vulnerability Concentration",
        "description": "County choropleth showing the share of tracts classified as highly vulnerable on the CVI.",
        "summary": f"{top_county['county_name']} has the highest share of high-CVI tracts at {format_pct(top_county['high_cvi_share'])}.",
        "group": "counties",
        "legend": [{"label": f"{bin_label(edges, index, digits=2)} ({counts[bin_label(edges, index, digits=2)]})", "color": palette[index]} for index in range(len(edges) - 1)],
        "styles": styles,
    }


def build_duration_layer(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        select
            county_fips,
            county_name,
            duration_p90,
            long_share_8h,
            long_share_24h,
            severe_weather_share
        from county_duration_2023
        order by county_fips
        """
    ).fetchall()

    palette = ["#edf8fb", "#b2e2e2", "#66c2a4", "#2ca25f", "#006d2c"]
    values = [float(row["duration_p90"]) for row in rows if row["duration_p90"] is not None]
    edges = equal_interval_bins(values)
    counts: Counter[str] = Counter()
    styles: dict[str, dict[str, str]] = {}

    for row in rows:
        dom_id = f"county-{row['county_fips']}"
        if row["duration_p90"] is None:
            fill = "#d9d2c3"
            label = "No data"
        else:
            index = bin_index(float(row["duration_p90"]), edges)
            fill = palette[index]
            label = bin_label(edges, index, digits=2)
        counts[label] += 1
        tooltip = (
            f"<strong>{row['county_name']} County</strong><br>"
            f"2023 duration p90: {format_float(row['duration_p90'], digits=2)} hours<br>"
            f"Long outages over 8h: {format_pct(row['long_share_8h'])}<br>"
            f"Long outages over 24h: {format_pct(row['long_share_24h'])}<br>"
            f"Severe-weather share: {format_pct(row['severe_weather_share'])}"
        )
        styles[dom_id] = {"fill": fill, "tooltip": tooltip}

    return {
        "id": "part10_duration",
        "part": "Part 10",
        "title": "County Duration Risk (2023)",
        "description": "County map for prolonged outage severity using the 90th-percentile outage duration.",
        "summary": f"County duration p90 ranges from {min(values):.2f} to {max(values):.2f} hours in the 2023 extract.",
        "group": "counties",
        "legend": [{"label": f"{bin_label(edges, index, digits=2)} ({counts[bin_label(edges, index, digits=2)]})", "color": palette[index]} for index in range(len(edges) - 1)],
        "styles": styles,
    }


def make_geometry_payload(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload = []
    for record in records:
        payload.append(
            {
                "id": record["id"],
                "label": record["label"],
                "countyName": record["county_name"],
                "path": record["path"],
                "bbox": record["bbox"],
            }
        )
    return payload


def load_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    font_candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Helvetica.ttc",
    ]
    for candidate in font_candidates:
        path = Path(candidate)
        if path.exists():
            return ImageFont.truetype(str(path), size=size)
    return ImageFont.load_default()


def render_county_static_map(
    county_records: list[dict[str, Any]],
    layer: dict[str, Any],
    output_path: Path,
) -> None:
    width = 1600
    height = 1080
    map_left = 60
    map_top = 140
    sidebar_left = 1170
    image = Image.new("RGB", (width, height), "#fcfbf7")
    draw = ImageDraw.Draw(image)

    title_font = load_font(40, bold=True)
    body_font = load_font(22)
    small_font = load_font(18)

    draw.text((60, 40), "Part 08: County Vulnerability Concentration", fill="#1f2a2e", font=title_font)
    draw.text((60, 92), "High-CVI tract share by county, derived from the GeoPackage county layer.", fill="#49565c", font=body_font)

    draw.rounded_rectangle((sidebar_left, 120, width - 50, height - 70), radius=28, fill="#f0ede6", outline="#ddd6c8", width=2)
    draw.text((sidebar_left + 28, 150), "Legend", fill="#1f2a2e", font=body_font)

    for polygon_record in county_records:
        feature_style = layer["styles"].get(polygon_record["id"], {"fill": "#d9d2c3"})
        fill = feature_style["fill"]
        for polygon in polygon_record["projected_polygons"]:
            outer_ring = [(x + map_left, y + map_top) for x, y in polygon[0]]
            if len(outer_ring) >= 3:
                draw.polygon(outer_ring, fill=fill, outline="#3a474d")
            for inner_ring in polygon[1:]:
                shifted_inner = [(x + map_left, y + map_top) for x, y in inner_ring]
                if len(inner_ring) >= 3:
                    draw.polygon(shifted_inner, fill="#fcfbf7", outline="#3a474d")

    for record in county_records:
        x, y, w, h = record["bbox"]
        label_position = (x + map_left + (w / 2), y + map_top + (h / 2))
        anchor = "mm"
        draw.text(label_position, record["label"], fill="#1f2a2e", font=small_font, anchor=anchor)

    legend_y = 205
    for item in layer["legend"]:
        draw.rounded_rectangle((sidebar_left + 30, legend_y, sidebar_left + 66, legend_y + 24), radius=4, fill=item["color"])
        draw.text((sidebar_left + 82, legend_y - 1), item["label"], fill="#1f2a2e", font=small_font)
        legend_y += 44

    draw.text((sidebar_left + 28, legend_y + 16), "Summary", fill="#1f2a2e", font=body_font)
    summary_lines = wrap_text(layer["summary"], body_font, width - sidebar_left - 95)
    text_y = legend_y + 58
    for line in summary_lines:
        draw.text((sidebar_left + 28, text_y), line, fill="#49565c", font=small_font)
        text_y += 28

    image.save(output_path)


def wrap_text(text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    dummy = Image.new("RGB", (10, 10))
    draw = ImageDraw.Draw(dummy)
    words = text.split()
    if not words:
        return [""]
    lines = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if draw.textlength(candidate, font=font) <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def build_preview_collage(
    output_path: Path,
    county_vulnerability_path: Path,
    html_path: Path,
) -> None:
    cards = [
        ("Part 06: Seasonality clusters", ROOT / "tract_cluster_map.png"),
        ("Part 07: Overall CVI", ROOT / "tract_cvi_overall.png"),
        ("Part 07: Priority overlap", ROOT / "tract_priority_bivariate.png"),
        ("Part 08: High-CVI tract share", county_vulnerability_path),
        ("Part 10: Duration risk", ROOT / "county_duration_risk_2023.png"),
    ]

    canvas_width = 2880
    canvas_height = 1880
    margin = 56
    header_height = 140
    gutter = 36
    card_width = (canvas_width - 2 * margin - 2 * gutter) // 3
    card_height = 760

    image = Image.new("RGB", (canvas_width, canvas_height), "#f7f4ed")
    draw = ImageDraw.Draw(image)
    title_font = load_font(52, bold=True)
    subtitle_font = load_font(24)
    card_title_font = load_font(28, bold=True)
    body_font = load_font(22)

    draw.text((margin, 34), "Updated Map Tasks Preview", fill="#1f2a2e", font=title_font)
    draw.text((margin, 92), "This montage pairs the updated part 06 to part 10 layers with the interactive HTML deliverable.", fill="#49565c", font=subtitle_font)

    positions = []
    for row_index in range(2):
        for col_index in range(3):
            left = margin + col_index * (card_width + gutter)
            top = header_height + margin + row_index * (card_height + gutter)
            positions.append((left, top))

    for (title, path), (left, top) in zip(cards, positions):
        draw.rounded_rectangle((left, top, left + card_width, top + card_height), radius=28, fill="#ffffff", outline="#ddd6c8", width=2)
        draw.text((left + 28, top + 24), title, fill="#1f2a2e", font=card_title_font)
        card_image = Image.open(path).convert("RGB")
        target_box = (card_width - 56, card_height - 118)
        card_image.thumbnail(target_box, Image.Resampling.LANCZOS)
        paste_left = left + (card_width - card_image.width) // 2
        paste_top = top + 88 + (target_box[1] - card_image.height) // 2
        image.paste(card_image, (paste_left, paste_top))

    note_left, note_top = positions[-1]
    draw.rounded_rectangle((note_left, note_top, note_left + card_width, note_top + card_height), radius=28, fill="#1f2a2e")
    draw.text((note_left + 28, note_top + 26), "Interactive File", fill="#fffaf0", font=card_title_font)
    note_lines = [
        "Open the HTML file in a browser for layer toggles,",
        "hover details, and click-to-zoom interaction.",
        "",
        f"HTML: {html_path.name}",
        "",
        "Manual finish steps:",
        "1. Upload the HTML + PNG preview to Google Drive.",
        "2. Add this preview image to your summary doc/slides.",
        "3. Share the HTML link during the presentation.",
    ]
    text_y = note_top + 86
    for line in note_lines:
        draw.text((note_left + 28, text_y), line, fill="#d9e2e7", font=body_font)
        text_y += 40

    image.save(output_path)


def write_html(
    tract_geometry: list[dict[str, Any]],
    county_geometry: list[dict[str, Any]],
    layers: list[dict[str, Any]],
    map_width: int,
    map_height: int,
    output_path: Path,
) -> None:
    geometry_payload = {
        "tracts": make_geometry_payload(tract_geometry),
        "counties": make_geometry_payload(county_geometry),
    }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NYC Outage Themes Interactive Map</title>
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
    * {{
      box-sizing: border-box;
    }}
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
      grid-template-columns: minmax(300px, 380px) 1fr;
      gap: 28px;
      padding: 28px;
      min-height: 100vh;
    }}
    .sidebar {{
      background: linear-gradient(180deg, var(--panel) 0%, var(--panel-soft) 100%);
      color: #fdf8f1;
      border-radius: 28px;
      padding: 28px;
      box-shadow: var(--shadow);
      display: flex;
      flex-direction: column;
      gap: 24px;
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
      font-size: 2rem;
      line-height: 1.08;
    }}
    .lede {{
      margin: 12px 0 0 0;
      color: #d4e0e4;
      line-height: 1.5;
      font-size: 0.98rem;
    }}
    .layer-buttons {{
      display: grid;
      gap: 10px;
    }}
    .layer-button {{
      width: 100%;
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.06);
      color: #fdf8f1;
      text-align: left;
      padding: 14px 16px;
      cursor: pointer;
      transition: transform 140ms ease, border-color 140ms ease, background 140ms ease;
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
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #f3c89b;
      margin-bottom: 4px;
    }}
    .layer-title {{
      display: block;
      font-weight: 700;
      font-size: 0.96rem;
      line-height: 1.3;
    }}
    .card {{
      background: rgba(255, 255, 255, 0.07);
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: 22px;
      padding: 18px;
    }}
    .card h2 {{
      margin: 0 0 10px 0;
      font-size: 1.05rem;
    }}
    .card p {{
      margin: 0;
      color: #e6eef1;
      line-height: 1.5;
      font-size: 0.95rem;
    }}
    .legend {{
      display: grid;
      gap: 10px;
      margin-top: 16px;
    }}
    .legend-item {{
      display: flex;
      align-items: center;
      gap: 12px;
      color: #f7efe3;
      font-size: 0.92rem;
    }}
    .legend-swatch {{
      width: 18px;
      height: 18px;
      border-radius: 5px;
      border: 1px solid rgba(255, 255, 255, 0.22);
      flex: 0 0 auto;
    }}
    .map-card {{
      background: var(--card);
      border-radius: 28px;
      box-shadow: var(--shadow);
      padding: 22px;
      display: flex;
      flex-direction: column;
      gap: 16px;
      min-width: 0;
    }}
    .map-toolbar {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      flex-wrap: wrap;
    }}
    .map-toolbar h2 {{
      margin: 0;
      font-size: 1.45rem;
    }}
    .map-toolbar p {{
      margin: 6px 0 0 0;
      color: var(--muted);
      line-height: 1.45;
      max-width: 720px;
    }}
    .toolbar-actions {{
      display: flex;
      gap: 10px;
    }}
    .toolbar-actions button {{
      border: 1px solid var(--line);
      background: #fffdfa;
      border-radius: 999px;
      padding: 10px 14px;
      color: var(--ink);
      cursor: pointer;
      font: inherit;
    }}
    .map-frame {{
      position: relative;
      border-radius: 24px;
      overflow: hidden;
      background:
        linear-gradient(180deg, rgba(237, 233, 222, 0.95) 0%, rgba(245, 241, 232, 0.98) 100%);
      border: 1px solid #e4dccd;
      min-height: 72vh;
    }}
    svg {{
      width: 100%;
      height: auto;
      display: block;
    }}
    .feature {{
      cursor: pointer;
      transition: opacity 120ms ease, stroke-width 120ms ease;
      fill-rule: evenodd;
    }}
    .feature:hover {{
      opacity: 0.84;
    }}
    .tract-feature {{
      stroke: rgba(34, 46, 51, 0.52);
      stroke-width: 0.42;
    }}
    .county-feature {{
      stroke: rgba(18, 29, 34, 0.85);
      stroke-width: 1.35;
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
    }}
    .footer-note {{
      color: var(--muted);
      font-size: 0.94rem;
      line-height: 1.45;
    }}
    @media (max-width: 1120px) {{
      .shell {{
        grid-template-columns: 1fr;
      }}
      .map-frame {{
        min-height: 58vh;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <aside class="sidebar">
      <div>
        <div class="eyebrow">Presentation Build</div>
        <h1>NYC Outage Themes</h1>
        <p class="lede">Interactive map for the updated spatial tasks already bundled in <code>NYC_Outage_Themes.gpkg</code>. Use the layer buttons to move across the part 06 to part 10 views, hover for details, and click a tract or county to zoom in.</p>
      </div>

      <div class="layer-buttons" id="layerButtons"></div>

      <section class="card">
        <h2 id="infoTitle"></h2>
        <p id="infoDescription"></p>
        <div class="legend" id="legend"></div>
      </section>

      <section class="card">
        <h2>Summary</h2>
        <p id="infoSummary"></p>
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
          <button type="button" id="resetView">Reset view</button>
        </div>
      </div>

      <div class="map-frame">
        <svg id="mapSvg" viewBox="0 0 {map_width} {map_height}" aria-label="Interactive NYC outage map">
          <g id="tractGroup"></g>
          <g id="countyGroup"></g>
        </svg>
        <div class="tooltip" id="tooltip"></div>
      </div>

      <div class="footer-note">
        The file is self-contained and can be uploaded directly to Google Drive or opened locally in a browser. For the presentation deck, pair this HTML with the preview PNG generated in the same output folder.
      </div>
    </main>
  </div>

  <script>
    const GEOMETRY = {json.dumps(geometry_payload, separators=(",", ":"))};
    const LAYERS = {json.dumps(layers, separators=(",", ":"))};

    const svg = document.getElementById("mapSvg");
    const tractGroup = document.getElementById("tractGroup");
    const countyGroup = document.getElementById("countyGroup");
    const tooltip = document.getElementById("tooltip");
    const layerButtons = document.getElementById("layerButtons");
    const legend = document.getElementById("legend");

    const infoTitle = document.getElementById("infoTitle");
    const infoDescription = document.getElementById("infoDescription");
    const infoSummary = document.getElementById("infoSummary");
    const mapPart = document.getElementById("mapPart");
    const mapTitle = document.getElementById("mapTitle");
    const mapDescription = document.getElementById("mapDescription");

    const defaultViewBox = [0, 0, {map_width}, {map_height}];
    let currentLayer = null;

    function createPaths(groupName, target, items, className) {{
      items.forEach((item) => {{
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        path.setAttribute("d", item.path);
        path.setAttribute("class", `feature ${{className}}`);
        path.dataset.featureId = item.id;
        path.dataset.group = groupName;
        path.dataset.tooltip = "";
        path.style.fill = "#d9d2c3";
        path.addEventListener("mousemove", (event) => {{
          if (!path.dataset.tooltip) return;
          tooltip.innerHTML = path.dataset.tooltip;
          tooltip.style.opacity = "1";
          tooltip.style.left = `${{event.offsetX + 18}}px`;
          tooltip.style.top = `${{event.offsetY + 18}}px`;
        }});
        path.addEventListener("mouseleave", () => {{
          tooltip.style.opacity = "0";
        }});
        path.addEventListener("click", () => zoomToBBox(item.bbox));
        target.appendChild(path);
      }});
    }}

    function zoomToBBox(bbox) {{
      const [x, y, width, height] = bbox;
      const pad = 18;
      const safeWidth = Math.max(width, 18);
      const safeHeight = Math.max(height, 18);
      svg.setAttribute("viewBox", `${{x - pad}} ${{y - pad}} ${{safeWidth + pad * 2}} ${{safeHeight + pad * 2}}`);
    }}

    function resetView() {{
      svg.setAttribute("viewBox", defaultViewBox.join(" "));
    }}

    function buildButtons() {{
      LAYERS.forEach((layer) => {{
        const button = document.createElement("button");
        button.type = "button";
        button.className = "layer-button";
        button.dataset.layerId = layer.id;
        button.innerHTML = `<span class="layer-part">${{layer.part}}</span><span class="layer-title">${{layer.title}}</span>`;
        button.addEventListener("click", () => setLayer(layer.id));
        layerButtons.appendChild(button);
      }});
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

    function applyStyles(groupName, styles) {{
      const selector = groupName === "tracts" ? ".tract-feature" : ".county-feature";
      document.querySelectorAll(selector).forEach((path) => {{
        const featureId = path.dataset.featureId;
        const style = styles[featureId] || {{}};
        path.style.fill = style.fill || "#d9d2c3";
        path.dataset.tooltip = style.tooltip || "";
      }});
    }}

    function toggleGroups(groupName) {{
      const tractVisible = groupName === "tracts";
      tractGroup.style.display = tractVisible ? "block" : "none";
      countyGroup.style.display = tractVisible ? "none" : "block";
      tractGroup.style.pointerEvents = tractVisible ? "auto" : "none";
      countyGroup.style.pointerEvents = tractVisible ? "none" : "auto";
    }}

    function setLayer(layerId) {{
      const layer = LAYERS.find((candidate) => candidate.id === layerId);
      if (!layer) return;
      currentLayer = layer;
      document.querySelectorAll(".layer-button").forEach((button) => {{
        button.classList.toggle("active", button.dataset.layerId === layerId);
      }});
      toggleGroups(layer.group);
      applyStyles(layer.group, layer.styles);
      updateLegend(layer.legend);
      infoTitle.textContent = layer.title;
      infoDescription.textContent = layer.description;
      infoSummary.textContent = layer.summary;
      mapPart.textContent = layer.part;
      mapTitle.textContent = layer.title;
      mapDescription.textContent = layer.description;
      resetView();
    }}

    createPaths("tracts", tractGroup, GEOMETRY.tracts, "tract-feature");
    createPaths("counties", countyGroup, GEOMETRY.counties, "county-feature");
    buildButtons();
    document.getElementById("resetView").addEventListener("click", resetView);
    svg.addEventListener("dblclick", resetView);
    setLayer(LAYERS[0].id);
  </script>
</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(GPKG_PATH)
    connection.row_factory = sqlite3.Row

    tract_records = load_tract_geometry(connection)
    county_records = load_county_geometry(connection)

    bounds = compute_bounds(tract_records + county_records)
    map_width, map_height = enrich_geometry(tract_records + county_records, bounds)

    layers = [
        build_cluster_layer(connection),
        build_cvi_layer(connection),
        build_priority_layer(connection),
        build_high_cvi_share_layer(connection),
        build_duration_layer(connection),
    ]

    html_path = OUTPUT_DIR / "nyc_outage_updated_layers.html"
    county_vulnerability_path = OUTPUT_DIR / "part_08_county_vulnerability_concentration.png"
    preview_path = OUTPUT_DIR / "nyc_outage_updated_layers_preview.png"

    write_html(tract_records, county_records, layers, map_width, map_height, html_path)
    render_county_static_map(county_records, layers[3], county_vulnerability_path)
    build_preview_collage(preview_path, county_vulnerability_path, html_path)

    print(f"Wrote {html_path}")
    print(f"Wrote {county_vulnerability_path}")
    print(f"Wrote {preview_path}")


if __name__ == "__main__":
    main()
