from __future__ import annotations


import re
from pathlib import Path
from typing import Iterable

import pandas as pd


BASE_DIR = Path('/Users/simonwang/Desktop/CU/Energy Project')
CVI_FILE = BASE_DIR / 'Master CVI Dataset - Oct 2023.xlsx'
OUTAGE_DIR = BASE_DIR / 'Outage_Dataset'

EVENTS_OUT = BASE_DIR / 'Outage_CVI_Events.csv'
COUNTY_OUT = BASE_DIR / 'County_Event_Counts_2014_2023.csv'
TRACT_OUT = BASE_DIR / 'Tract_Event_Counts_2014_2023.csv'

CVI_SHEET = 'Domain CVI Values'
START_YEAR = 2014
END_YEAR = 2023
NYC_COUNTY_FIPS = {'36005', '36047', '36061', '36081', '36085'}
COUNTY_NAMES = {
    '36005': 'Bronx',
    '36047': 'Kings',
    '36061': 'New York',
    '36081': 'Queens',
    '36085': 'Richmond',
}

CVI_RENAME_MAP = {
    'State': 'state',
    'County': 'county',
    'FIPS Code': 'tract_fips',
    'Overall CVI Score': 'cvi_overall',
    'Baseline: All': 'cvi_baseline_all',
    'Baseline: Health': 'cvi_baseline_health',
    'Baseline: Social Economic': 'cvi_baseline_social_econ',
    'Baseline: Infrastructure': 'cvi_baseline_infra',
    'Baseline: Environment': 'cvi_baseline_env',
    'Climate Change: All': 'cvi_climate_all',
    'Climate Change: Health': 'cvi_climate_health',
    'Climate Change: Social Economic': 'cvi_climate_social_econ',
    'Climate Change: Extreme Events': 'cvi_climate_extreme_events',
}

OUTAGE_CORE_COLUMNS = [
    'county_fips',
    'outage_year',
    'outage_start_time',
    'outage_duration_hours',
    'outage_min_customers',
    'outage_max_customers',
    'outage_mean_customers',
    'outage_event_type',
    'outage_end_time',
    'outage_event_uid',
]

EVENTS_OUTPUT_COLUMNS = [
    'state',
    'county',
    'cvi_overall',
    'cvi_baseline_all',
    'cvi_baseline_health',
    'cvi_baseline_social_econ',
    'cvi_baseline_infra',
    'cvi_baseline_env',
    'cvi_climate_all',
    'cvi_climate_health',
    'cvi_climate_social_econ',
    'cvi_climate_extreme_events',
    'tract_fips',
    'county_fips',
    'outage_year',
    'outage_start_time',
    'outage_duration_hours',
    'outage_min_customers',
    'outage_max_customers',
    'outage_mean_customers',
    'outage_event_type',
    'outage_end_time',
    'outage_event_uid',
]

TRACT_PREFIX_COLUMNS = [
    'state',
    'county',
    'tract_fips',
    'county_fips',
    'cvi_overall',
    'cvi_baseline_all',
    'cvi_baseline_health',
    'cvi_baseline_social_econ',
    'cvi_baseline_infra',
    'cvi_baseline_env',
    'cvi_climate_all',
    'cvi_climate_health',
    'cvi_climate_social_econ',
    'cvi_climate_extreme_events',
]

WITH_EVENTS_REQUIRED = {
    'event_id',
    'Event Type',
    'fips',
    'state',
    'county',
    'start_time',
    'duration',
    'min_customers',
    'max_customers',
    'mean_customers',
}

MERGED_REQUIRED = {
    'fips',
    'state',
    'county',
    'start_time',
    'duration',
    'min_customers',
    'max_customers',
    'mean_customers',
}


def normalize_fips(series: pd.Series, width: int) -> pd.Series:
    """Return zero-padded numeric FIPS codes with a fixed width."""
    return (
        series.astype(str)
        .str.replace(r'\.0$', '', regex=True)
        .str.replace(r'\D', '', regex=True)
        .str.zfill(width)
    )


def extract_year(path: Path) -> int | None:
    """Extract the first year found in a filename."""
    match = re.search(r'(?<!\d)(20\d{2})(?!\d)', path.name)
    return int(match.group(1)) if match else None


def selected_outage_files(outage_dir: Path, start_year: int, end_year: int) -> list[Path]:
    """Return all non-group outage CSVs in the requested year range."""
    files: list[Path] = []
    for path in sorted(outage_dir.glob('*.csv')):
        if '_group' in path.name:
            continue
        year = extract_year(path)
        if year is not None and start_year <= year <= end_year:
            files.append(path)
    if not files:
        raise FileNotFoundError(
            f'No outage files found in {outage_dir} for {start_year}-{end_year}.'
        )
    return files


def load_cvi(cvi_file: Path) -> pd.DataFrame:
    """Load tract-level NYC CVI scores from the workbook."""
    cvi = pd.read_excel(cvi_file, sheet_name=CVI_SHEET)
    missing = [column for column in CVI_RENAME_MAP if column not in cvi.columns]
    if missing:
        raise ValueError(f'CVI sheet is missing expected columns: {missing}')

    cvi = cvi.rename(columns=CVI_RENAME_MAP)[list(CVI_RENAME_MAP.values())].copy()
    cvi['tract_fips'] = normalize_fips(cvi['tract_fips'], 11)
    cvi['county_fips'] = cvi['tract_fips'].str[:5]
    cvi['state'] = cvi['state'].astype(str).str.strip()
    cvi['county'] = cvi['county'].astype(str).str.strip()
    return cvi[cvi['county_fips'].isin(NYC_COUNTY_FIPS)].copy()


def from_with_events(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Normalize outage files that include event IDs and event types."""
    out = df.copy()
    out['county_fips'] = normalize_fips(out['fips'], 5)
    out['county'] = out['county'].astype(str).str.strip()
    out['outage_year'] = year
    out['outage_event_uid'] = f'{year}_' + out['event_id'].astype(str)

    if 'end_time' not in out.columns:
        out['end_time'] = pd.NA

    out = out.rename(
        columns={
            'start_time': 'outage_start_time',
            'duration': 'outage_duration_hours',
            'end_time': 'outage_end_time',
            'min_customers': 'outage_min_customers',
            'max_customers': 'outage_max_customers',
            'mean_customers': 'outage_mean_customers',
            'Event Type': 'outage_event_type',
        }
    )
    return out


def from_merged_rows(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Normalize outage files that do not include event metadata."""
    out = df.copy()
    out['county_fips'] = normalize_fips(out['fips'], 5)
    out['county'] = out['county'].astype(str).str.strip()
    out['outage_year'] = year
    out['outage_event_uid'] = [f'{year}_{idx}' for idx in out.index]
    out['outage_event_type'] = pd.NA
    out['outage_end_time'] = pd.NA

    out = out.rename(
        columns={
            'start_time': 'outage_start_time',
            'duration': 'outage_duration_hours',
            'min_customers': 'outage_min_customers',
            'max_customers': 'outage_max_customers',
            'mean_customers': 'outage_mean_customers',
        }
    )
    return out


def normalize_outage_file(path: Path) -> pd.DataFrame | None:
    """Read one outage file and map it into the shared output schema."""
    year = extract_year(path)
    if year is None:
        return None

    raw = pd.read_csv(path)
    columns = set(raw.columns)

    if WITH_EVENTS_REQUIRED.issubset(columns):
        normalized = from_with_events(raw, year)
    elif MERGED_REQUIRED.issubset(columns):
        normalized = from_merged_rows(raw, year)
    else:
        return None

    normalized = normalized[normalized['county_fips'].isin(NYC_COUNTY_FIPS)].copy()
    if normalized.empty:
        return None

    return normalized[OUTAGE_CORE_COLUMNS]


def load_outages(files: Iterable[Path]) -> pd.DataFrame:
    """Load and concatenate all selected outage files."""
    parts = [frame for path in files if (frame := normalize_outage_file(path)) is not None]
    if not parts:
        raise ValueError('No NYC outage rows found in the selected outage files.')
    return pd.concat(parts, ignore_index=True)


def build_event_output(cvi: pd.DataFrame, outage_rows: pd.DataFrame) -> pd.DataFrame:
    """Create the tract-context event-level merged dataset."""
    return cvi.merge(outage_rows, on='county_fips', how='left')[EVENTS_OUTPUT_COLUMNS]


def build_county_counts(outage_rows: pd.DataFrame) -> pd.DataFrame:
    """Create one row per county with annual and total outage counts."""
    counts = outage_rows.groupby(['county_fips', 'outage_year'], as_index=False).agg(
        outage_rows=('outage_event_uid', 'size'),
        outage_unique_events=('outage_event_uid', 'nunique'),
    )

    rows = []
    for county_fips in sorted(NYC_COUNTY_FIPS):
        county_slice = counts[counts['county_fips'] == county_fips]
        row = {'county_fips': county_fips, 'county': COUNTY_NAMES[county_fips]}
        total_rows = 0
        total_unique = 0

        for year in range(START_YEAR, END_YEAR + 1):
            year_slice = county_slice[county_slice['outage_year'] == year]
            yearly_rows = int(year_slice['outage_rows'].sum()) if not year_slice.empty else 0
            yearly_unique = (
                int(year_slice['outage_unique_events'].sum()) if not year_slice.empty else 0
            )
            row[f'outage_rows_{year}'] = yearly_rows
            row[f'outage_unique_events_{year}'] = yearly_unique
            total_rows += yearly_rows
            total_unique += yearly_unique

        row[f'outage_rows_{START_YEAR}_{END_YEAR}'] = total_rows
        row[f'outage_unique_events_{START_YEAR}_{END_YEAR}'] = total_unique
        rows.append(row)

    return pd.DataFrame(rows)


def build_tract_counts(cvi: pd.DataFrame, county_counts: pd.DataFrame) -> pd.DataFrame:
    """Attach county-level counts back to each tract."""
    columns = TRACT_PREFIX_COLUMNS[:]
    columns.extend(f'outage_rows_{year}' for year in range(START_YEAR, END_YEAR + 1))
    columns.extend(
        f'outage_unique_events_{year}' for year in range(START_YEAR, END_YEAR + 1)
    )
    columns.append(f'outage_rows_{START_YEAR}_{END_YEAR}')
    columns.append(f'outage_unique_events_{START_YEAR}_{END_YEAR}')

    return cvi.merge(county_counts, on=['county_fips', 'county'], how='left')[columns]


def main() -> None:
    cvi = load_cvi(CVI_FILE)
    outage_files = selected_outage_files(OUTAGE_DIR, START_YEAR, END_YEAR)
    outage_rows = load_outages(outage_files)

    event_output = build_event_output(cvi, outage_rows)
    county_counts = build_county_counts(outage_rows)
    tract_counts = build_tract_counts(cvi, county_counts)

    event_output.to_csv(EVENTS_OUT, index=False)
    county_counts.to_csv(COUNTY_OUT, index=False)
    tract_counts.to_csv(TRACT_OUT, index=False)

    print(f'Wrote {EVENTS_OUT} rows={len(event_output)} cols={len(event_output.columns)}')
    print(f'Wrote {COUNTY_OUT} rows={len(county_counts)} cols={len(county_counts.columns)}')
    print(f'Wrote {TRACT_OUT} rows={len(tract_counts)} cols={len(tract_counts.columns)}')
    print(f'Outage files used: {len(outage_files)}')


if __name__ == '__main__':
    main()
