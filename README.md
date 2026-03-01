# EnergyProject_dataset

Dataset repository for the NYC CVI + outage integration work.

## Quick intro

This repo includes raw outage/CVI sources and cleaned outputs.
The latest severe-weather-only outputs are in `generated_data/`:
- `SW_Outage.csv`: Severe-weather-related outage records (all available regions/years in the source set).
- `SW_Outage_NYC.csv`: NYC-only subset of severe-weather-related outage records.

## Folder structure

- `raw_data/`
  - `Master CVI Dataset - Oct 2023.xlsx`
  - `Outage_Dataset/` (original outage CSV/DOCX files)

- `generated_data/`
  - `Outage_CVI_Events.csv`
    - Event-level merged dataset (CVI + outage records, 2014-2023 NYC).
  - `Tract_CVI_Event_Counts_2014_2023.csv`
    - No-event-detail version (one row per tract, yearly and total outage event counts).
  - `County_Event_Counts_2014_2023.csv`
    - County-level outage event count summary.
  - `SW_Outage.csv`
    - Severe-weather-related outage records.
  - `SW_Outage_NYC.csv`
    - NYC-only severe-weather-related outage records.

## Notes

Large files are tracked with Git LFS.
