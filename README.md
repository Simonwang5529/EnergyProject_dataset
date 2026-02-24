# EnergyProject_dataset

Dataset repository for the NYC CVI + outage integration work.

## Folder structure

- `raw_data/`
  - `Master CVI Dataset - Oct 2023.xlsx`
  - `Outage_Dataset/` (original outage CSV/DOCX files)

- `generated_data/`
  - `Outage_CVI_Events.csv`
    - Event-level merged dataset (CVI + outage records, 2014-2023 NYC).
  - `Outage_CVI_Merged_Counts_2014_2023.csv`
    - No-event-detail version (one row per tract, yearly and total outage event counts).
  - `Outage_County_Event_Counts_2014_2023.csv`
    - County-level outage event count summary.

## Notes

Large files are tracked with Git LFS.
