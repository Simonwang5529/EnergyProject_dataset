# NYC Power Outage Vulnerability Analysis

Columbia University Data Science Institute — NYC CVI + Outage Integration Project.

**Research question:** Does community vulnerability (CVI) predict worse outage frequency and duration across NYC counties (2014–2023)?

**Main finding:** The resilience channel (duration) is a stronger and more consistent vulnerability signal than the frequency channel. Higher social-economic CVI counties experience longer outages — especially in the upper tail (p90, share >24h) — across OLS, GEE, and Ridge specifications.

## Notebooks

- `notebooks/EDA_Outage_enhanced.ipynb` — Full 12-part EDA: descriptive analysis, data-structure diagnostics, county-year CVI models, duration models, robustness summary, mechanism interpretation.
- `notebooks/clean_electricity_low_income_nyc_outage_analysis.ipynb` — Policy-alignment analysis: 2025 Clean Electricity low-income eligibility vs. 2014–2023 historical outage burden.

## Reports & Docs

- `docs/EDA_Outage_Report.docx` — Five-page methods and results summary.
- `docs/EDA_Outage_Report.md` — Markdown version of the report.
- `docs/Clean_Electricity_Policy_Alignment_Summary.docx` — Policy framing note for the Clean Electricity dataset.
- `docs/index.html` / `docs/industry-brief.html` — Web-facing summaries.

## Calculated Outputs

- `EDA_Outage_Calculated_CSVs/` — Part-by-part model outputs (tract trends, county-year panels, CVI model summaries, duration models).

## Data

- `raw_data/` — Source datasets (Master CVI Dataset, Outage_Dataset/).
- `generated_data/` — Merged and cleaned outputs (Outage_CVI_Events.csv, SW_Outage_NYC.csv, etc.).

## Key Limitations

- County-year models rest on N≈50 (5 counties × 10 years). P-values are descriptive approximations.
- Outage measures are county-uniform in the source data — tract-level CVI inference is not supported.
- Ridge alpha=0.5 (Parts 9b/10b) not cross-validated; pending LOO-CV before paper submission.

## Notes

Large files are tracked with Git LFS.
