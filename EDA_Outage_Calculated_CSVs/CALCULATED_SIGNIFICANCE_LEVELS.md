# Calculated CSV Significance Levels

This document records which results in `EDA_Outage_Calculated_CSVs` include both significance thresholds:

- `95% significance level` means `p < 0.05` or `q < 0.05`
- `99% significance level` means `p < 0.01` or `q < 0.01`

Updated: `2026-04-03`

## 1. CSVs that contain both 95% and 99% significance outputs in the same file

### [tract_lr.csv](part_01/tract_lr.csv)
- Path: `part_01/tract_lr.csv`
- Meaning: regression results for `severe weather -> outage occurrence`
- 95% column: `sig_05`
- 99% column: `sig_01`

### [tract_trend.csv](part_02/tract_trend.csv)
- Path: `part_02/tract_trend.csv`
- Meaning: tract-level occurrence and duration trends
- 95% columns: `occ_trend_sig_05`, `dur_trend_sig_05`, `occ_trend_dir_05`, `dur_trend_dir_05`
- 99% columns: `occ_trend_sig_01`, `dur_trend_sig_01`, `occ_trend_dir_01`, `dur_trend_dir_01`
- Backward-compatible aliases kept: `occ_trend_dir`, `dur_trend_dir` still point to the 95% version

### [year_month_test.csv](part_03/year_month_test.csv)
- Path: `part_03/year_month_test.csv`
- Meaning: year-level month effect test
- 95% column: `year_has_month_effect_sig_05`
- 99% column: `year_has_month_effect_sig_01`

### [month_detail.csv](part_03/month_detail.csv)
- Path: `part_03/month_detail.csv`
- Meaning: month-level residual significance within each year
- 95% column: `high_month_sig_05`
- 99% column: `high_month_sig_01`

### [tract_sw.csv](part_04/tract_sw.csv)
- Path: `part_04/tract_sw.csv`
- Meaning: tract-level Spearman + FDR results for severe weather versus occurrence / duration
- 95% columns: `occ_sig_05`, `dur_sig_05`
- 99% columns: `occ_sig_01`, `dur_sig_01`

### [sig_summary.csv](part_04/sig_summary.csv)
- Path: `part_04/sig_summary.csv`
- Meaning: summary counts across analysis types
- Storage format: this file uses one field, `sig_level`, instead of paired `_05` / `_01` columns
- `sig_level = 0.05` means 95%
- `sig_level = 0.01` means 99%

### [month_trend.csv](part_05/month_trend.csv)
- Path: `part_05/month_trend.csv`
- Meaning: month-level occurrence and duration trends
- 95% columns: `occ_sig_05`, `dur_sig_05`
- 99% columns: `occ_sig_01`, `dur_sig_01`

### [clean_model_compact.csv](part_09/clean_model_compact.csv)
- Path: `part_09/clean_model_compact.csv`
- Meaning: CVI effect significance in the clean model set
- 95% column: `cvi_effect_sig_05`
- 99% column: `cvi_effect_sig_01`

### [clean_model_plot_data.csv](part_09/clean_model_plot_data.csv)
- Path: `part_09/clean_model_plot_data.csv`
- Meaning: compact plotting table for clean model results
- 95% column: `sig_05`
- 99% column: `sig_01`

### [duration_model_summary.csv](part_10/duration_model_summary.csv)
- Path: `part_10/duration_model_summary.csv`
- Meaning: weather and CVI coefficient significance in duration models
- 95% columns: `cvi_sig_05`, `weather_sig_05`
- 99% columns: `cvi_sig_01`, `weather_sig_01`

## 2. Results exported as paired 95% and 99% files

These results are not stored as two columns in one CSV. They are exported as separate paired files.

### Trend count files
- 95%:
  [occ_trend_counts.csv](part_02/occ_trend_counts.csv)
  [dur_trend_counts.csv](part_02/dur_trend_counts.csv)
- 99%:
  [occ_trend_counts_01.csv](part_02/occ_trend_counts_01.csv)
  [dur_trend_counts_01.csv](part_02/dur_trend_counts_01.csv)

### Significant high month files
- 95%:
  [sig_months.csv](part_03/sig_months.csv)
- 99%:
  [sig_months_01.csv](part_03/sig_months_01.csv)

## 3. Short conclusion

It appears in:

- regression significance outputs
- tract trend significance outputs
- month effect significance outputs
- severe weather correlation significance outputs
- month trend significance outputs
- clean model and duration model coefficient significance outputs
