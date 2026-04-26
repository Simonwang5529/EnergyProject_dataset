# Clean Electricity Low-Income Eligibility Source

Workbook added:

- `CleanElectricityLowIncome_Excel_20260217.xlsx`

Workbook description:

- Title in workbook: `Low-Income Community Bonus Credit Program Layers [Update January 2026]`
- Sheet used in notebook: `2025 Tract percentages`
- Key fields used: `Census Tract GEOID 2025`, `State FIPS`, `County FIPS`,
  `County Name`, `Percent in Category 1`, `Percent in ASC PPC`,
  `Percent in ASC CEJST-E`

Official source pages:

- IRS program page: https://www.irs.gov/credits-deductions/clean-electricity-low-income-communities-bonus-credit-amount-program
- IRS related resource, "Maps for Category 1 and Geographic Selection Criteria":
  https://experience.arcgis.com/experience/12227d891a4d471497ac13f60fffd822

Use in this repository:

- The Clean Electricity workbook is a 2025/current tract-level eligibility
  layer. It is used as a current policy benchmark in
  `notebooks/clean_electricity_low_income_nyc_outage_analysis.ipynb`, not as a
  year-varying 2014-2023 exposure.
