"""Part 9's county-month models again, with standard errors clustered by county.

The 600 rows hold only five distinct values of each CVI term, and Part 9's HC3 and Poisson standard errors treat the rows
as independent. This refits the same formulas and clusters by county (CR1 for OLS, sandwich for Poisson; t with 4 df).
With five clusters these standard errors are rough too, so read them as "the data can't settle this", not as a new
estimate of the effect.

    python scripts/part9_clustered_check.py   # writes EDA_Outage_Calculated_CSVs/part_09/clean_model_clustered_check.csv
"""
import csv
from pathlib import Path

import numpy as np

CSVS = Path(__file__).resolve().parent.parent / "EDA_Outage_Calculated_CSVs"
ROWS = list(csv.DictReader((CSVS / "part_08/county_month_panel.csv").open()))
REPORTED = {(r["family"], r["cvi_term"]): r for r in csv.DictReader((CSVS / "part_09/clean_model_compact.csv").open())
            if r["model"] != "climate_interaction"}
TERMS = {"z_cvi_overall_mean": "Overall CVI", "z_cvi_baseline_social_econ_mean": "Social-economic CVI",
         "z_cvi_climate_extreme_events_mean": "Climate-extreme CVI", "z_high_cvi_share": "High-vulnerability share"}
COUNTY = np.array([r["county_fips"] for r in ROWS])
MONTHS = sorted({r["month"] for r in ROWS}, key=float)[1:]


def design(term):
    return np.array([[1, float(r["year_centered"]), float(r["z_severe_weather_count"]), float(r[term])]
                     + [float(r["month"] == m) for m in MONTHS] for r in ROWS])


def ols(X, y):
    bread = np.linalg.inv(X.T @ X)
    b = bread @ X.T @ y
    return b, bread, y - X @ b


def poisson(X, y):
    b = np.zeros(X.shape[1])
    b[0] = np.log(y.mean())
    for _ in range(100):  # Newton steps
        mu = np.exp(X @ b)
        step = np.linalg.solve(X.T @ (X * mu[:, None]), X.T @ (y - mu))
        b += step
        if np.abs(step).max() < 1e-10:
            break
    mu = np.exp(X @ b)
    return b, np.linalg.inv(X.T @ (X * mu[:, None])), y - mu


def clustered_se(X, resid, bread, is_ols, column):
    groups = sorted(set(COUNTY))
    meat = sum(np.outer(X[COUNTY == g].T @ resid[COUNTY == g], X[COUNTY == g].T @ resid[COUNTY == g]) for g in groups)
    n, k, G = *X.shape, len(groups)
    scale = G / (G - 1) * ((n - 1) / (n - k) if is_ols else 1)
    return (scale * bread @ meat @ bread)[column, column] ** 0.5


def p_t4(t):  # two-sided p of Student's t with 4 df, in closed form
    return 1 - abs(t) * (6 + t * t) / (4 + t * t) ** 1.5


out = []
for term, label in TERMS.items():
    X = design(term)
    for family, y, fit in (("OLS", "log_outage_occurrence", ols), ("Poisson", "outage_occurrence", poisson)):
        b, bread, resid = fit(X, np.array([float(r[y]) for r in ROWS]))
        reported = REPORTED[family, term]
        # the same model as Part 9; statsmodels stops its Poisson fit earlier, about 2e-5 away
        assert abs(b[3] - float(reported["cvi_coef"])) < 1e-3, (family, term, b[3], reported["cvi_coef"])
        se = clustered_se(X, resid, bread, family == "OLS", 3)
        weather_se = clustered_se(X, resid, bread, family == "OLS", 2)
        out.append({"family": family, "label": label, "cvi_term": term, "cvi_coef": round(float(b[3]), 4),
                    "reported_p": reported["cvi_p"], "clustered_se": round(float(se), 4), "clustered_p": round(float(p_t4(b[3] / se)), 3),
                    "ci_low": round(float(b[3] - 2.776 * se), 3), "ci_high": round(float(b[3] + 2.776 * se), 3),
                    "weather_coef": round(float(b[2]), 4), "weather_clustered_p": round(float(p_t4(b[2] / weather_se)), 3)})

path = CSVS / "part_09/clean_model_clustered_check.csv"
with path.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=out[0])
    w.writeheader()
    w.writerows(out)
for r in out:
    print(r)
