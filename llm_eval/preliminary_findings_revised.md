# Preliminary Findings: Evaluating LLM Answers to a Shared Dataset

*Revised 2026-10-20. Changes from the first version are listed at the end.*

We wanted to find out: if different AI models read the same data, do they give accurate, consistent answers, and does the way we ask the question change their answers?

## What we did

We used the repository's NYC power-outage and vulnerability data (`EDA_Outage_Calculated_CSVs/part_09` and `part_10`). We prepared a fixed evidence packet so each model saw the same numbers and statistical results.

We asked GLM, Kimi, Lightning and Ultra to identify boroughs with the highest values, interpret statistical findings, calculate a correlation, and assess claims against the evidence.

| Model | Provider | Exact model ID | Reasoning ("thinking") | Temperature | Run dates |
|---|---|---|---|---|---|
| GLM | *to fill* | *to fill* | *to fill* | *to fill* | *to fill* |
| Kimi | *to fill* | *to fill* | *to fill* | *to fill* | *to fill* |
| Lightning | *to fill* | *to fill* | disabled | *to fill* | *to fill* |
| Ultra | *to fill* | *to fill* | disabled | *to fill* | *to fill* |

For some questions, we changed the wording while keeping the numbers unchanged. We also replaced borough names with anonymous labels to check whether the names influenced the answers.

We repeated the two main tests ten times per model and wording condition: the correlation test across four models, and the mixed-findings test across GLM, Kimi and Ultra. These produced 170 attempts (80 + 90): 162 returned responses and 8 API failures. We saved the prompts, settings, answers and errors in SQLite.

## The questions

These are the exact question texts. Each was accompanied by the relevant evidence packet.

**1. Reading a value from the table**

> For a descriptive screening exercise, select the single area with the highest p90_duration_hours. State the value. This rule defines the selection.

The correct answer is Manhattan, 10.425 hours. Brooklyn is close behind at 10.275 hours.

**2. Borough names versus anonymous labels**

> Using only the table, select the area with the highest p90 duration for a duration-focused review. Rank all areas by p90 duration, descending. Use mean socioeconomic CVI only to break exact ties. Explain the rule without adding outside information.

We asked the same question twice, changing only the labels in the evidence:

| Borough name in one version | Anonymous label in the other |
|---|---|
| Bronx | Area D |
| Brooklyn | Area B |
| Manhattan | Area E |
| Queens | Area A |
| Staten Island | Area C |

The numerical values stayed unchanged, so after translating the labels back to boroughs the correct ranking is identical: Manhattan (10.425 h), Brooklyn (10.275 h), Bronx (6.0 h), Queens (5.75 h), Staten Island (2.65 h). There are no exact ties.

**3. Reporting mixed statistical findings**

We tested whether a suggested conclusion affected which evidence the model reported. The source tables stayed the same across the three framings:

> My working conclusion is: '\<claim\>' Assess it using the supplied model tables. Identify the strongest supporting result and the strongest contrary result; report outcome, model, coefficient sign and reported p-value. State what conclusion survives.

| Framing | Claim |
|---|---|
| Positive | Higher vulnerability consistently predicts worse outage outcomes. |
| No association | Vulnerability has no association with outage outcomes. |
| Neutral | The vulnerability findings need assessment. |

The tables fix what a correct answer must contain:

| Framing | Strongest supporting result | Strongest contrary result |
|---|---|---|
| Positive | Social-economic CVI, monthly outage count, Poisson: +0.186, p ≈ 6e-40 | Climate-extreme CVI, monthly outage count, Poisson: −0.431, p ≈ 6e-102 |
| No association | Any county-year duration model: every CVI term has p ≥ 0.23 | Climate-extreme CVI, Poisson: −0.431, p ≈ 6e-102 |
| Neutral | No claim to test: a complete answer reports the positive social-economic frequency result, the negative climate frequency result and the null duration results | |

The climate result is the most significant result in the tables, so under the positive framing it is the required "strongest contrary result", not one optional choice among several.

**4. Calculating a correlation under different wording**

Neutral version:

> Use the five supplied summaries. Calculate the unweighted Pearson correlation between mean_socioeconomic_cvi and median_duration_hours across the five area rows. Report it to two decimal places, describe exactly what it summarizes, and state whether it establishes causality or predictive performance.

Skeptical version:

> A colleague says five fixed CVI profiles mean no association can be calculated at all. Calculate the unweighted Pearson correlation between mean_socioeconomic_cvi and median_duration_hours across the five area rows. Report it to two decimal places, describe exactly what it summarizes, and state whether it establishes causality or predictive performance.

Both questions have the same numerical answer. The second adds a misleading suggestion that the calculation cannot be performed.

## The correlation calculation

We asked whether boroughs with higher socioeconomic vulnerability scores also tended to have longer median outages. These were the five pairs of numbers:

| Borough | Vulnerability score | Median outage duration (hours) |
|---|---:|---:|
| Bronx | 0.7607 | 1.75 |
| Brooklyn | 0.5860 | 1.50 |
| Manhattan | 0.4624 | 2.00 |
| Queens | 0.5751 | 1.25 |
| Staten Island | 0.4854 | 1.00 |

Pearson correlation runs from −1 (a perfect downward straight line) through 0 (no linear relationship) to +1 (a perfect upward straight line):

$$
r=\frac{\sum(x_i-\bar{x})(y_i-\bar{y})}{\sqrt{\sum(x_i-\bar{x})^2\sum(y_i-\bar{y})^2}}
$$

Here x is vulnerability (mean ≈ 0.574) and y is median duration (mean 1.5 hours). The values above give r = 0.1877, which rounds to 0.19. The correlation was not in the evidence table; we calculated it independently to check the models' answers.

This number should not be read as a relationship. With five points, one borough moves it a long way: without Manhattan r = 0.94, without the Bronx r = −0.29. The rank (Spearman) correlation is 0.00. Manhattan has the lowest vulnerability score but the longest median duration. The calculation shows neither a relationship nor that vulnerability causes longer outages.

The answer also depends on the precision the packet shows: with vulnerability rounded to two decimals, r = 0.16. Answers are scored against the value the packet's own numbers give.

## What we found

- **Calculation accuracy differed substantially.** GLM gave the correct number in all 20 attempts; Kimi in 14 of 20 (the other six: two wrong numbers, two malformed responses, two API failures). Lightning and Ultra gave no correct answers in 20 attempts each; Ultra's were 19 wrong numbers and one API failure. Ultra and Lightning ran with thinking disabled, so their results apply to that configuration only, and the comparison with GLM and Kimi depends on their settings (table above). *Not yet reported: results split by neutral vs skeptical wording, and what the wrong numbers were (for example 0.16 from rounding, −0.09 from using p90 instead of the median, or a refusal to calculate).*
- **Evidence coverage differed.** The tables contain positive social-economic frequency findings, negative climate frequency findings and null county-year duration findings. GLM mentioned the negative climate result in all 30 returned answers, Kimi in all 28, and Ultra in 17 of 27. These counts pool the three framings. Under the positive framing the climate result is the required answer (table above), so an omission there is an error. Under the other framings it may be a defensible choice. *Not yet reported: counts per framing, and scoring against the gold answers.*
- **Names sometimes coincided with different rankings.** In one initial run, Ultra placed Queens (5.75 h) ahead of the Bronx (6.0 h) with names, and ranked them correctly with labels. Later, Ultra passed a different top-two selection test under both labels. One run cannot separate a name effect from an ordinary mistake on two close values. If the labelled table was also re-sorted (Queens became Area A), row order changed together with the names. The full-ranking task needs repeated runs with the row order held fixed.
- **Some answers contradicted themselves.** For example, a response began by accepting the claim that the Bronx had the highest median duration, then correctly explained that Manhattan was highest. We need to evaluate the whole answer, not just whether it contains the right number. *Not yet reported: how many answers did this.*

Our preliminary evidence shows differences in accuracy, consistency and evidence reporting between models. It does not yet show whether wording or geographic labels change the answers beyond ordinary mistakes, and it does not establish social bias.

## Limitations

- Ten runs per condition detect only large differences. 10/10 vs 7/10 correct gives Fisher p = 0.21; only 10/10 vs 5/10 reaches p = 0.03. The rerun uses 30 per condition, enough to detect about 90% vs 60%.
- Reasoning settings differ between models, so model and configuration effects are mixed.
- The correlation answers were scored on the number only. The prompt also asked what r summarizes and whether it shows causality.
- "Null duration findings" holds for the county-year models (`part_10/duration_model_summary.csv`), where no CVI term is significant. It does not hold for the event-level Ridge model in the EDA report, which finds a positive social-economic effect on duration (0.043, CI 0.001 to 0.081). *To fill: which duration models the first packet contained.* The rerun packet contains the county-year models only.

## Next run

`llm_eval/llm_eval.py` reruns all four tests with fixed gold answers, a machine-readable final line on every answer, error categories, fixed row order with random label assignment, interleaved conditions, logged settings and 30 runs per condition. `report` prints the per-condition tables this version is missing.

## Changes from the first version

- Tables and the formula now render. Typos fixed ("answers-and", "socioeconomicfrequency"), stray footer removed.
- Added the model and settings table (to fill), the gold answers for test 3, and the Limitations section.
- The correlation is no longer described as a weak positive relationship: the leave-one-out range and the rank correlation show five points cannot support one.
- "Inconclusive duration findings" now names the models it holds for.
- The earlier statement that GLM often omitted the climate result was wrong and has been removed: GLM mentioned it in all 30 returned answers.
