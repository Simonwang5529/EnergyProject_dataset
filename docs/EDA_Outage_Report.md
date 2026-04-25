# EDA Outage Report: Five-Page Methods And Results Summary

Data: NYC five counties, 2014-2023, outage events joined with Climate Vulnerability Index (CVI) indicators.

Main question: whether outage burden is related to time, severe weather, seasonality, and social vulnerability.

## Page 1: Study Design And Modeling Logic

The analysis starts with a tract-level exploratory dataset, but the final interpretation does not rely on tract-level regression as final evidence. The key diagnostic in Part 8 shows that outage outcomes repeat within the same county-year. That means tract-level models create many rows but not many independent outage outcomes.

The report uses four evidence levels:

- Tract-year EDA for maps, screening, and pattern discovery.
- County-year models for the core diagnostic and duration analysis.
- County-month models for the main Part 9 frequency model.
- Event-level models for the strongest duration and resilience checks.

The most important methodological change is moving the clean Part 9 frequency models to a balanced county-month panel. With five counties, ten years, and twelve months per year, the frequency model has `600` county-month observations rather than only `50` county-year observations.

This distinction matters for interpretation. Tract-level rows are still useful for mapping where vulnerability is concentrated, but they should not be counted as independent outage observations when the same county-year outage total is repeated across many tracts. The county-month panel keeps the analysis closer to the real outage process while giving enough observations for a simple frequency model.



## Page 2: Exploratory Methods Used In Parts 0-6

Parts 0-6 describe the outage data before final modeling. These steps reveal seasonality, geographic structure, and data limitations.

- Part 0 cleaned outage dates, county FIPS, tract FIPS, duration, customer counts, and severe-weather flags.
- Part 1 used per-tract OLS models of outage count and log duration on severe-weather count, with FDR correction.
- Part 2 used per-tract OLS year-trend models, also with FDR correction.
- Part 3 used year-by-month chi-square tests with FDR residual checks.
- Part 4 used visual checks and Spearman correlation because outage counts are skewed.
- Part 5 used Kendall's tau and Theil-Sen monthly trend checks.
- Part 6 used K-Means clustering of monthly outage-share profiles.

These methods support the descriptive story, but they should not be treated as the main causal or inferential evidence.

The early parts are still important because they explain why later models are needed. Parts 1 and 2 show that severe weather and time are related to outage outcomes, but they also reveal the danger of overstating precision. Part 3 shows clear seasonal structure, which is why later count models include month effects. Part 6 shows that the apparent tract clusters mostly duplicate county patterns, which reinforces the decision to move away from tract-level inference.

In short, Parts 0-6 are best described as exploratory EDA. They establish patterns and guide model choice, but they are not the strongest evidence for final claims about CVI.



## Page 3: Corrected Count And Frequency Methods

Part 8 is the central correction. It shows that outage occurrence, mean outage duration, and severe-weather counts are identical for tracts inside the same county-year. The corrected models therefore collapse away the false tract-level precision.

The corrected frequency methods are:

- Part 8a: county-year collapse, OLS, Poisson, and VIF checks.
- Part 8b: Negative Binomial count models and a county-month seasonality model.
- Part 9: county-month one-CVI-at-a-time OLS and Poisson models with month effects.
- Part 9b: theory-driven OLS vs Ridge using social-economic CVI, infrastructure CVI, and severe weather.

The count-model upgrade is important. Poisson overdispersion is `41.73`, far above the ideal value of `1`, so raw Poisson is not the best final count model. Negative Binomial improves AIC from `2297.1` under Poisson to `551.1` with the same main predictors. In the county-month seasonality model, month effects remain highly significant (`p = 5.44e-11`) while controlling for county, year, and days in month.

Part 9 then simplifies the CVI question. Instead of putting many highly correlated CVI variables into the same model, it fits one main CVI concept at a time. This is easier to interpret and avoids unstable coefficients caused by multicollinearity. The core predictors are year, severe-weather count, month effects, and one CVI measure. This structure answers a focused question: after accounting for time, severe weather, and seasonality, does a county with higher vulnerability have a higher monthly outage count?

Part 9b keeps the same cautious spirit but adds a sensitivity check. Ridge regression shrinks coefficients when predictors are correlated, so it is useful for checking whether the sign of social-economic CVI, infrastructure CVI, and severe weather remains stable.



## Page 4: CVI, Duration, And Robustness Methods

The CVI analysis is intentionally split across frequency and resilience questions. Frequency asks whether vulnerable places have more outage events. Resilience asks whether, once outages occur, vulnerable places remain without power longer.

The CVI and duration methods are:

- Part 10: county-year duration models for median duration, p90 duration, share above 8 hours, and share above 24 hours.
- Part 10b: event-level OLS, GEE logistic regression, Ridge, and customer-hours models.
- Part 11: robustness comparison across county-year frequency, county-year duration, event-level OLS, and event-level Ridge.
- Part 12: mechanism synthesis comparing frequency and resilience.

Linear regression is acceptable for log-duration and screening models. Negative Binomial is preferred for overdispersed outage counts. Logistic regression is appropriate only for binary outcomes, such as severe outage yes/no. Ridge is used as a sensitivity check, not as the main explanatory model.

The duration analysis is deliberately separated from the frequency analysis. A county can have many outages but relatively short restoration times, or fewer outages but longer restoration times. For equity and resilience, the second question is often more policy-relevant. That is why the report separately checks median duration, p90 duration, long-outage shares, event-level log duration, and customer-hours.

The event-level models are especially useful because they avoid the small county-year sample. They use `4,518` deduplicated outage events and allow the report to test whether severe weather and CVI are associated with outage duration once individual events are the unit of analysis.

## Page 4b: How To Read The Models

The methods answer different questions, so the coefficients should not all be interpreted the same way.

- OLS on log outcomes estimates average changes in transformed outage count or duration.
- Poisson and Negative Binomial models estimate count relationships; incidence rate ratios (IRRs) are easier to explain than raw log coefficients.
- Logistic models estimate binary severe-outage probability, not outage count or duration.
- Ridge coefficients are mainly used to see whether signs remain stable when predictors are correlated.

For nontechnical readers, the safest language is comparative and directional. For example, say that higher social-economic vulnerability is associated with higher monthly outage counts in the clean county-month model, not that vulnerability alone causes outages. For duration, say that severe weather is the clearest predictor, while the CVI duration signal is most visible in the event-level Ridge model.

The report should also be clear about what not to overclaim. Tract-level patterns are valuable for maps and screening, but they are not the final inferential level. Climate-extreme-events CVI has an unexpected negative sign in Part 9, so it should be treated as a modeling caution rather than a policy headline. Infrastructure CVI is positive in the theory-driven frequency model but imprecise, so it is suggestive rather than definitive.



## Page 5: Main Results And Final Interpretation

1. Severe weather is the strongest and most stable predictor. In event-level models, severe weather is strongly associated with log duration (`coef = 0.446`, `p < 0.001`) and log customer-hours (`coef = 1.506`, `p < 0.001`).

2. The best frequency model uses county-month data. Part 9 now uses `600` county-month observations. In the clean one-CVI-at-a-time models, social-economic CVI is the strongest positive CVI signal. In Poisson, `z_cvi_baseline_social_econ_mean = 0.186`, with `IRR = 1.204`, meaning a one-standard-deviation increase is associated with about a `20%` higher monthly outage count.

3. Overall CVI is also positive in Part 9. In the county-month Poisson model, `z_cvi_overall_mean = 0.133`, with `IRR = 1.142`. This supports the broader vulnerability story, although social-economic CVI is clearer.

4. Climate-extreme-events CVI has a negative sign in Part 9 (`IRR = 0.650` in Poisson). Because this direction is not intuitive, it should be described cautiously rather than treated as a headline result.

5. Duration evidence is more mixed at the county-year level. In Part 10, severe weather predicts median duration, p90 duration, and the share of outages longer than 8 hours, but CVI terms are not consistently significant.

6. The strongest CVI duration evidence comes from event-level Ridge. Social-economic CVI is positive for log duration and its interval excludes zero (`coef = 0.043`, CI about `0.001` to `0.081`). Event-level clustered OLS is positive too, but its interval crosses zero.

7. Infrastructure CVI is suggestive but imprecise. In Part 9b, the infrastructure coefficient is positive in both OLS and Ridge, but confidence intervals cross zero.

8. The county-year frequency sensitivity check is positive but uncertain for social-economic CVI. In Part 9b, social-economic CVI is positive in both OLS and Ridge, but both confidence intervals cross zero. This means the direction is consistent, but the evidence is not strong enough to call it definitive.

9. Severe weather is reliable across model types. It is positive in the frequency models, duration models, and customer-hours models. This is the least ambiguous result in the report.

10. The interpretation of vulnerability should be careful. Social-economic CVI is the clearest CVI signal, especially in the county-month frequency model and event-level Ridge duration model. But because some models are imprecise, the report should describe vulnerability as plausible and policy-relevant, not universally proven across every outcome.

## Final Conclusion

The careful story is not that every CVI measure strongly predicts every outage outcome. The strongest result is that severe weather is robustly related to outage burden and duration. Vulnerability remains policy-relevant, especially social-economic vulnerability, but it is model-sensitive. The most defensible interpretation is that vulnerability may matter more through the resilience channel: once outages occur, higher-vulnerability places may remain without power longer.

For presentation, the recommended final hierarchy is: use Negative Binomial for outage counts, county-month Part 9 models for the clean CVI frequency story, event-level models for duration and customer-hours, and Ridge only as a sensitivity check. The final result should emphasize severe weather first, then social-economic vulnerability as the most credible CVI pattern, and then the caution that some CVI effects vary by outcome and model.

Plain-language takeaway: the outage data show a clear weather story and a more cautious vulnerability story. Severe weather is consistently linked with more serious outage outcomes. Social-economic vulnerability is also important, especially in the updated 600-row county-month frequency model and in the event-level Ridge duration model. However, not every CVI measure behaves the same way, and some estimates remain imprecise. The report should therefore frame CVI as an equity and resilience signal that deserves attention, while being transparent that the strongest statistical evidence is for severe weather and for social-economic vulnerability rather than for every vulnerability dimension.
