# Follow-up: what the 2026-10-20 replication shows, and what to run next

*2026-10-28. Results are Sahil's follow-up runs (230 planned trials, 227 returned). The intervals and p-values below
were computed from his counts with `wilson()` and `fisher()` in `llm_eval.py`.*

## Status, 2026-11-04

**Run A is done** (Sahil, 2026-11-01, at PR commit `65bc2bc`). Correct r / returned answers:

| Configuration | Thinking off (4,096 tokens) | Thinking on (16,384 tokens) |
|---|---:|---:|
| Lightning | 3/60 | 57/58 |
| Ultra | 1/59 | 60/60 |

Simon recounted these cells from the raw ledger. On this one table, the failures below were largely a matter of
configuration. The cost is real: median output went from 157 to 7,568 tokens for Lightning and from 178 to 2,352 for
Ultra, and median time from 1.8 s and 6.8 s to 83 s and 75 s.

The token limits differ between the arms, but an equal-limit rerun would add little. No thinking-off answer came near its
limit (1,192 tokens at most), and the earlier thinking-off batches at 16,384 tokens also failed (at temperature 0.2).

**What changes for the runs below:**
- `z-ai/glm-5.3` is retired (HTTP 410) and `moonshotai/kimi-k2.6` is not accessible (HTTP 404), so the four base models
  in Runs B and C can't be run as written. "Kimi" was k3 in the early batches and k2.6 later: label them separately.
- Sahil's October 23 study (705 responses) already compares names and labels with row order fixed: Lightning 22/30 vs
  23/30, Ultra 30/30 vs 30/30. Only the row-order arm of Run B is still open.
- **The omission was real, and its cause is open.** `mentions_climate_neg` and `sign` here missed a bare `-`, the bug Sahil
  found in his scorer. Both are fixed. On the first study's free-form answers, rescoring only moves Ultra from 15/27 to
  17/27 (GLM 30/30, Kimi 27/28 by this measure, 28/28 by hand), and the misses do omit the result. On the October 23
  ledger, which has no checklist but does have an answer template, Lightning goes from 54/90 to 87/90 and Ultra from 71/90
  to 83/90 (86/90 reviewed by Sahil). So something other than a checklist can remove the omission: the template, the
  temperature (0.2 then, 0.7 now) or the packet.
- **Run C as first written was not an exact replication.** Its no-checklist rows carry the answer template, which the
  first study did not have. `+freeform` conditions now drop it, and `Ultra t0.2` in `models.example.json` matches the
  first study's temperature.
- `means_ok` now requires the value as rounded, as Sahil's analysis does: `x=0.5740` is wrong. Lightning's steps rows
  are 15/20 means right, not 17/20.
- `corr_new` adds three correlations: another column of the same table, a negative r, and ten rows.

**Next, in order, about 980 calls:**

| Command | Calls | Question |
|---|---:|---|
| `run models.json --tasks corr_new --reps 20` (4 configurations, not `Ultra t0.2`) | 240 | Does thinking on hold beyond one table? |
| `run models.json --tasks max_p90,ranking --reps 20` (2 thinking configurations) | 200 | Does it fix maximum selection and ranking? Includes the row-order arm of Run B |
| `run models.json --tasks mixed --reps 20` (Lightning, Ultra, `Ultra t0.2`) | 540 | Template, checklist or temperature: what removes the omission? |

`run` uses every entry in the file, so give each command a `models.json` with only the entries named. Each `run` uses
the same `runs.sqlite`; use `--seed` or move it aside as described below.

Then finish the 40-answer hand check, ideally with a second rater.

## What the replication settles

| Finding | Evidence | Status |
|---|---|---|
| Lightning and Ultra get the correlation wrong (thinking disabled) | Correct, pooled over both rounds: Lightning 1/40 (95% CI 0–13%), Ultra 0/40 (0–9%). GLM 39/40 (87–100%). GLM vs Ultra in the follow-up: Fisher p < 1e-9 | **Established** for this configuration |
| Lightning's wrong answers are not caused by the wording | The same neutral prompt gave 0.55, −0.53 and 0.19 | Established. The cause is still unknown: reading the table, or the arithmetic |
| Ultra ranks differently with borough names | Names 14/15 (70–99%) vs labels 12/15 (55–93%), Fisher p = 0.60. This reverses the direction of the first run | **Not supported**: report as no evidence of a name effect |
| Lightning ranks badly | Names 2/15 (4–38%), labels 5/15 (15–58%), p = 0.39 | Poor in both conditions, with no difference between them |
| Ultra omits the contrary climate result | With a checklist that names all three findings, GLM, Kimi and Ultra each included it in 30/30 (89–100%) | **Not replicated**: the earlier pattern came from prompts without a checklist, so this test was not a replication |

Sahil's caveats apply as written. Row position and alias changed together across his five rotations. Each condition has
15 runs. Lightning was not in the coverage test.

## Design gaps this branch fixes in `llm_eval.py`

1. **Thinking was off for Lightning and Ultra only.** "The model is weak" and "the configuration is weak" are mixed
   together. `models.example.json` now has `Lightning+thinking` and `Ultra+thinking` entries, with the same model and a
   reasoning switch to fill in.
2. **No way to tell where a wrong r comes from.** The new `corr/steps` condition asks for the two column means before r.
   The report counts `means right`, so right means with a wrong r points to the arithmetic, and wrong means point to
   reading the table. (Gold means: x = 0.5739, y = 1.500.)
3. **Row order was confounded with labels.** Ranking is now a 2×2: `names`, `labels` (fixed row order, random alias
   per run), `names_shuffled`, `labels_shuffled` (random row order, logged in `row_order`). `labels` vs `names` tests
   the alias with row order held fixed. `*_shuffled` vs fixed tests row order.
4. **The checklist's own effect was never tested.** `mixed` now crosses the three original framings with `+checklist`.
   `positive` vs `positive+checklist` asks whether the checklist is what removed the omission. The `+freeform` rows, added later, are the replication of the first study's prompts.
5. **Provider failures shrank Kimi's sample.** Calls are retried twice (10 s, then 20 s). The report shows
   `failed (retried)`, so a retried success is still visible.
6. **Results were raw counts.** Every cell now has a Wilson 95% interval.

`python llm_eval/llm_eval.py selftest` covers all six changes. A dry run with a fake model went through
run → SQLite → report. **Nothing has been run against the real models.**

## Runs, in priority order

*Run A is done; for what comes next, use the table under Status. `GLM` and `Kimi` can no longer run.*

Run A matters most and is the cheapest. 20 reps per condition separate ≈95% from ≈55% at p < 0.05.

| Run | Command | Calls | Question it answers |
|---|---|---:|---|
| A | `run models.json --tasks corr --reps 20` (all 6 entries) | 360 | Does turning thinking on fix Lightning/Ultra? Do they misread the table or miscompute? |
| B | `run models.json --tasks ranking --reps 20` (4 base models) | 320 | Is there a name effect once row order is held fixed? |
| C | `run models.json --tasks mixed --reps 20` (4 base models, Lightning included) | 480 | Does the omission come back without a checklist, and does the checklist remove it? |

Use a separate `runs.sqlite` per run (move it aside between runs), or keep one database and read the tables by task.

If a run stops partway (network, quota, Ctrl-C), rerun the same command. Jobs that already have an answer are skipped,
failed ones are retried, and each job gets the same prompt as before. A new `--seed` starts an independent batch. Check the
`truncated` tag in Run A: thinking models can spend `max_tokens` on reasoning, and a cut-off answer counts as a
configuration problem, not a wrong number. If many answers are truncated, raise `max_tokens` and rerun those jobs.

After the runs, `python llm_eval/llm_eval.py handcheck` writes `handcheck.csv`, 10 answers per model × task with the model
hidden. Score two things with yes/no, the ones the scoring script cannot judge: `r_described_ok` (corr answers: does
the answer say r is a five-point unweighted summary that shows neither causality nor predictive power) and
`contradicts_itself`. The command will not overwrite an existing file. `report` adds the scores as a final table.
Before reading any results, agree on two things: the scored endpoint for each run is the `correct` column (for C, the
`mentions climate CVI negative` column), and any comparison not in the Fisher line is exploratory.

## Answered by Sahil's audit (2026-11-01)

- Model IDs, provider, temperature and dates are now in `preliminary_findings_revised.md`.
- The first packet's duration evidence was the six county-year OLS models.
- His coverage prompt is not `CHECKLIST`: it asks for each finding's model, direction and p-value, then an overall
  assessment. `+checklist` here is a new comparison, not a replication of his follow-up.

## Implication for the LAICS chatbot

Do not let the model compute statistics from tables. Compute numbers in code, pass them to the model as facts, and let
the model explain them. Run A doesn't change this: thinking on got one table right, at about a minute per answer, and
code is exact in milliseconds.
