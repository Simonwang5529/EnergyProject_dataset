# Follow-up: what the 2026-10-20 replication shows, and what to run next

*2026-10-28. Results are Sahil's follow-up runs (230 planned trials, 227 returned). The intervals and p-values below
were computed from his counts with `wilson()` and `fisher()` in `llm_eval.py`.*

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
   `positive` vs `positive+checklist` asks whether the checklist is what removed the omission. The rows without a
   checklist are the exact replication of the first study.
5. **Provider failures shrank Kimi's sample.** Calls are retried twice (10 s, then 20 s). The report shows
   `failed (retried)`, so a retried success is still visible.
6. **Results were raw counts.** Every cell now has a Wilson 95% interval.

`python llm_eval/llm_eval.py selftest` covers all six changes. A dry run with a fake model went through
run → SQLite → report. **Nothing has been run against the real models.**

## Runs, in priority order

Run A matters most and is the cheapest. 20 reps per condition separate ≈95% from ≈55% at p < 0.05.

| Run | Command | Calls | Question it answers |
|---|---|---:|---|
| A | `run models.json --tasks corr --reps 20` (all 6 entries) | 360 | Does turning thinking on fix Lightning/Ultra? Do they misread the table or miscompute? |
| B | `run models.json --tasks ranking --reps 20` (4 base models) | 320 | Is there a name effect once row order is held fixed? |
| C | `run models.json --tasks mixed --reps 20` (4 base models, Lightning included) | 480 | Does the omission come back without a checklist, and does the checklist remove it? |

Use a separate `runs.sqlite` per run (move it aside between runs), or keep one database and read the tables by task.
Before reading any results, agree on two things: the scored endpoint for each run is the `correct` column (for C, the
`mentions climate CVI negative` column), and any comparison not in the Fisher line is exploratory.

## Still needed from the first runs

- Exact model IDs, providers, temperature and run dates for all four models: the model table in
  `preliminary_findings_revised.md` is still empty.
- Which duration tables the first evidence packet contained.
- Sahil's follow-up prompts and the audit ledger. These are needed to know whether his coverage checklist matches
  `CHECKLIST` here; if it does not, use his text.

## Implication for the LAICS chatbot

Do not let the model compute statistics from tables. Compute numbers in code, pass them to the model as facts, and let
the model explain them. If a model must do the arithmetic, GLM and Kimi (thinking settings as tested) are the only ones
that did so reliably.
