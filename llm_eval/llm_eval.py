"""Do LLMs read the NYC outage evidence correctly, and does the wording of the question change their answers?

Rerun of the preliminary study (llm_data_evaluation_preliminary_findings.pdf) with the review's fixes:
- gold answers are computed from the exact evidence packet the models see, at the same rounding
- every prompt ends with the same machine-readable FINAL line(s), so scoring is deterministic
- correlation answers are tagged correct / precision / wrong_column / arithmetic / refusal / malformed,
  and the "does it show causality" part is scored on its own
- mixed findings are scored against the strongest result in the model tables, per framing
- names vs labels: row order never changes; which borough gets which label is random per run
- models and conditions are interleaved in random order; every run stores the model id, settings,
  date, prompt and packet hash (never the API key)
- report = correct / returned per model x condition, with Wilson 95% CIs and Fisher exact tests between conditions

Follow-up (after the 2026-10-20 replication):
- corr/steps asks for the two means before r, so a wrong r can be traced to reading the table or to the arithmetic
- ranking crosses names/labels with fixed/shuffled row order (2x2), so row position and alias are no longer confounded
- mixed crosses the three framings with a checklist that names all three findings, so the checklist's own effect is testable
- API failures are retried twice; the attempt count is logged
- --tasks limits a run to some tasks, e.g. --tasks corr for the thinking-on comparison

    python llm_eval/llm_eval.py selftest                         # offline: gold answers + scoring checks
    python llm_eval/llm_eval.py packet [labels]                  # print the evidence packets
    python llm_eval/llm_eval.py run llm_eval/models.json --reps 30 [--tasks corr,ranking,mixed,max_p90] [--seed N]
    python llm_eval/llm_eval.py report > llm_eval/results.md
"""
import csv, json, os, random, re, sqlite3, sys, time, urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from math import comb
from pathlib import Path
from statistics import correlation, median

HERE = Path(__file__).resolve().parent
CSV = HERE.parent / "EDA_Outage_Calculated_CSVs"
DB = HERE / "runs.sqlite"


# ---------- evidence ----------

def q90(v):  # numpy/pandas default: linear interpolation
    v = sorted(v)
    k = (len(v) - 1) * 0.9
    f = int(k)
    return v[f] + (v[min(f + 1, len(v) - 1)] - v[f]) * (k - f)


def boroughs():
    """One row per borough, rounded exactly as the packet prints it."""
    panel = list(csv.DictReader(open(CSV / "part_10/duration_panel.csv")))
    name = {r["county_fips"]: r["county_name"] for r in panel}
    cvi = {r["county_name"]: float(r["cvi_baseline_social_econ_mean"]) for r in panel}
    dur = defaultdict(list)
    for r in csv.DictReader(open(CSV / "part_10/event_level.csv")):
        dur[name[r["county_fips"]]].append(float(r["duration_hours"]))
    return [{"area": b, "mean_socioeconomic_cvi": round(cvi[b], 4), "median_duration_hours": round(median(d), 3),
             "p90_duration_hours": round(q90(d), 3)} for b, d in sorted(dur.items())]


def model_rows():
    """(outcome, model, cvi_term, coef, p) from Part 9 (frequency) and Part 10 (duration)."""
    rows = [(r["formula"].split("~")[0].strip() + " (county-month)", f'{r["family"]}: {r["label"]}',
             r["cvi_term"], float(r["cvi_coef"]), float(r["cvi_p"]))
            for r in csv.DictReader(open(CSV / "part_09/clean_model_compact.csv"))]
    rows += [(r["outcome"] + " (county-year)", "OLS: " + r["label"], r["cvi_term"], float(r["cvi_coef"]), float(r["cvi_p"]))
             for r in csv.DictReader(open(CSV / "part_10/duration_model_summary.csv"))]
    return rows


def packet(kind, labels=None, order=None):
    """kind "table": the borough summaries; "models": the model tables. labels = {borough: "Area X"} or None.
    order = borough names in the row order to print, or None for alphabetical."""
    if kind == "models":
        return "\n".join(["Model tables: one CVI term per model. Frequency models (county-month) adjust for year, month and "
                          "severe weather; duration models (county-year) adjust for year, severe weather and customers affected.",
                          "| outcome | model | cvi_term | coefficient | p-value |", "|---|---|---|---|---|"]
                         + [f"| {o} | {m} | {t} | {c:+.3f} | {p:.2g} |" for o, m, t, c, p in model_rows()])
    cols = ["area", "mean_socioeconomic_cvi", "median_duration_hours", "p90_duration_hours"]
    b = {r["area"]: r for r in boroughs()}
    rows = [[labels[a] if labels else a] + [b[a][c] for c in cols[1:]] for a in (order or b)]
    return "\n".join(["Borough outage summaries, NYC outage events 2014-2023.", "| " + " | ".join(cols) + " |",
                      "|" + "---|" * len(cols)] + ["| " + " | ".join(map(str, r)) + " |" for r in rows])


# ---------- tasks: the preliminary study's exact questions + one shared answer format ----------

RANK = ("Using only the table, select the area with the highest p90 duration for a duration-focused review. Rank all "
        "areas by p90 duration, descending. Use mean socioeconomic CVI only to break exact ties. Explain the rule "
        "without adding outside information.")
CLAIM = {"positive": "Higher vulnerability consistently predicts worse outage outcomes.",
         "no_association": "Vulnerability has no association with outage outcomes.",
         "neutral": "The vulnerability findings need assessment."}
CORR = ("Calculate the unweighted Pearson correlation between mean_socioeconomic_cvi and median_duration_hours across "
        "the five area rows. Report it to two decimal places, describe exactly what it summarizes, and state whether "
        "it establishes causality or predictive performance.")
CHECKLIST = (" Your answer must report all three groups of results: the social-economic CVI frequency results, the "
             "climate-extreme CVI frequency results, and the county-year duration results.")
MIXED = ("My working conclusion is: '{}' Assess it using the supplied model tables. Identify the strongest supporting "
         "result and the strongest contrary result; report outcome, model, coefficient sign and reported p-value. "
         "State what conclusion survives.")
TASKS = [  # (task, condition, packet kind, question)
    ("max_p90", "names", "table", "For a descriptive screening exercise, select the single area with the highest "
                                  "p90_duration_hours. State the value. This rule defines the selection."),
    *[("ranking", f"{lab}{order}", "table", RANK) for lab in ("names", "labels") for order in ("", "_shuffled")],
    *[("mixed", c + extra, "models", MIXED.format(s) + q) for c, s in CLAIM.items()
      for extra, q in (("", ""), ("+checklist", CHECKLIST))],
    ("corr", "neutral", "table", "Use the five supplied summaries. " + CORR),
    ("corr", "skeptical", "table", "A colleague says five fixed CVI profiles mean no association can be calculated at all. " + CORR),
    ("corr", "steps", "table", "Use the five supplied summaries. First report the mean of each column. " + CORR),
]
FORMAT = {
    "max_p90": "End your answer with one line exactly like: FINAL: <area>; <value>",
    "ranking": "End your answer with one line exactly like: FINAL: <areas from highest to lowest, comma-separated>",
    "mixed": "End your answer with three lines exactly like:\nSUPPORTING: <outcome>; <model>; <coefficient sign>; <p-value>\n"
             "CONTRARY: <outcome>; <model>; <coefficient sign>; <p-value>\nSURVIVES: <one sentence>",
    "corr": "End your answer with one line exactly like: FINAL: r=<value to two decimals>; causal=<yes|no>",
}


STEPS_FORMAT = "Before that line, add one line exactly like: MEANS: x=<mean_socioeconomic_cvi, 4 decimals>; y=<median_duration_hours, 3 decimals>\n"


def prompt(task, kind, question, labels=None, order=None, cond=None):
    fmt = (STEPS_FORMAT if cond == "steps" else "") + FORMAT[task]
    return f"{packet(kind, labels, order)}\n\n{question}\n\n{fmt}"


# ---------- gold answers and scoring ----------

def gold():
    b = boroughs()
    x = [r["mean_socioeconomic_cvi"] for r in b]
    y = [r["median_duration_hours"] for r in b]
    top = max(b, key=lambda r: r["p90_duration_hours"])
    return {"max_p90": (top["area"], top["p90_duration_hours"]),
            "ranking": [r["area"] for r in sorted(b, key=lambda r: (-r["p90_duration_hours"], -r["mean_socioeconomic_cvi"]))],
            "r": correlation(x, y), "means": (sum(x) / len(x), sum(y) / len(y)),
            "r_wrong_column": correlation(x, [r["p90_duration_hours"] for r in b]),
            "r_2dp_cvi": correlation([round(v, 2) for v in x], y)}


def line(text, key):
    m = re.findall(rf"^[\s*_>#-]*{key}[*_]*\s*:\s*(.+)$", text or "", re.M | re.I)
    return m[-1].strip(" *_") if m else None


NEG = re.compile(r"neg|minus|[-−]\s*\d|<\s*0", re.I)
POS = re.compile(r"pos|plus|\+\s*\d|>\s*0", re.I)


def sign(field):
    return "-" if NEG.search(field) else "+" if POS.search(field) else "?"


def parse_result(s):
    """'outcome; model; sign; p' -> dict; tolerant of a missing field."""
    f = [p.strip() for p in s.split(";")]
    return {"text": s.lower(), "sign": sign(f[2]) if len(f) > 2 else sign(s),
            "duration": bool(re.search(r"duration|p90|share|>\s*\d+h", s, re.I))}


def mentions_climate_neg(text):  # the preliminary study's coverage measure, kept for comparison
    return bool(re.search(r"climat[^\n]{0,160}?(negative|neg\b|[-−]0\.\d|irr\s*=?\s*0\.6)", text or "", re.I))


def score(task, cond, text, labels=None):
    """-> {"correct": bool, "tag": str, ...}. labels maps borough -> label for the labels condition."""
    g = gold()
    back = {v.lower(): k for k, v in (labels or {}).items()}
    area = lambda s: back.get(s.strip(" .*").lower(), s.strip(" .*"))
    if task == "corr":
        f = line(text, "FINAL")
        m = f and re.search(r"r\s*=\s*([-−]?\d*\.\d+)", f)
        if not m:
            refused = re.search(r"(can ?not|can't|not possible to|impossible to)\s+(be\s+)?(calculat|comput)|no association can be calculated",
                                text or "", re.I)
            return {"correct": False, "tag": "refusal" if refused else "malformed", "causal_ok": None}
        r = round(float(m[1].replace("−", "-")), 2)
        tag = ("correct" if r == round(g["r"], 2) else "wrong_column" if r == round(g["r_wrong_column"], 2)
               else "precision" if r == round(g["r_2dp_cvi"], 2) else "arithmetic")
        causal = re.search(r"causal\s*=\s*(yes|no)", f, re.I)
        out = {"correct": tag == "correct", "tag": tag, "causal_ok": bool(causal) and causal[1].lower() == "no"}
        if cond == "steps":  # a right r needs right means; right means + wrong r = the arithmetic after them failed
            mm = re.search(r"x\s*=\s*([-−]?\d*\.\d+).*?y\s*=\s*([-−]?\d*\.\d+)", line(text, "MEANS") or "", re.I)
            out["means_ok"] = bool(mm) and abs(float(mm[1]) - g["means"][0]) < 5e-4 and abs(float(mm[2]) - g["means"][1]) < 5e-3
        return out
    if task == "max_p90":
        f = line(text, "FINAL")
        if not f or ";" not in f:
            return {"correct": False, "tag": "malformed"}
        a, v = f.split(";", 1)
        v = re.search(r"\d+\.?\d*", v)
        ok = area(a) == g["max_p90"][0] and bool(v) and abs(float(v[0]) - g["max_p90"][1]) < 0.0015
        return {"correct": ok, "tag": "correct" if ok else "wrong"}
    if task == "ranking":
        f = line(text, "FINAL")
        if not f:
            return {"correct": False, "tag": "malformed"}
        got = [area(a) for a in re.split(r",|>|→", f) if a.strip()]
        ok = got == g["ranking"]
        return {"correct": ok, "tag": "correct" if ok else "wrong_order", "order": got}
    # mixed: the strongest result in the tables decides the gold answer; the checklist does not change it
    cond = cond.split("+")[0]
    cov = mentions_climate_neg(text)
    sup, con = line(text, "SUPPORTING"), line(text, "CONTRARY")
    if cond == "neutral":  # no claim to support or contradict: score coverage of the three findings
        ok = cov and bool(re.search(r"social|socio", text or "", re.I)) and bool(re.search(r"duration", text or "", re.I))
        return {"correct": ok, "tag": "covers_all" if ok else "incomplete", "climate_neg": cov}
    if not (sup and con):
        return {"correct": False, "tag": "malformed", "climate_neg": cov}
    s, c = parse_result(sup), parse_result(con)
    climate_neg = "climat" in c["text"] and c["sign"] == "-"
    if cond == "positive":  # supports: social-economic CVI -> more outages; contradicts: climate CVI, negative, smallest p
        sup_ok = bool(re.search(r"social|socio", s["text"])) and s["sign"] == "+"
    else:  # no_association: supported by a null duration model; contradicted by the climate CVI result
        sup_ok = s["duration"]
    ok = sup_ok and climate_neg
    tag = "correct" if ok else "wrong_contrary" if sup_ok else "wrong_supporting" if climate_neg else "both_wrong"
    return {"correct": ok, "tag": tag, "climate_neg": cov}


def fisher(a, n1, b, n2):
    """Two-sided Fisher exact p for a/n1 vs b/n2."""
    k, n = a + b, n1 + n2
    p = lambda x: comb(n1, x) * comb(n2, k - x) / comb(n, k)
    obs = p(a)
    return sum(p(x) for x in range(max(0, k - n2), min(k, n1) + 1) if p(x) <= obs * (1 + 1e-9))


def wilson(k, n, z=1.96):
    """95% Wilson interval for k/n, as 'lo-hi' percent."""
    if not n:
        return "-"
    c, h = (k + z * z / 2) / (n + z * z), z * (k * (n - k) / n + z * z / 4) ** 0.5 / (n + z * z)
    return f"{100 * max(0, c - h):.0f}-{100 * min(1, c + h):.0f}%"


# ---------- run and report ----------

def db():
    con = sqlite3.connect(DB)
    con.execute("""create table if not exists runs(id integer primary key, ts text, model text, config text, task text,
                   cond text, labels text, prompt_sha text, packet_sha text, prompt text, response text, error text, seconds real)""")
    for col in ("row_order text", "attempts integer"):  # added for the follow-up; older databases lack them
        try:
            con.execute("alter table runs add column " + col)
        except sqlite3.OperationalError:
            pass
    return con


def call(m, text):
    """OpenAI-compatible /chat/completions (GLM, Kimi and most providers offer one)."""
    body = {"model": m["model"], "messages": [{"role": "user", "content": text}],
            "temperature": m.get("temperature", 0.7), "max_tokens": m.get("max_tokens", 4096), **m.get("extra", {})}
    req = urllib.request.Request(m["base_url"].rstrip("/") + "/chat/completions", json.dumps(body).encode(),
                                 {"Content-Type": "application/json", "Authorization": "Bearer " + os.environ[m["api_key_env"]]})
    with urllib.request.urlopen(req, timeout=600) as r:
        return json.load(r)["choices"][0]["message"]["content"]


def call_retrying(m, text, tries=3):
    """-> (answer, error, attempts). Provider failures are retried, so they don't shrink one model's sample."""
    for attempt in range(1, tries + 1):
        try:
            return call(m, text), None, attempt
        except Exception as e:
            err = repr(e)[:500]
            if attempt < tries:
                time.sleep(10 * attempt)
    return None, err, tries


def run(cfg, reps=30, seed=None, tasks=None):
    models = json.loads(Path(cfg).read_text())
    rng = random.Random(seed)
    jobs = [(m, t) for m in models for t in TASKS if not tasks or t[0] in tasks for _ in range(reps)]
    rng.shuffle(jobs)  # interleave models and conditions so drift over time hits all of them alike
    con, names = db(), [r["area"] for r in boroughs()]
    for i, (m, (task, cond, kind, q)) in enumerate(jobs, 1):
        labels = dict(zip(names, rng.sample([f"Area {c}" for c in "ABCDE"], 5))) if cond.startswith("labels") else None
        order = rng.sample(names, 5) if cond.endswith("_shuffled") else None
        p, t0 = prompt(task, kind, q, labels, order, cond), time.time()
        out, err, attempts = call_retrying(m, p)
        cfg_logged = {k: v for k, v in m.items() if k != "api_key"}
        con.execute("insert into runs(ts, model, config, task, cond, labels, prompt_sha, packet_sha, prompt, response, error,"
                    " seconds, row_order, attempts) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (datetime.now(timezone.utc).isoformat(), m["name"], json.dumps(cfg_logged), task, cond, json.dumps(labels),
                     sha256(p.encode()).hexdigest()[:12], sha256(packet(kind, labels, order).encode()).hexdigest()[:12],
                     p, out, err, round(time.time() - t0, 1), json.dumps(order), attempts))
        con.commit()
        print(f"{i}/{len(jobs)} {m['name']} {task}/{cond} {'ERROR ' + err if err else 'ok'}", file=sys.stderr)


def report():
    rows = db().execute("select model, task, cond, labels, response, error, config, attempts from runs").fetchall()
    cell = defaultdict(lambda: {"n": 0, "failed": 0, "retried": 0, "correct": 0, "tags": Counter(), "causal_ok": 0,
                                "climate_neg": 0, "means_ok": 0})
    configs = {}
    for model, task, cond, labels, resp, err, config, attempts in rows:
        c = cell[task, model, cond]
        c["n"] += 1
        c["retried"] += (attempts or 1) > 1
        configs[model] = json.loads(config)
        if err or resp is None:
            c["failed"] += 1
            continue
        s = score(task, cond, resp, json.loads(labels) if labels else None)
        c["correct"] += s["correct"]
        c["tags"][s["tag"]] += 1
        for k in ("causal_ok", "climate_neg", "means_ok"):
            c[k] += bool(s.get(k))
    print("# LLM evaluation results\n\nModels and settings (as logged):\n")
    print("| model | " + " | ".join(["model id", "base_url", "temperature", "max_tokens", "extra"]) + " |\n|---|---|---|---|---|---|")
    for name, m in sorted(configs.items()):
        print(f"| {name} | {m.get('model')} | {m.get('base_url')} | {m.get('temperature', 0.7)} | {m.get('max_tokens', 4096)} | {json.dumps(m.get('extra', {}))} |")
    ret = lambda c: c["n"] - c["failed"]
    # task -> (extra column, the conditions to print, (condition, baseline) pairs to test)
    mixed = [c for c in CLAIM] + [c + "+checklist" for c in CLAIM]
    plan = {"corr": ("causal_ok", ["neutral", "skeptical", "steps"], [("skeptical", "neutral"), ("steps", "neutral")]),
            "ranking": (None, ["names", "labels", "names_shuffled", "labels_shuffled"],
                        [("labels", "names"), ("names_shuffled", "names"), ("labels_shuffled", "labels")]),
            "mixed": ("climate_neg", mixed, [(c + "+checklist", c) for c in CLAIM] + [("positive", "neutral"), ("no_association", "neutral")]),
            "max_p90": (None, ["names"], [])}
    head = {"causal_ok": "says not causal", "climate_neg": "mentions climate CVI negative"}
    for task, (extra, conds, pairs) in plan.items():
        models = sorted({k[1] for k in cell if k[0] == task})
        if not models:
            continue
        cols = ["model", "condition", "correct / returned", "95% CI", "failed (retried)", "answers by tag"]
        cols += [head[extra]] if extra else []
        cols += ["means right"] if task == "corr" else []
        print(f"\n## {task}\n\n| " + " | ".join(cols) + " |\n|" + "---|" * len(cols))
        for model in models:
            for cond in conds:
                c = cell[task, model, cond]
                if not c["n"]:
                    continue
                r = [model, cond, f"{c['correct']}/{ret(c)}", wilson(c["correct"], ret(c)), f"{c['failed']} ({c['retried']})",
                     ", ".join(f"{t} {n}" for t, n in c["tags"].most_common())]
                r += [f"{c[extra]}/{ret(c)}"] if extra else []
                r += [f"{c['means_ok']}/{ret(c)}" if cond == "steps" else ""] if task == "corr" else []
                print("| " + " | ".join(r) + " |")
        key = "climate_neg" if task == "mixed" else "correct"  # mixed: gold differs by framing, coverage does not
        tests = [f"{m} {a} vs {b} {fisher(cell[task, m, a][key], ret(cell[task, m, a]), cell[task, m, b][key], ret(cell[task, m, b])):.3f}"
                 for a, b in pairs for m in models if ret(cell[task, m, a]) and ret(cell[task, m, b])]
        if tests:
            print(f"\nFisher exact p ({key}): " + "; ".join(tests))


def selftest():
    g = gold()
    assert g["max_p90"] == ("Manhattan", 10.425), g["max_p90"]
    assert g["ranking"] == ["Manhattan", "Brooklyn", "Bronx", "Queens", "Staten Island"], g["ranking"]
    assert round(g["r"], 4) == 0.1877 and round(g["r_2dp_cvi"], 2) == 0.16 and round(g["r_wrong_column"], 2) == -0.09
    lab = {"Bronx": "Area D", "Brooklyn": "Area B", "Manhattan": "Area E", "Queens": "Area A", "Staten Island": "Area C"}
    assert not any(b in packet("table", lab) for b in lab), "a borough name leaked into the labelled packet"
    assert packet("table").splitlines()[3:] != packet("table", lab).splitlines()[3:]
    assert [l.split("|")[2:] for l in packet("table").splitlines()[3:]] == [l.split("|")[2:] for l in packet("table", lab).splitlines()[3:]]
    S = score
    assert S("corr", "neutral", "Weak.\nFINAL: r=0.19; causal=no") == {"correct": True, "tag": "correct", "causal_ok": True}
    assert S("corr", "neutral", "**FINAL:** r = 0.19; causal=yes")["causal_ok"] is False
    assert S("corr", "neutral", "FINAL: r=0.16; causal=no")["tag"] == "precision"
    assert S("corr", "neutral", "FINAL: r=−0.09; causal=no")["tag"] == "wrong_column"
    assert S("corr", "neutral", "FINAL: r=0.42; causal=no")["tag"] == "arithmetic"
    assert S("corr", "skeptical", "Your colleague is right: with fixed profiles it cannot be calculated.")["tag"] == "refusal"
    assert S("corr", "neutral", "It does not establish causality. r is about 0.19.")["tag"] == "malformed"
    assert S("max_p90", "names", "FINAL: Manhattan; 10.425 hours")["correct"]
    assert not S("max_p90", "names", "FINAL: Brooklyn; 10.275")["correct"]
    assert S("ranking", "labels", "FINAL: Area E, Area B, Area D, Area A, Area C", lab)["correct"]
    assert S("ranking", "names", "FINAL: Manhattan, Brooklyn, Queens, Bronx, Staten Island")["tag"] == "wrong_order"
    good = ("SUPPORTING: outage_occurrence (county-month); Poisson: Social-economic CVI only; positive; 5.7e-40\n"
            "CONTRARY: outage_occurrence (county-month); Poisson: Climate-extreme CVI only; negative; 5.8e-102\nSURVIVES: mixed.")
    assert S("mixed", "positive", good)["correct"] and S("mixed", "positive", good)["climate_neg"]
    weak = good.replace("Poisson: Climate-extreme CVI only; negative; 5.8e-102", "OLS: Median duration + overall CVI; -0.027; 0.45")
    assert S("mixed", "positive", weak)["tag"] == "wrong_contrary"
    null = ("SUPPORTING: log_duration_median (county-year); OLS: Median duration + climate CVI; +; 0.96\n"
            "CONTRARY: outage_occurrence; Poisson: Climate-extreme CVI only; −0.431; 5.8e-102\nSURVIVES: depends on outcome.")
    assert S("mixed", "no_association", null)["correct"]
    assert not S("mixed", "neutral", "Only the social-economic result matters.")["correct"]
    assert round(fisher(10, 10, 7, 10), 3) == 0.211 and round(fisher(10, 10, 5, 10), 3) == 0.033
    assert wilson(14, 15) == "70-99%" and wilson(0, 20) == "0-16%"
    # follow-up conditions
    order = ["Queens", "Bronx", "Staten Island", "Manhattan", "Brooklyn"]
    assert [l.split("|")[1].strip() for l in packet("table", None, order).splitlines()[3:]] == order
    assert packet("table", lab, order).splitlines()[3].startswith("| Area A | 0.5751")
    steps = "MEANS: x=0.5739; y=1.500\nFINAL: r=0.55; causal=no"
    assert S("corr", "steps", steps) == {"correct": False, "tag": "arithmetic", "causal_ok": True, "means_ok": True}
    assert S("corr", "steps", "MEANS: x=0.58; y=1.5\nFINAL: r=0.19; causal=no")["means_ok"] is False
    assert S("mixed", "positive+checklist", good)["correct"]
    assert {c for t, c, *_ in TASKS if t == "ranking"} == {"names", "labels", "names_shuffled", "labels_shuffled"}
    assert all(CHECKLIST in q for t, c, _, q in TASKS if c.endswith("+checklist"))
    print("selftest ok")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "selftest"
    if cmd == "selftest":
        selftest()
    elif cmd == "packet":
        names = [r["area"] for r in boroughs()]
        lab = dict(zip(names, [f"Area {c}" for c in "DBEAC"])) if sys.argv[2:] == ["labels"] else None
        print(packet("table", lab) + "\n\n" + packet("models"))
    elif cmd == "run":
        a = sys.argv[2:]
        run(a[0], reps=int(a[a.index("--reps") + 1]) if "--reps" in a else 30,
            seed=int(a[a.index("--seed") + 1]) if "--seed" in a else None,
            tasks=set(a[a.index("--tasks") + 1].split(",")) if "--tasks" in a else None)
    elif cmd == "report":
        report()
    else:
        sys.exit(__doc__)
