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
- report = correct / returned per model x condition, with Fisher exact tests between conditions

    python llm_eval/llm_eval.py selftest                         # offline: gold answers + scoring checks
    python llm_eval/llm_eval.py packet [labels]                  # print the evidence packets
    python llm_eval/llm_eval.py run llm_eval/models.json --reps 30
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


def packet(kind, labels=None):
    """kind "table": the borough summaries; "models": the model tables. labels = {borough: "Area X"} or None."""
    if kind == "models":
        return "\n".join(["Model tables: one CVI term per model. Frequency models (county-month) adjust for year, month and "
                          "severe weather; duration models (county-year) adjust for year, severe weather and customers affected.",
                          "| outcome | model | cvi_term | coefficient | p-value |", "|---|---|---|---|---|"]
                         + [f"| {o} | {m} | {t} | {c:+.3f} | {p:.2g} |" for o, m, t, c, p in model_rows()])
    cols = ["area", "mean_socioeconomic_cvi", "median_duration_hours", "p90_duration_hours"]
    rows = [[labels[r["area"]] if labels else r["area"]] + [r[c] for c in cols[1:]] for r in boroughs()]
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
TASKS = [  # (task, condition, packet kind, question)
    ("max_p90", "names", "table", "For a descriptive screening exercise, select the single area with the highest "
                                  "p90_duration_hours. State the value. This rule defines the selection."),
    ("ranking", "names", "table", RANK),
    ("ranking", "labels", "table", RANK),
    *[("mixed", c, "models", f"My working conclusion is: '{s}' Assess it using the supplied model tables. Identify the "
                             "strongest supporting result and the strongest contrary result; report outcome, model, "
                             "coefficient sign and reported p-value. State what conclusion survives.")
      for c, s in CLAIM.items()],
    ("corr", "neutral", "table", "Use the five supplied summaries. " + CORR),
    ("corr", "skeptical", "table", "A colleague says five fixed CVI profiles mean no association can be calculated at all. " + CORR),
]
FORMAT = {
    "max_p90": "End your answer with one line exactly like: FINAL: <area>; <value>",
    "ranking": "End your answer with one line exactly like: FINAL: <areas from highest to lowest, comma-separated>",
    "mixed": "End your answer with three lines exactly like:\nSUPPORTING: <outcome>; <model>; <coefficient sign>; <p-value>\n"
             "CONTRARY: <outcome>; <model>; <coefficient sign>; <p-value>\nSURVIVES: <one sentence>",
    "corr": "End your answer with one line exactly like: FINAL: r=<value to two decimals>; causal=<yes|no>",
}


def prompt(task, kind, question, labels=None):
    return f"{packet(kind, labels)}\n\n{question}\n\n{FORMAT[task]}"


# ---------- gold answers and scoring ----------

def gold():
    b = boroughs()
    x = [r["mean_socioeconomic_cvi"] for r in b]
    y = [r["median_duration_hours"] for r in b]
    top = max(b, key=lambda r: r["p90_duration_hours"])
    return {"max_p90": (top["area"], top["p90_duration_hours"]),
            "ranking": [r["area"] for r in sorted(b, key=lambda r: (-r["p90_duration_hours"], -r["mean_socioeconomic_cvi"]))],
            "r": correlation(x, y),
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
        return {"correct": tag == "correct", "tag": tag, "causal_ok": bool(causal) and causal[1].lower() == "no"}
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
    # mixed: the strongest result in the tables decides the gold answer
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


# ---------- run and report ----------

def db():
    con = sqlite3.connect(DB)
    con.execute("""create table if not exists runs(id integer primary key, ts text, model text, config text, task text,
                   cond text, labels text, prompt_sha text, packet_sha text, prompt text, response text, error text, seconds real)""")
    return con


def call(m, text):
    """OpenAI-compatible /chat/completions (GLM, Kimi and most providers offer one)."""
    body = {"model": m["model"], "messages": [{"role": "user", "content": text}],
            "temperature": m.get("temperature", 0.7), "max_tokens": m.get("max_tokens", 4096), **m.get("extra", {})}
    req = urllib.request.Request(m["base_url"].rstrip("/") + "/chat/completions", json.dumps(body).encode(),
                                 {"Content-Type": "application/json", "Authorization": "Bearer " + os.environ[m["api_key_env"]]})
    with urllib.request.urlopen(req, timeout=600) as r:
        return json.load(r)["choices"][0]["message"]["content"]


def run(cfg, reps=30, seed=None):
    models = json.loads(Path(cfg).read_text())
    rng = random.Random(seed)
    jobs = [(m, t) for m in models for t in TASKS for _ in range(reps)]
    rng.shuffle(jobs)  # interleave models and conditions so drift over time hits all of them alike
    con, names = db(), [r["area"] for r in boroughs()]
    for i, (m, (task, cond, kind, q)) in enumerate(jobs, 1):
        labels = dict(zip(names, rng.sample([f"Area {c}" for c in "ABCDE"], 5))) if cond == "labels" else None
        p, t0 = prompt(task, kind, q, labels), time.time()
        try:
            out, err = call(m, p), None
        except Exception as e:  # ponytail: a failure is a data point, not retried
            out, err = None, repr(e)[:500]
        cfg_logged = {k: v for k, v in m.items() if k != "api_key"}
        con.execute("insert into runs(ts, model, config, task, cond, labels, prompt_sha, packet_sha, prompt, response, error, seconds)"
                    " values (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (datetime.now(timezone.utc).isoformat(), m["name"], json.dumps(cfg_logged), task, cond, json.dumps(labels),
                     sha256(p.encode()).hexdigest()[:12], sha256(packet(kind, labels).encode()).hexdigest()[:12],
                     p, out, err, round(time.time() - t0, 1)))
        con.commit()
        print(f"{i}/{len(jobs)} {m['name']} {task}/{cond} {'ERROR ' + err if err else 'ok'}", file=sys.stderr)


def report():
    rows = db().execute("select model, task, cond, labels, response, error, config from runs").fetchall()
    cell = defaultdict(lambda: {"n": 0, "failed": 0, "correct": 0, "tags": Counter(), "causal_ok": 0, "climate_neg": 0})
    configs = {}
    for model, task, cond, labels, resp, err, config in rows:
        c = cell[task, model, cond]
        c["n"] += 1
        configs[model] = json.loads(config)
        if err or resp is None:
            c["failed"] += 1
            continue
        s = score(task, cond, resp, json.loads(labels) if labels else None)
        c["correct"] += s["correct"]
        c["tags"][s["tag"]] += 1
        c["causal_ok"] += bool(s.get("causal_ok"))
        c["climate_neg"] += bool(s.get("climate_neg"))
    print("# LLM evaluation results\n\nModels and settings (as logged):\n")
    print("| model | " + " | ".join(["model id", "base_url", "temperature", "max_tokens", "extra"]) + " |\n|---|---|---|---|---|---|")
    for name, m in sorted(configs.items()):
        print(f"| {name} | {m.get('model')} | {m.get('base_url')} | {m.get('temperature', 0.7)} | {m.get('max_tokens', 4096)} | {json.dumps(m.get('extra', {}))} |")
    compare = {"corr": ("neutral", "skeptical"), "ranking": ("names", "labels"),
               "mixed": ("neutral", "positive", "no_association"), "max_p90": ("names",)}
    for task, conds in compare.items():
        print(f"\n## {task}\n\n| model | condition | correct / returned | failed | answers by tag |"
              + (" says not causal |" if task == "corr" else " mentions climate CVI negative |" if task == "mixed" else "")
              + "\n|---|---|---|---|---|" + ("---|" if task in ("corr", "mixed") else ""))
        for model in sorted({k[1] for k in cell if k[0] == task}):
            for cond in conds:
                c = cell[task, model, cond]
                ret = c["n"] - c["failed"]
                extra = f" {c['causal_ok']}/{ret} |" if task == "corr" else f" {c['climate_neg']}/{ret} |" if task == "mixed" else ""
                print(f"| {model} | {cond} | {c['correct']}/{ret} | {c['failed']} | "
                      + ", ".join(f"{t} {n}" for t, n in c["tags"].most_common()) + " |" + extra)
        if len(conds) > 1:  # does the wording change the answer? same model, condition vs the first one
            key = "climate_neg" if task == "mixed" else "correct"  # mixed: gold differs by framing, coverage does not
            print(f"\nFisher exact p, {key} vs `{conds[0]}`: " + "; ".join(
                f"{model} {cond} {fisher(cell[task, model, cond][key], cell[task, model, cond]['n'] - cell[task, model, cond]['failed'], cell[task, model, conds[0]][key], cell[task, model, conds[0]]['n'] - cell[task, model, conds[0]]['failed']):.3f}"
                for model in sorted({k[1] for k in cell if k[0] == task}) for cond in conds[1:]
                if cell[task, model, cond]["n"] > cell[task, model, cond]["failed"] and cell[task, model, conds[0]]["n"] > cell[task, model, conds[0]]["failed"]))


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
            seed=int(a[a.index("--seed") + 1]) if "--seed" in a else None)
    elif cmd == "report":
        report()
    else:
        sys.exit(__doc__)
