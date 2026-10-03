"""Planner with/without ablation (offline, local Ollama only, no Azure).

Conditions per case:
  default      : no LLM at all (build_default_config)  -> "without planner AI"
  base_raw     : base qwen2.5:7b-instruct raw JSON
  base_repair  : same raw JSON + deterministic repair layer
  ft_raw       : fine-tuned planner-agent raw JSON
  ft_repair    : same raw JSON + repair layer           -> production minus self-check
  ft_full      : repair + self-check (assurance + 1 re-plan) -> full planner
  base_full    : base model + repair + self-check

Scores: valid_json, structural (assurance rules), safe (executor name checks),
compiles (every notebook stage compiles), executable (all three), intent,
correct (executable and intent).
"""
import json, os, re, sys, time
from unittest import mock

_UNIFIED = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CALLER_CWD = os.getcwd()  # output paths resolve from where the script was run
sys.path.insert(0, _UNIFIED)
os.chdir(_UNIFIED)
import requests
from planner_agent import ollama_planner as op
from planner_agent.planner_common import build_default_config
from planner_agent.self_check import plan_with_verification
from assurance_agent import AssuranceAgent
from executor_agent.plan_safety import plan_safety_issues
from executor_agent.notebook_builder import build_notebook_source

HOST = "http://localhost:11434"
OUT = os.path.join(_CALLER_CWD, sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] != "--summary" else "ablation_results.json")

SALES = {
    "columns": ["order_id", "region", "product", "quantity", "unit_price", "customer_email"],
    "inferred_types": {"order_id": "integer", "region": "string", "product": "string",
                       "quantity": "integer", "unit_price": "double", "customer_email": "string"},
    "row_count": 5000, "size_hint": "small (< 5MB)",
    "samples": [{"order_id": "101", "region": "EU", "product": "Mouse", "quantity": "3",
                 "unit_price": "19.99", "customer_email": "a@b.com"}],
}
ZOO_COLS = ["animal_name", "hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator",
            "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "class_type"]
ZOO = {
    "columns": ZOO_COLS,
    "inferred_types": {c: ("string" if c == "animal_name" else "integer") for c in ZOO_COLS},
    "row_count": 101, "size_hint": "small (< 5MB)",
    "samples": [dict(zip(ZOO_COLS, ["aardvark", "1", "0", "0", "1", "0", "0", "1", "1", "1", "1",
                                    "0", "0", "4", "0", "0", "1"]))],
}
IOT = {
    "columns": ["device_id", "location", "temperature", "humidity", "battery", "reading_ts"],
    "inferred_types": {"device_id": "string", "location": "string", "temperature": "double",
                       "humidity": "double", "battery": "integer", "reading_ts": "timestamp"},
    "row_count": 900000, "size_hint": "large (50-200MB)",
    "samples": [{"device_id": "D1", "location": "lab", "temperature": "21.5", "humidity": "40.2",
                 "battery": "88", "reading_ts": "2026-01-01 10:00:00"}],
}

N = r"\s*"
CASES = [
    # (name, schema, prompt, [regexes over config json])
    ("s_canonical", SALES, "ingest data from raw into bronze; then in silver, keep only rows where quantity > 5.", [r"quantity\s*>\s*5"]),
    ("s_free_num", SALES, "drop the cheap stuff - only keep orders where unit_price >= 100", [r"unit_price\s*>=\s*100"]),
    ("s_free_str", SALES, "only EU region rows please", [r"region\s*=+\s*'?EU'?"]),
    ("s_derive", SALES, "make a new column total = quantity * unit_price", [r"total\s*=\s*quantity\s*\*\s*unit_price"]),
    ("s_upper", SALES, "uppercase the region column", [r"upper\(\s*region\s*\)"]),
    ("s_agg", SALES, "average unit_price per region and a row count", [r'"group_by"\s*:\s*\[\s*"region"', r'"op"\s*:\s*"avg"', r'"op"\s*:\s*"count"']),
    ("s_numbered", SALES, "Stage 1: keep rows where quantity > 2. Stage 2: keep rows where region = 'EU'", [r"quantity\s*>\s*2", r"region\s*=+\s*'?EU'?"]),
    ("s_typo", SALES, "filtr rows whre quantiy > 10", [r"quantity\s*>\s*10"]),
    ("s_sum", SALES, "total revenue: sum of unit_price for each product", [r'"group_by"\s*:\s*\[\s*"product"', r'"op"\s*:\s*"sum"']),
    ("s_two_filters", SALES, "keep EU orders with quantity of at least 10", [r"region\s*=+\s*'?EU'?", r"quantity\s*>=\s*10"]),
    ("s_between", SALES, "keep orders priced between 10 and 50", [r"unit_price\s*(between\s*10\s*and\s*50|>=?\s*10)"]),
    ("s_dedup_filter", SALES, "remove rows with quantity below 1 and then count orders per region", [r"quantity\s*(>=\s*1|>\s*0)", r'"group_by"\s*:\s*\[\s*"region"', r'"op"\s*:\s*"count"']),
    ("z_predators", ZOO, "Keep only the animals that are predators", [r"predator\s*=+\s*1"]),
    ("z_aquatic_pred", ZOO, "keep aquatic animals that are also predators", [r"aquatic\s*=+\s*1", r"predator\s*=+\s*1"]),
    ("z_count_legs", ZOO, "count how many animals there are for each number of legs", [r'"group_by"\s*:\s*\[\s*"legs"', r'"op"\s*:\s*"count"']),
    ("z_not_venom", ZOO, "only non-venomous animals", [r"venomous\s*(=+|!=)\s*[01]"]),
    ("z_numbered", ZOO, "Stage 1: keep only aquatic animals. Stage 2: keep only predators.", [r"aquatic\s*=+\s*1", r"predator\s*=+\s*1"]),
    ("z_avg_legs", ZOO, "average legs per class_type", [r'"group_by"\s*:\s*\[\s*"class_type"', r'"op"\s*:\s*"avg"']),
    ("i_hot", IOT, "keep readings where temperature is above 30", [r"temperature\s*>\s*30"]),
    ("i_lowbat", IOT, "flag devices with low battery: keep battery < 20", [r"battery\s*<\s*20"]),
    ("i_fahrenheit", IOT, "add temperature in fahrenheit as temp_f", [r"temp_f\s*=.*temperature"]),
    ("i_avg_loc", IOT, "average humidity by location", [r'"group_by"\s*:\s*\[\s*"location"', r'"op"\s*:\s*"avg"']),
    ("i_lab", IOT, "only the readings from the lab location", [r"location\s*=+\s*'?lab'?"]),
    ("i_combo", IOT, "keep humidity above 60 then compute max temperature per location", [r"humidity\s*>\s*60", r'"op"\s*:\s*"max"']),
]


def chat(model, schema, prompt):
    user = json.dumps({"schema": schema, "user_prompt": prompt}, ensure_ascii=False)
    r = requests.post(f"{HOST}/api/chat", timeout=300, json={
        "model": model, "stream": False, "format": "json",
        "messages": [{"role": "system", "content": op.SYSTEM_PROMPT}, {"role": "user", "content": user}],
        "options": {"temperature": 0.2, "top_p": 0.8, "num_ctx": 4096}})
    r.raise_for_status()
    return r.json()["message"]["content"].strip()


class _Resp:
    status_code = 200
    def __init__(self, raw): self._raw = raw; self.text = raw
    def json(self): return {"message": {"content": self._raw}}


def repair(raw, schema, prompt):
    """Run the production planner path on an already-generated raw output."""
    with mock.patch.object(op.requests, "post", return_value=_Resp(raw)):
        return op.decide_pipeline_config(schema, prompt)


def score(cfg_or_raw, schema, prompt, expects):
    s = {k: False for k in ("valid_json", "structural", "safe", "compiles", "executable", "intent", "correct")}
    cfg = cfg_or_raw
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except ValueError:
            return s
    if not isinstance(cfg, dict):
        return s
    if isinstance(cfg.get("config"), dict):
        cfg = cfg["config"]
    s["valid_json"] = True
    try:
        s["structural"] = AssuranceAgent().assure(prompt, cfg, schema, run_semantic=False).overall_status == "pass"
    except Exception:
        s["structural"] = False
    try:
        s["safe"] = not plan_safety_issues(cfg)
    except Exception:
        s["safe"] = False
    ok = True
    try:
        stages = [st for st in cfg.get("stages", []) if isinstance(st, dict)]
        if not stages:
            ok = False
        for st in stages:
            if st.get("type") in ("notebook", "stream"):
                build_notebook_source(st, "acct")
    except Exception:
        ok = False
    s["compiles"] = ok
    s["executable"] = s["structural"] and s["safe"] and s["compiles"]
    blob = json.dumps(cfg)
    s["intent"] = all(re.search(rx, blob, re.IGNORECASE) for rx in expects)
    s["correct"] = s["executable"] and s["intent"]
    return s


def main():
    # Low-heat options: PAPER_EVAL_START=12 resumes at case 12; PAPER_EVAL_SKIP_FULL=1
    # skips the self-check conditions (2-3 extra model calls per case).
    start = int(os.getenv("PAPER_EVAL_START", "0"))
    skip_full = os.getenv("PAPER_EVAL_SKIP_FULL") == "1"
    results = json.load(open(OUT)) if start and os.path.exists(OUT) else []
    quiet = open(os.devnull, "w")
    for name, schema, prompt, expects in CASES[start:]:
        row = {"case": name}
        t = time.time()
        with mock.patch("sys.stdout", quiet):
            row["default"] = score(build_default_config(schema, prompt), schema, prompt, expects)
        for tag, model in (("base", "qwen2.5:7b-instruct"), ("ft", "planner-agent")):
            t0 = time.time()
            try:
                raw = chat(model, schema, prompt)
            except Exception as e:
                raw = ""
            row[f"{tag}_latency_s"] = round(time.time() - t0, 2)
            row[f"{tag}_raw"] = score(raw, schema, prompt, expects)
            with mock.patch.object(op, "_planner_model", return_value=model), mock.patch("sys.stdout", quiet):
                cfg, fb = repair(raw, schema, prompt)
            row[f"{tag}_repair"] = score(cfg, schema, prompt, expects)
            row[f"{tag}_repair_fallback"] = fb
            if skip_full:
                continue
            t1 = time.time()
            with mock.patch.object(op, "_planner_model", return_value=model), mock.patch("sys.stdout", quiet):
                cfg2, fb2, ver = plan_with_verification(
                    lambda f: op.decide_pipeline_config(schema, prompt, review_feedback=f), schema, prompt)
            row[f"{tag}_full"] = score(cfg2, schema, prompt, expects)
            row[f"{tag}_full_attempts"] = ver.get("attempts")
            row[f"{tag}_full_verified"] = ver.get("verified")
            row[f"{tag}_full_latency_s"] = round(time.time() - t1, 2)
        results.append(row)
        print(name, f"{time.time() - t:.0f}s",
              {k: v["correct"] for k, v in row.items() if isinstance(v, dict)}, flush=True)
        json.dump(results, open(OUT, "w"), indent=1)




def summarize(path):
    """Print the per-condition pass rates for a results file from main()."""
    import statistics
    res = json.load(open(path))
    conds = ["default", "base_raw", "base_repair", "base_full", "ft_raw", "ft_repair", "ft_full"]
    checks = ["valid_json", "structural", "safe", "compiles", "executable", "intent", "correct"]
    print(f"{len(res)} cases")
    print("condition".ljust(13) + "".join(c[:10].rjust(11) for c in checks))
    for c in conds:
        rs = [r for r in res if c in r]
        if rs:
            print(c.ljust(13) + f"(n={len(rs)})".ljust(0) + "".join(
                f"{100 * sum(r[c][k] for r in rs) / len(rs):10.0f}%" for k in checks))
    for tag in ("base", "ft"):
        res_t = [r for r in res if tag + "_full_latency_s" in r]
        if not res_t:
            continue
        print(f"{tag}: raw-call latency median {statistics.median(r[tag + '_latency_s'] for r in res_t):.1f}s; "
              f"full planner median {statistics.median(r[tag + '_full_latency_s'] for r in res_t):.1f}s; "
              f"fallback after repair {sum(r[tag + '_repair_fallback'] for r in res)}/{len(res)}; "
              f"verified {sum(bool(r[tag + '_full_verified']) for r in res_t)}/{len(res_t)}; "
              f"re-planned {sum((r[tag + '_full_attempts'] or 1) > 1 for r in res_t)}/{len(res_t)}")


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "--summary":
        summarize(os.path.join(_CALLER_CWD, sys.argv[2]))
    else:
        main()
