# Databricks notebook source
# Genie space "UBI Underwriting Analyst" over gold + live Lakebase disputes/overrides: instructions, trusted SQL, 12-question
# benchmark. mode=create | bench (native eval vs expected SQL). Proof: evidence/lakebase_genie_app.md
import json, time
from databricks.sdk import WorkspaceClient
api, WH, G, L = WorkspaceClient().api_client.do, "<warehouse-id>", "surajb.ubi_telematics", "ubi_ops_sbang.ops"
REF = "/Volumes/surajb/ubi_telematics/ref/genie"; dbutils.widgets.text("mode", "create")
TABLES = [G + t for t in [".bronze_claims", ".bronze_policies", ".gold_driver_features", ".gold_driver_scores", ".gold_portfolio_kpis"]] + [L + ".disputes", L + ".overrides"]
TEXT = """Meridian Mutual UBI underwriting. Loss ratio = incurred / earned premium (base_premium * exposure_years); claim frequency =
claims / exposure years; book totals = gold_portfolio_kpis WHERE state = 'ALL'. Rates are per 100 miles; decile 10 = riskiest. premium_modifier
is a renewal-only filed change, capped -30%..+15%, never mid-term. driver_id joins features, scores, policies, disputes, overrides; claims
join policies on policy_id. Approved override: status = 'approved'; this week = decided_at in the last 7 days."""
CL = f"LEFT JOIN (SELECT policy_id, count(*) n, sum(incurred) inc FROM {G}.bronze_claims GROUP BY 1) c USING (policy_id)"
TRUSTED = [
    ("Loss ratio and claim frequency by risk decile", f"""SELECT s.decile, count(*) drivers, sum(nvl(n, 0)) claims, sum(nvl(inc, 0)) /
  sum(base_premium * exposure_years) loss_ratio, sum(nvl(n, 0)) / sum(exposure_years) claim_freq
  FROM {G}.gold_driver_scores s JOIN {G}.bronze_policies USING (driver_id) {CL} GROUP BY 1 ORDER BY 1"""),
    ("Top-risk drivers by state", f"""SELECT f.state, driver_id, s.score, s.decile, s.premium_modifier FROM {G}.gold_driver_scores s
  JOIN {G}.gold_driver_features f USING (driver_id) QUALIFY rank() OVER (PARTITION BY f.state ORDER BY s.score DESC) <= 5"""),
    ("Override rate and reasons this week", f"""SELECT reason_code, count(*) n, count_if(status = 'approved') approved,
  count(*) / (SELECT count(*) FROM {G}.gold_driver_scores) override_rate FROM {L}.overrides WHERE created_at >= current_date() - 7 GROUP BY 1"""),
    ("Average premium modifier by territory", f"""SELECT territory, count(*) drivers, avg(premium_modifier) avg_modifier
  FROM {G}.gold_driver_scores JOIN {G}.bronze_policies USING (driver_id) GROUP BY 1 ORDER BY 1""")]
K, S = f"{G}.gold_portfolio_kpis", f"{G}.gold_driver_scores"
BENCH = [  # (question, expected SQL, independent of Genie)
    ("How many drivers are enrolled in UBI?", f"SELECT ubi_enrolled FROM {K} WHERE state = 'ALL'"),
    ("What is the overall loss ratio of the book?", f"SELECT loss_ratio FROM {K} WHERE state = 'ALL'"),
    ("Which state has the highest claim frequency?", f"SELECT state FROM {K} WHERE state <> 'ALL' ORDER BY claim_freq DESC LIMIT 1"),
    ("How many drivers are in risk decile 10?", f"SELECT count(*) FROM {S} WHERE decile = 10"),
    ("What is the average premium modifier for decile 10 drivers?", f"SELECT avg(premium_modifier) FROM {S} WHERE decile = 10"),
    ("Which territory has the highest average premium modifier?", f"""SELECT territory FROM {S} JOIN {G}.bronze_policies USING (driver_id)
  GROUP BY 1 ORDER BY avg(premium_modifier) DESC LIMIT 1"""),
    ("Which 5 drivers have the highest risk score?", f"SELECT driver_id FROM {S} ORDER BY score DESC LIMIT 5"),
    ("What are the reason codes for driver D-0427?", f"SELECT reason_codes FROM {S} WHERE driver_id = 'D-0427'"),
    ("How many disputes are open?", f"SELECT count(*) FROM {L}.disputes WHERE status = 'open'"),
    ("What is the average hard brakes per 100 miles in decile 10?", f"""SELECT avg(hard_brakes_per_100mi)
  FROM {G}.gold_driver_features JOIN {S} USING (driver_id) WHERE decile = 10"""),
    ("What is the claim frequency in decile 10 vs decile 1?", f"SELECT decile, claim_freq FROM ({TRUSTED[0][1]}) WHERE decile IN (1, 10)"),
    ("Which overrides were approved this week and why?", f"""SELECT driver_id, reason_code FROM {L}.overrides
  WHERE status = 'approved' AND decided_at >= current_date() - 7""")]

# COMMAND ----------
ids = iter(f"{i:032x}" for i in range(1, 99))  # sorted hex ids
space = {"version": 2, "config": {"sample_questions": [{"id": next(ids), "question": [q]} for q, _ in BENCH[:4]]},
         "data_sources": {"tables": [{"identifier": t} for t in TABLES]},
         "instructions": {"text_instructions": [{"id": next(ids), "content": [TEXT]}],
                          "example_question_sqls": [{"id": next(ids), "question": [q], "sql": [s]} for q, s in TRUSTED]},
         "benchmarks": {"questions": [{"id": next(ids), "question": [q], "answer": [{"format": "SQL", "content": [s]}]} for q, s in BENCH]}}
try: sid = json.load(open(REF + "_space.json"))["space_id"]
except OSError: sid = None
body = {"warehouse_id": WH, "serialized_space": json.dumps(space), "title": "UBI Underwriting Analyst"}
if dbutils.widgets.get("mode") == "create":
    sid = api("PATCH", f"/api/2.0/genie/spaces/{sid}", body=body)["space_id"] if sid else api("POST", "/api/2.0/genie/spaces", body=body)["space_id"]
    open(REF + "_space.json", "w").write(json.dumps({"space_id": sid}))

# COMMAND ----------
if dbutils.widgets.get("mode") == "bench":  # native eval: Genie answer vs expected SQL
    g = f"/api/2.0/genie/spaces/{sid}/eval-runs"; e = api("POST", g, body={})["eval_run_id"]
    while api("GET", f"{g}/{e}")["eval_run_status"] == "RUNNING": time.sleep(15)
    rs = [dict(r, **api("GET", f"{g}/{e}/results/{r['result_id']}")) for r in api("GET", f"{g}/{e}/results").get("eval_results", [])]
    out = [dict(q=r["question"], assessment=r.get("assessment"), reasons=r.get("assessment_reasons")) for r in rs]
    ok = sum(o["assessment"] == "GOOD" for o in out)
    open(REF + "_bench.json", "w").write(json.dumps(dict(space_id=sid, eval_run_id=e, passed=ok, total=len(out), results=out), indent=1))
    print(ok, "/", len(out))
