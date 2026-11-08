# Databricks notebook source
# Lakebase Autoscaling: project, UC database catalog, synced tables (scores TRIGGERED, features SNAPSHOT), native ops
# tables, 8 seeded disputes, point-lookup latency. Idempotent. Proof: evidence/lakebase_genie_app.md
import json, time, numpy as np, psycopg2  # preinstalled; pip psycopg wheels abort at import on serverless
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(); api = w.api_client.do
PJ = "projects/ubi-ops-sbang"; BR, CAT, DB, S = PJ + "/branches/production", "ubi_ops_sbang", "ubi_ops", "surajb.ubi_telematics"
def make(path, idq, body):  # create if missing; wait for the operation
    try: op = api("POST", "/api/2.0/postgres/" + path, query=idq, body=body)
    except Exception as e:
        if "exist" in str(e).lower(): return
        raise
    while not op.get("done"): time.sleep(10); op = api("GET", "/api/2.0/postgres/" + op["name"])
    assert not op.get("error"), op
make("projects", {"project_id": PJ[9:]}, {"spec": {"pg_version": 17, "default_endpoint_settings": {"autoscaling_limit_min_cu": 0.5,
    "autoscaling_limit_max_cu": 2, "suspend_timeout_duration": "300s"}}})  # scale to zero
make("catalogs", {"catalog_id": CAT}, {"spec": {"postgres_database": DB, "branch": BR, "create_database_if_missing": True}})
for t, src, mode in [("driver_scores_online", "gold_driver_scores", "TRIGGERED"), ("driver_features_online", "gold_driver_features_sync", "SNAPSHOT")]:
    make("synced_tables", {"synced_table_id": f"{CAT}.online.{t}"}, {"spec": {"branch": BR, "postgres_database": DB, "source_table_full_name": f"{S}.{src}",
        "primary_key_columns": ["driver_id"], "scheduling_policy": mode, "create_database_objects_if_missing": True,
        "new_pipeline_spec": {"storage_catalog": S[:6], "storage_schema": S[7:]}}})

# COMMAND ----------
ep = api("GET", f"/api/2.0/postgres/{BR}/endpoints/primary")["status"]["hosts"]["host"]
def conn():
    tok = api("POST", "/api/2.0/postgres/credentials", body={"endpoint": BR + "/endpoints/primary"})["token"]
    k = psycopg2.connect(host=ep, dbname=DB, user=w.current_user.me().user_name, password=tok, sslmode="require"); k.autocommit = True
    return k.cursor()
DDL = """CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.disputes (dispute_id serial PRIMARY KEY, driver_id text, trip_id text, customer_statement text,
  status text DEFAULT 'open', llm_summary text, created_at timestamptz DEFAULT now(), UNIQUE (driver_id, trip_id));
CREATE TABLE IF NOT EXISTS ops.overrides (override_id serial PRIMARY KEY, driver_id text, old_modifier numeric, new_modifier numeric
  CHECK (new_modifier BETWEEN -0.30 AND 0.15), reason_code text, justification text, requested_by text NOT NULL, approved_by text,
  status text NOT NULL DEFAULT 'proposed', demo boolean NOT NULL DEFAULT false, created_at timestamptz DEFAULT now(), decided_at timestamptz,
  CONSTRAINT four_eyes CHECK (approved_by IS NULL OR demo OR approved_by <> requested_by));
CREATE TABLE IF NOT EXISTS ops.audit_log (id bigserial PRIMARY KEY, ts timestamptz DEFAULT now(), actor text NOT NULL, action text,
  entity text, before jsonb, after jsonb, llm_prompt_hash text);
CREATE OR REPLACE FUNCTION ops.append_only() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'append-only'; END$$;
CREATE OR REPLACE TRIGGER audit_append_only BEFORE UPDATE OR DELETE ON ops.audit_log FOR EACH ROW EXECUTE FUNCTION ops.append_only();"""
SAY = ["My passenger was using my phone, not me", "That hard brake was a deer at night", "My son borrowed the car that week",
       "GPS speed is wrong, I was under the limit", "Night trips are my hospital shift", "I was a passenger in a friend's car",
       "I braked for a school bus", "It was a closed track day"]
# D-0427 day-27 trip + latest trip of 7 decile-10 drivers
seed = spark.sql(f"""(SELECT driver_id, trip_id FROM {S}.silver_trips WHERE trip_id = 'TD-0427-20261027174200') UNION ALL (SELECT driver_id,
  max_by(trip_id, start_ts) FROM {S}.silver_trips JOIN {S}.gold_driver_scores USING (driver_id) WHERE decile = 10 GROUP BY 1 ORDER BY 1 LIMIT 7)""").collect()
c = conn(); c.execute(DDL)
for (d, t), say in zip(seed, SAY):
    c.execute("INSERT INTO ops.disputes (driver_id, trip_id, customer_statement) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (d, t, say))
c.execute("SELECT count(*) FROM ops.disputes"); n = c.fetchone()[0]
dbutils.widgets.text("app_sp", ""); sp = dbutils.widgets.get("app_sp")  # app SP role: read online, write ops, append-only audit
if sp: c.execute(f"""GRANT USAGE ON SCHEMA ops, online TO "{sp}"; GRANT SELECT ON ALL TABLES IN SCHEMA online TO "{sp}"; GRANT USAGE ON ALL
  SEQUENCES IN SCHEMA ops TO "{sp}"; GRANT SELECT, INSERT, UPDATE ON ops.disputes, ops.overrides TO "{sp}"; GRANT INSERT ON ops.audit_log TO "{sp}";""")

# COMMAND ----------
# After the first sync: 1,000 point lookups on synced scores
for _ in range(90):
    try:
        c.execute("SELECT count(*) FROM online.driver_scores_online")
        if c.fetchone()[0]: break
    except psycopg2.Error: pass
    time.sleep(20)
c.execute("SELECT driver_id FROM online.driver_scores_online"); ids = [r[0] for r in c.fetchall()]; lat = []
t0 = time.perf_counter(); c = conn(); c.execute("SELECT 1"); cold = (time.perf_counter() - t0) * 1000
for i in np.random.default_rng(7).choice(ids, 1000):
    t = time.perf_counter(); c.execute("SELECT score, decile, premium_modifier, reason_codes FROM online.driver_scores_online WHERE driver_id = %s", (i,)); c.fetchone()
    lat.append((time.perf_counter() - t) * 1000)
Q, K = "SELECT driver_id, decile, premium_modifier FROM ", lambda rs: {(a, int(b), round(float(m), 4)) for a, b, m in rs}  # gold vs synced
gs = K(spark.sql(Q + "surajb.ubi_telematics.gold_driver_scores").collect()); c.execute(Q + "online.driver_scores_online"); ls = K(c.fetchall())
c.execute("SELECT extract(epoch FROM now() - min(scored_at)) / 3600 FROM online.driver_scores_online"); age = round(float(c.fetchone()[0]), 1)
sim = len(gs ^ set(sorted(ls)[1:]))  # self-test: one row missing (partial sync) must be flagged
out = dict(disputes=n, rows=len(ids), connect_ms=round(cold, 1), **{f"p{k}_ms": round(float(np.percentile(lat, k)), 2) for k in (50, 95)},
           gold=len(gs), mismatched=len(gs ^ ls), partial_sync_test="detected" if sim else "MISSED", score_age_h=age)
open("/Volumes/surajb/ubi_telematics/ref/lakebase_bench.json", "w").write(json.dumps(out)); print(out)
assert not out["mismatched"] and sim, out  # fail the job before Genie/app
