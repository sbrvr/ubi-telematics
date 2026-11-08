# App data layer: warehouse SQL, Lakebase (psycopg2), LLM dispute drafts, Genie. Used by app.py and run_checks;
# the LLM drafts, a human decides. Proof: evidence/lakebase_genie_app.md
import hashlib, json, os, re, time, psycopg2
WH, LLM = os.getenv("WAREHOUSE_ID", "<warehouse-id>"), os.getenv("LLM_ENDPOINT", "databricks-claude-fable-5-1")
G, BR = "surajb.ubi_telematics", "projects/ubi-ops-sbang/branches/production"
PROMPT = """You assist a Meridian Mutual underwriter reviewing a telematics dispute. Use ONLY the JSON facts given. Rules:
- Cite real event timestamps (HH:MM:SS) and speeds from the facts; never invent data.
- Telematics pricing applies at renewal only, through the filed rating plan; never promise a refund, discount or premium change.
- You recommend; a human underwriter decides and a second person approves any override.
Return only JSON: {"summary": "<exactly 4 sentences for the underwriter>", "recommended_action": "uphold|adjust|escalate",
"rationale": "<1-2 sentences>", "customer_letter": "<short, plain-language letter; explains the review, no pricing promises>"}"""

def sql(w, q, **p):  # -> list of dicts; :named params
    r = w.api_client.do("POST", "/api/2.0/sql/statements", body={"warehouse_id": WH, "statement": q, "wait_timeout": "30s",
        "parameters": [{"name": k, "value": str(v)} for k, v in p.items()]})
    cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
    return [dict(zip(cols, row)) for row in (r.get("result") or {}).get("data_array") or []]

K = {}  # pooled Lakebase connection per identity, kept until near token expiry
def pg(w, fresh=False):  # Lakebase as the calling identity (OAuth)
    k, exp = K.get(id(w), (None, 0))
    if fresh or not k or k.closed or time.time() > exp:
        ep = w.api_client.do("GET", f"/api/2.0/postgres/{BR}/endpoints/primary")["status"]["hosts"]["host"]
        tok = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": BR + "/endpoints/primary"})["token"]
        k = psycopg2.connect(host=ep, dbname="ubi_ops", user=w.current_user.me().user_name, password=tok, sslmode="require"); k.autocommit = True
        K[id(w)] = (k, time.time() + 2700)
    return k

def pgq(w, q, a=None):  # -> list of dicts (or rowcount); reconnects once if dropped
    try: c = pg(w).cursor(); c.execute(q, a)
    except psycopg2.OperationalError: c = pg(w, True).cursor(); c.execute(q, a)
    with c: return [dict(zip([d[0] for d in c.description], r)) for r in c.fetchall()] if c.description else c.rowcount

def trip_facts(w, trip_id):  # events + speed vs limit from silver
    t = sql(w, f"""SELECT trip_id, driver_id, start_ts, end_ts, round(distance_mi, 1) miles, duration_s, speeding_seconds, round(night_share, 2) night
      FROM {G}.silver_trips WHERE trip_id = :t""", t=trip_id)[0]
    t["events"] = sql(w, f"""SELECT date_format(ts, 'HH:mm:ss') at, event_type, round(speed_mph) mph, round(magnitude, 1) magnitude
      FROM {G}.silver_events WHERE trip_id = :t ORDER BY ts""", t=trip_id)
    t["speed_vs_limit"] = sql(w, f"""SELECT road_class, count(*) secs, round(avg(speed_mph)) mph, max(maxspeed_mph) lim, count_if(speed_mph >
      maxspeed_mph + 10) over10 FROM {G}.silver_gps_matched WHERE device_id = :d AND ts BETWEEN :a AND :b GROUP BY 1 ORDER BY 2 DESC LIMIT 4""",
      d="TD-" + t["driver_id"][2:], a=t["start_ts"], b=t["end_ts"])
    return t

def draft(w, dispute_id, actor, ws=None):  # w: SQL identity, ws: app SP; never writes overrides
    ws = ws or w
    d = pgq(ws, "SELECT driver_id, trip_id, customer_statement FROM ops.disputes WHERE dispute_id = %s", (dispute_id,))[0]
    s = sql(w, f"SELECT decile, premium_modifier, reason_codes FROM {G}.gold_driver_scores WHERE driver_id = :d", d=d["driver_id"])[0]
    facts = dict(customer_statement=d["customer_statement"], score=s, trip=trip_facts(w, d["trip_id"]))
    msg = [{"role": "system", "content": PROMPT}, {"role": "user", "content": json.dumps(facts, default=str)}]
    h = hashlib.sha256(json.dumps(msg).encode()).hexdigest()
    c = ws.api_client.do("POST", f"/serving-endpoints/{LLM}/invocations", body={"messages": msg, "max_tokens": 1500})["choices"][0]["message"]["content"]
    txt = c if isinstance(c, str) else "".join(b.get("text", "") for b in c if b.get("type") == "text")
    out = json.loads(re.search(r"\{.*\}", txt, re.S).group(0))
    assert out["recommended_action"] in ("uphold", "adjust", "escalate") and {"summary", "rationale", "customer_letter"} <= out.keys(), out
    pgq(ws, "INSERT INTO ops.audit_log (actor, action, entity, before, after, llm_prompt_hash) VALUES (%s, 'llm_draft', %s, %s, %s, %s)",
        (actor, f"dispute:{dispute_id}", json.dumps(facts, default=str), json.dumps(out), h))
    pgq(ws, "UPDATE ops.disputes SET llm_summary = %s WHERE dispute_id = %s", (out["summary"], dispute_id))
    return dict(out, prompt_hash=h, facts=facts)

def genie(w, space, q):  # -> text, SQL, rows
    g = f"/api/2.0/genie/spaces/{space}"; r = w.api_client.do("POST", g + "/start-conversation", body={"content": q})
    u = f"{g}/conversations/{r['conversation_id']}/messages/{r['message_id']}"
    for _ in range(40):
        m = w.api_client.do("GET", u)
        if m["status"] in ("COMPLETED", "FAILED", "CANCELLED"): break
        time.sleep(3)
    out = dict(status=m["status"], text=" ".join(a["text"]["content"] for a in m.get("attachments") or [] if a.get("text")), sql=None, rows=[])
    for a in [a for a in m.get("attachments") or [] if a.get("query")][:1]:
        res = w.api_client.do("GET", f"{u}/attachments/{a['attachment_id']}/query-result")["statement_response"]
        out.update(sql=a["query"]["query"], rows=(res.get("result") or {}).get("data_array") or [])
    return out
