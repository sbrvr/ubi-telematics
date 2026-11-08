# UBI console (Streamlit): disputes -> driver drill-down (map, speed vs limit, score) -> AI draft -> override -> 2nd-person
# approve -> Lakebase + audit_log; Ask Genie. Proof: evidence/lakebase_genie_app.md
import json, os, threading
from concurrent.futures import ThreadPoolExecutor
import pandas as pd, pydeck as pdk, streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
from databricks.sdk import WorkspaceClient
import app_data as D
st.set_page_config("UBI underwriting console", layout="wide")
SP, G, GS = st.cache_resource(WorkspaceClient)(), D.G, os.getenv("GENIE_SPACE_ID")
DEMO = os.getenv("DEMO_SELF_APPROVE", "true") == "true"
h = st.context.headers; ME, TOK = h.get("X-Forwarded-Email", "local"), h.get("X-Forwarded-Access-Token")
WU = WorkspaceClient(host=SP.config.host, token=TOK, auth_type="pat") if TOK else SP  # SQL as the user: UC masks/filters apply

@st.cache_data(ttl=60, show_spinner=False)
def sql(user, q, **p): return pd.DataFrame(D.sql(WU, q, **p))  # cache keyed by user
def pg(q, a=None): return D.pgq(SP, q, a)
def par(*fs):  # parallel reads
    c = get_script_run_ctx()
    with ThreadPoolExecutor(len(fs), initializer=lambda: add_script_run_ctx(threading.current_thread(), c)) as x: return [f.result() for f in [x.submit(f) for f in fs]]
J = lambda v: json.dumps(v, default=str)
AU = "INSERT INTO ops.audit_log (actor, action, entity, before, after) SELECT %s, %s, "  # override write + audit row in one statement (atomic)
JB = "%s::jsonb, %s::jsonb FROM o"

view = st.sidebar.radio("View", ["Disputes", "Driver", "Ask Genie"], key="view"); st.sidebar.caption(f"Signed in as {ME}")
if view == "Disputes":
    k = sql(ME, f"SELECT * FROM {G}.gold_portfolio_kpis WHERE state = 'ALL'").iloc[0]
    F = dict(policies="{:,.0f}", ubi_enrolled="{:,.0f}", earned_premium="${:,.0f}", loss_ratio="{:.1%}", claim_freq="{:.2%}")
    for c, n in zip(st.columns(5), F): c.metric(n, F[n].format(float(k[n])))
    ds = pd.DataFrame(pg("SELECT dispute_id, driver_id, trip_id, customer_statement, status FROM ops.disputes ORDER BY status DESC, 1"))
    st.subheader("Dispute queue"); st.dataframe(ds, hide_index=True, use_container_width=True)
    i = st.selectbox("Dispute", ds.index, format_func=lambda i: f"#{ds.dispute_id[i]} {ds.driver_id[i]}")
    st.button("Review", on_click=lambda: st.session_state.update(driver=ds.driver_id[i], trip=ds.trip_id[i], view="Driver"))

elif view == "Driver":
    d = st.text_input("Driver", st.session_state.get("driver", "D-0427"))
    st_ok, x, tr = par(lambda: set(sql(ME, f"SELECT state FROM {G}.underwriter_states WHERE user_name = current_user()").get("state", [])),
        lambda: (pg("SELECT * FROM online.driver_features_online JOIN online.driver_scores_online USING (driver_id) WHERE driver_id = %s", (d,)) or [None])[0],
        lambda: sql(ME, f"SELECT trip_id, start_ts, end_ts, round(distance_mi, 1) mi, speeding_seconds s FROM {G}.silver_trips WHERE driver_id = :d ORDER BY 2 DESC", d=d))
    if not x: st.stop()
    if "*" not in st_ok and x["state"] not in st_ok: st.error("State not in your underwriter_states"); st.stop()  # synced rows carry no UC filter
    st.header(f"{d} ({x['state']})")
    F = dict(decile="{:.0f}", premium_modifier="{:+.1%}", score="{:.4f}", phone_handling_per_hr="{:.2f}")
    for c, n in zip(st.columns(4), F): c.metric(n, F[n].format(float(x[n])))
    ts = pd.Timestamp(x["scored_at"]); age = (pd.Timestamp.now("UTC") - (ts if ts.tzinfo else ts.tz_localize("UTC"))).total_seconds() / 3600
    old = age > float(os.getenv("STALE_HOURS", "36"))  # stale gate: no approval queues a renewal on an old score
    (st.error if old else st.caption)(f"Score v{x['model_version']}, {age:.0f} h old; features to {x['as_of_date']}" + (": STALE, approvals locked" if old else ""))
    st.markdown("**Reasons:** " + " · ".join(x["reason_codes"]) + "  \n_Priced at renewal only, filed band -30%..+15%._")
    ids = tr.trip_id.tolist(); t = st.selectbox("Trip", ids, ids.index(st.session_state["trip"]) if st.session_state.get("trip") in ids else 0)
    r = tr.set_index("trip_id").loc[t]
    pts, ev = par(lambda: sql(ME, f"SELECT ts, lat, lon, speed_mph, maxspeed_mph FROM {G}.silver_gps_matched WHERE device_id = :d AND ts BETWEEN :a AND :b ORDER BY ts",
              d="TD-" + d[2:], a=r.start_ts, b=r.end_ts).set_index("ts").astype(float), lambda: sql(ME, f"""SELECT e.event_type, date_format(e.ts, 'HH:mm:ss') at,
      g.lat, g.lon FROM {G}.silver_events e JOIN {G}.silver_gps_matched g ON g.device_id = e.device_id AND g.ts = e.ts WHERE e.trip_id = :t""", t=t))
    ev = ev.astype({"lat": float, "lon": float}) if len(ev) else ev
    st.caption(f"Trip {t}: {r.mi} mi, {r.s} s >10 mph over the limit, {len(ev)} device events")
    st.pydeck_chart(pdk.Deck(layers=[pdk.Layer("PathLayer", [{"path": pts[["lon", "lat"]].values.tolist()}], get_path="path", get_color=[37, 99, 235], width_min_pixels=3),
        pdk.Layer("ScatterplotLayer", ev, get_position=["lon", "lat"], get_fill_color=[220, 38, 38], get_radius=40, pickable=True)],
        initial_view_state=pdk.ViewState(latitude=pts.lat.mean(), longitude=pts.lon.mean(), zoom=12), tooltip={"text": "{event_type} {at}"}))
    st.line_chart(pts[["speed_mph", "maxspeed_mph"]], height=180)
    dsp = pg("SELECT dispute_id FROM ops.disputes WHERE driver_id = %s AND trip_id = %s", (d, t))
    if dsp and st.button("Draft with AI (a human decides)"):
        o = D.draft(WU, dsp[0]["dispute_id"], ME, SP)
        st.info(f"**{o['recommended_action']}**: {o['rationale']}"); st.write(o["summary"]); st.text_area("Letter draft", o["customer_letter"], height=160)
    with st.form("ov"):
        st.subheader("Propose override"); new = st.number_input("New renewal modifier", -0.30, 0.15, 0.0, 0.01)
        rc, why = st.text_input("Reason code", "PASSENGER_PHONE_ATTESTED"), st.text_input("Justification")
        if st.form_submit_button("Propose"):
            o = pg("""WITH o AS (INSERT INTO ops.overrides (driver_id, old_modifier, new_modifier, reason_code, justification, requested_by) VALUES
              (%s, %s, %s, %s, %s, %s) RETURNING override_id) """ + AU + "'override:' || override_id, " + JB + " RETURNING (SELECT override_id FROM o) override_id",
              (d, x["premium_modifier"], new, rc, why, ME, ME, "override_proposed", J({"modifier": x["premium_modifier"]}), J({"driver_id": d, "modifier": new, "reason": rc})))[0]
            st.success(f"Override {o['override_id']} proposed; a second person must approve.")
    for o in pg("SELECT * FROM ops.overrides WHERE driver_id = %s ORDER BY 1 DESC", (d,)):
        c1, c2, c3 = st.columns([5, 1, 1]); selfie, i = o["requested_by"] == ME, o["override_id"]
        c1.write(f"#{i} {o['old_modifier']} -> {o['new_modifier']} {o['reason_code']}, {o['requested_by']}: **{o['status']}** {o['approved_by'] or ''}"
                 + (" (demo self-approval)" if o["demo"] else ""))
        lock = o["status"] != "proposed" or (selfie and not DEMO) or old  # four eyes unless the labelled demo flag
        dec = "approved" if c2.button("Approve", key=f"a{i}", disabled=lock) else "rejected" if c3.button("Reject", key=f"r{i}", disabled=lock) else None
        if dec:
            pg("""WITH o AS (UPDATE ops.overrides SET status = %s, approved_by = %s, decided_at = now(), demo = demo OR %s WHERE override_id = %s RETURNING 1),
              r AS (UPDATE ops.disputes SET status = 'resolved' WHERE %s AND driver_id = %s AND status = 'open') """ + AU + "%s, " + JB,
               (dec, ME, selfie, i, dec == "approved", d, ME, "override_" + dec, f"override:{i}", J({"modifier": o["old_modifier"]}), J({"demo_self_approval": selfie,
                "policy_admin": f"renewal adjustment {o['new_modifier']} queued for P-{d[2:]}" if dec == "approved" else None}))); st.rerun()

else:
    st.header("Ask Genie")
    for m in st.session_state.setdefault("chat", []): st.chat_message(m[0]).write(m[1])
    if q := st.chat_input("e.g. Which overrides were approved this week and why?"):
        st.chat_message("user").write(q); o = D.genie(WU, GS, q); st.session_state.chat += [("user", q), ("assistant", o["text"])]
        with st.chat_message("assistant"):
            st.write(o["text"]); st.dataframe(pd.DataFrame(o["rows"]), hide_index=True)
            if o["sql"]: st.code(o["sql"], language="sql")
