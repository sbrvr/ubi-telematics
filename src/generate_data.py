# Databricks notebook source
# Seeded synthetic UBI on Austin OSM roads: 1k drivers, ~50M 1 Hz GPS, events, policies, claims, defects;
# asserts invariants (ref/gen_stats.json). Proof: evidence/run_log.md
import json, pickle, numpy as np, pandas as pd, osmnx as ox
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra
V = "/Volumes/surajb/ubi_telematics"; L, R, STG = V + "/landing", V + "/ref", "surajb.ubi_telematics._gen"
ND, T0, D0 = 1000, pd.Timestamp("2026-10-01"), "drop=2026-10-01-00"; rg = np.random.default_rng(42)
spark.conf.set("spark.sql.session.timeZone", "UTC")
cdf, ow = spark.createDataFrame, lambda x: x.write.mode("overwrite")

# COMMAND ----------
# Roads (unsimplified; maxspeed by class); graph ships in UDF closure
GP = R + "/osm_graph.pkl"  # cache: reproducible, no API outage
try: G = pickle.load(open(GP, "rb"))
except OSError:
    G = ox.add_edge_speeds(ox.graph_from_point((30.2672, -97.7431), dist=12000, network_type="drive", simplify=False), fallback=40)
    pickle.dump(G, open(GP, "wb"))
nd, ed = ox.graph_to_gdfs(ox.convert.to_undirected(G)); ed = ed.reset_index(); mph = (ed.speed_kph / 1.609).round()
ow(cdf(pd.DataFrame(dict(segment_id=ed.index, wkt=ed.geometry.to_wkt(), maxspeed_mph=mph, road_class=ed.highway.explode().groupby(level=0).first()))).selectExpr("*", "h3_coverash3(wkt, 9) h3_res9")).parquet(R + "/road_segments")
N, gi = len(nd), nd.index.get_indexer; NX, NY = nd.x.values, nd.y.values
u, v, w, sp = gi(ed.u), gi(ed.v), ed.length.values, mph.values * .447
a = pd.DataFrame(dict(i=np.r_[u, v], j=np.r_[v, u], w=np.r_[w, w], s=np.r_[sp, sp])).sort_values("w").drop_duplicates(["i", "j"])
A, B = [csr_matrix((a[c], (a.i, a.j)), (N, N)) for c in "ws"]

# Drivers, trips, policies; claims ~ Poisson(latent risk x rating x exposure)
PZ = "safe average risky phone".split(); P = np.repeat(PZ, [500, 350, 120, 30]); rg.shuffle(P)
j = np.argmax(P == "phone"); P[[j, 426]] = P[[426, j]]  # D-0427 = phone persona
pz = lambda *x: dict(zip(PZ, x)); SF, NIGHT, WKND, RISK = pz(.92, 1, 1.15, 1.08), pz(.03, .07, .2, .1), pz(0, .2, .5, .3), pz(.6, 1, 2.2, 1.8)
d = pd.DataFrame(dict(driver_id=[f"D-{i:04d}" for i in range(1, ND + 1)], persona=P)); d["device_id"] = "TD-" + d.driver_id.str[2:]; trad = 1
for c, m, p in [("age_band", {"16-24": 1.6, "25-34": 1.1, "35-54": .9, "55-64": .95, "65+": 1.15}, [.12, .22, .38, .16, .12]),
    ("vehicle", dict(sedan=1, suv=1.05, pickup=1.1, sports=1.4), [.4, .35, .2, .05]), ("territory", dict(T1=1.2, T2=1.05, T3=1, T4=.95, T5=.9), None)]:
    d[c] = rg.choice(list(m), ND, p=p); trad *= d[c].map(m)
d["state"] = np.where(rg.random(ND) < .95, "TX", rg.choice(["OK", "LA", "NM"], ND)); d["consent"] = rg.random(ND) < .95
d.loc[426, ["state", "consent"]] = ["TX", True]
sk = np.zeros(ND); sk[rg.choice(np.r_[:426, 427:ND], 5, 0)] = rg.integers(1800, 7200, 5); SK = ",".join(f"'{x}'" for x in d.device_id[sk > 0])
loc = rg.integers(0, N, (ND, 5)); SLOT = np.r_[0, 2, 22, 6:22:2]; plan = []  # home/work/POIs; 2h slots
for i in range(ND):
    z, cur = P[i], 0
    for dd in range(30):
        n = min(3, rg.integers(1, 4) + ((dd + 3) % 7 > 4 and rg.random() < WKND[z]))
        hh = np.sort(rg.choice(SLOT, n, False, p=np.r_[[NIGHT[z] / 3] * 3, [(1 - NIGHT[z]) / 8] * 8]) + rg.random(n))
        disp = i == 426 and dd == 26  # disputed trip
        for h in ([17.7] if disp else hh):
            o = loc[i, cur]; cur = (cur + rg.integers(1, 5)) % 5; plan.append((i, int(T0.timestamp() + (dd * 24 + h) * 3600), o, loc[i, cur], disp))
plan = pd.DataFrame(plan, columns="i start o d disp".split())
risk = d.persona.map(RISK) * rg.lognormal(0, .2, ND); ex = rg.uniform(1, 3, ND).round(2)
lam = risk * trad; lam *= .065 * ex.sum() / (lam * ex).sum()  # 6.5%/yr
dt = lambda x: (T0 - pd.to_timedelta(x, "D")).strftime("%Y-%m-%d"); s0 = rg.integers(0, 182, ND)
pol = d.drop(columns="persona").assign(policy_id="P-" + d.driver_id.str[2:], exposure_years=ex, base_premium=(1350 * trad).round(), term_start=dt(s0), term_end=dt(s0 - 182))
ci = np.repeat(range(ND), rg.poisson(lam * ex)); nc = len(ci)
clm = pol.iloc[ci][["policy_id", "driver_id"]].assign(claim_id=[f"C-{x:05d}" for x in range(nc)], loss_date=dt(rg.random(nc) * ex[ci] * 365), incurred=rg.lognormal(8.3, 1, nc).round())
dz = plan[plan.disp].assign(driver_id="D-0427", device_id="TD-0427")
for pa, x in [(L + "/policies/" + D0, pol), (L + "/claims/" + D0, clm), (R + "/disputes_seed", dz),
    (R + "/driver_truth", d.assign(latent_risk=risk)[["driver_id", "persona", "latent_risk"]])]:
    ow(cdf(x).selectExpr("to_json(struct(*))").coalesce(1)).text(pa)

# Per driver: shortest paths -> 1 Hz GPS (5-15 m noise), dropouts, events
EVR = dict(hard_brake=[1, 2.5, 7, 3], harsh_corner=[1, 2, 5, 2.5], rapid_accel=[.8, 2, 5, 2], phone_handling=[.3, 1, 1.5, 8])  # /100 mi; phone /h
def gen(df):
    i = df.i.iloc[0]; z = P[i]; rs = np.random.default_rng(42 + i); src = np.unique(df.o); pr = dict(zip(src, dijkstra(A, indices=src, return_predecessors=True)[1])); out = []
    for t in df.sort_values("start").itertuples():
        p = [t.d]
        while p[-1] >= 0 and p[-1] != t.o: p.append(pr[t.o][p[-1]])
        if p[-1] < 0 or len(p) < 3: continue
        p = np.array(p[::-1]); X, Y = NX[p], NY[p]; S = (B[p[:-1], p[1:]].A1 * SF[z] * rs.normal(1, .08)).clip(3)
        D = np.r_[0, np.cumsum(np.hypot(np.diff(X) * 96e3, np.diff(Y) * 110.6e3))]; T = np.r_[0, np.cumsum(np.diff(D) / S)]
        s = np.arange(T[-1]); m = len(s); q = np.interp(s, T, D); sg = rs.uniform(5, 15); f = lambda c, k: np.interp(q, D, c) + rs.normal(0, sg, m) / k
        g = pd.DataFrame(dict(kind="g", tt=t.start + s, lon=f(X, 96e3), lat=f(Y, 110.6e3), speed_mph=(np.interp(s, T[1:], S) / .447 + rs.normal(0, 1.5, m)).clip(0)))
        for et, r in EVR.items():
            ph = et == "phone_handling"; k = 7 * ph if t.disp else rs.poisson(r[PZ.index(z)] * (m / 3600 if ph else D[-1] / 160934))
            out.append(g.iloc[rs.integers(0, m, k)].assign(kind="e", event_type=et, magnitude=rs.uniform(*[(.3, .7), (10, 60)][ph], k)))  # g | s
        if rs.random() < .15: x = rs.integers(m); g = g.iloc[np.r_[:x, x + rs.integers(30, 120):m]]  # dropouts
        out.append(g)
    return pd.concat(out).assign(driver_id=d.driver_id[i], device_id=d.device_id[i], persona=z, ts=lambda r: r.tt + sk[i])
sch = "kind string,driver_id string,device_id string,persona string,event_type string,tt double,ts double,lat double,lon double,speed_mph double,magnitude double"
ow(cdf(plan).repartition(200, "i").groupBy("i").applyInPandas(gen, sch)).saveAsTable(STG)

# Hourly drops: 2% dup, 3% late (+10 min), 0.5% malformed; day 30 held back
for k, nm, c in [("g", "gps", "speed_mph"), ("e", "events", "speed_mph, event_type, magnitude")]:
    ow(spark.sql(f"""SELECT if(rand(3) < .005, left(v, length(v) div 2), v) value, drop FROM (SELECT explode(sequence(1, if(r2 < .02, 2, 1))) _n,
    to_json(struct(device_id, driver_id, string(timestamp_seconds(floor(ts))) AS ts, lat, lon, {c})) v,
    date_format(timestamp_seconds(tt + if(minute(timestamp_seconds(tt)) >= 50 AND r1 < .18, 600, 0)), 'yyyy-MM-dd-HH') drop
    FROM (SELECT *, rand(1) r1, rand(2) r2 FROM {STG} WHERE kind = '{k}'))""").repartition("drop")).partitionBy("drop").text(f"{L}/{nm}")
    dbutils.fs.rm(f"{L}/holdback/{nm}", True)
    [dbutils.fs.mv(f.path, f"{L}/holdback/{nm}/{f.name}", True) for f in dbutils.fs.ls(f"{L}/{nm}") if f.name >= "drop=2026-10-30"]

# COMMAND ----------
# Invariants, on the written files
st = {}; rd = lambda p: spark.read.text(p).selectExpr("*", "_metadata.file_path f", "try_parse_json(value) j")
for nm in ["gps", "events"]:
    st[nm] = rd(f"{L}/{nm}").union(rd(f"{L}/holdback/{nm}")).selectExpr("count(*) rows", "count(DISTINCT f) files", "avg(int(j IS NULL)) malformed",
    "count(j) / count(DISTINCT if(j IS NULL, NULL, value)) - 1 dup",
    f"avg(if(j IS NULL OR j:device_id::string IN ({SK}), NULL, int(drop != replace(left(j:ts::string, 13), ' ', '-')))) late").first().asDict()
t0 = dz.start.iloc[0]; rw = spark.sql(f"""SELECT persona, sum(int(event_type = 'hard_brake')) / sum(int(kind = 'g')) * 3600, sum(int(driver_id = 'D-0427'
    AND event_type = 'phone_handling' AND tt BETWEEN {t0} AND {t0} + 3600)) FROM {STG} GROUP BY 1""").collect()
hb, n427 = {r[0]: r[1] for r in rw}, sum(r[2] for r in rw)
st.update(policies=len(pol), claims=nc, hb_hr=hb, d0427_phone=n427, skew=SK)
st["checks"] = chk = {"risky_hb_3x_safe": hb["risky"] > 3 * hb["safe"], "d0427_disputed_trip": n427 == 7,
    **{f"{n}_{k}_pm_0.5pt": abs(st[n][k] - t) <= .005 for n in ["gps", "events"] for k, t in [("dup", .02), ("late", .03), ("malformed", .005)]}}
open(R + "/gen_stats.json", "w").write(json.dumps(st, indent=1))
spark.sql(f"DROP TABLE {STG}"); assert all(chk.values()), chk
