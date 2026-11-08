# Databricks notebook source
# XGBoost Poisson risk score (telematics + rating factors) -> filed Poisson GLM relativities -> capped renewal modifier,
# SHAP reason codes, UC model ubi_risk_score@champion, gold_driver_scores (CDF). Proof: evidence/model_and_governance.md
import json, numpy as np, pandas as pd, mlflow, shap, xgboost as xgb
from mlflow import MlflowClient
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import PoissonRegressor as PR
from sklearn.metrics import mean_poisson_deviance as dev
from sklearn.model_selection import KFold
S, V = "surajb.ubi_telematics", "/Volumes/surajb/ubi_telematics/ref"; UCM = S + ".ubi_risk_score"
for k in ["mode", "version", "approver"]: dbutils.widgets.text(k, "")  # train -> @challenger; promote (human) -> @champion; score
mlflow.set_registry_uri("databricks-uc"); cl = MlflowClient()
if dbutils.widgets.get("mode") == "promote":  # job path to @champion (named approver); rollback = promote prior
    v, who, P = dbutils.widgets.get("version"), dbutils.widgets.get("approver"), f"{V}/model_promotion.json"; assert v and who
    try: h = json.load(open(P))
    except OSError: h = []
    h.append(dict(at=str(pd.Timestamp.now("UTC"))[:16], champion=v, previous=cl.get_registered_model(UCM).aliases.get("champion"), approved_by=who))
    cl.set_registered_model_alias(UCM, "champion", v); json.dump(h, open(P, "w")); dbutils.notebook.exit(json.dumps(h[-1]))
TEL = ["hard_brakes_per_100mi", "harsh_corners_per_100mi", "rapid_accel_per_100mi", "pct_night_miles", "speeding_index",
       "phone_handling_per_hr", "total_miles", "pct_highway"]
LAB = dict(zip(TEL, ["Hard braking", "Harsh cornering", "Rapid acceleration", "Night driving", "Speeding", "Phone handling", "Mileage", "Highway share"]))
d = spark.sql(f"""SELECT f.*, age_band, vehicle, territory, exposure_years e, nvl(n, 0) n FROM {S}.gold_driver_features f JOIN {S}.bronze_policies
  USING (driver_id) LEFT JOIN (SELECT driver_id, count(*) n FROM {S}.bronze_claims GROUP BY 1) USING (driver_id)""").toPandas()
oh = lambda c=None: pd.get_dummies(d[["age_band", "vehicle", "territory"]], dtype=float).reindex(columns=c, fill_value=0.0)
y, e = d.n.values.astype(float), d.e.values

def gini(y, p, w):  # exposure-weighted Lorenz Gini of claims ordered by predicted frequency
    o = np.argsort(-p); x, c = np.r_[0, np.cumsum(w[o])] / w.sum(), np.r_[0, np.cumsum(y[o])] / y.sum()
    return float(np.sum((c[1:] + c[:-1]) * np.diff(x)) - 1)

# COMMAND ----------
XP = dict(objective="count:poisson", n_estimators=300, learning_rate=0.03, max_depth=3, min_child_weight=5, subsample=0.8,
          colsample_bytree=0.8, reg_lambda=5, random_state=7)
if dbutils.widgets.get("mode") == "train":
    T = oh(); X = pd.concat([d[TEL], T], axis=1); ob, ot = np.zeros(len(d)), np.zeros(len(d))
    for tr, te in KFold(5, shuffle=True, random_state=7).split(X):  # one row per driver -> driver-grouped folds
        ob[te] = PR(alpha=1e-3, max_iter=2000).fit(T.iloc[tr], y[tr] / e[tr], sample_weight=e[tr]).predict(T.iloc[te]) * e[te]
        ot[te] = xgb.XGBRegressor(**XP).fit(X.iloc[tr], y[tr], base_margin=np.log(e[tr])).predict(X.iloc[te], base_margin=np.log(e[te]))
    gb, gt, rg = gini(y, ob / e, e), gini(y, ot / e, e), np.random.default_rng(7)
    bs = [gini(y[i], ot[i] / e[i], e[i]) - gini(y[i], ob[i] / e[i], e[i]) for i in (rg.integers(0, len(d), len(d)) for _ in range(1000))]
    lo, hi = np.percentile(bs, [2.5, 97.5]); q = pd.qcut(ot / e, 10, labels=False) + 1
    dec = d.assign(q=q, p=ot).groupby("q").agg(drivers=("n", "size"), claims=("n", "sum"), exposure=("e", "sum"), expected=("p", "sum"))
    dec["freq"] = dec.claims / dec.exposure
    # Filed rating plan: Poisson GLM on rating factors + telematics decile (out-of-fold), relativities normalised to mean 1
    R = pd.concat([T, pd.get_dummies(pd.Series(q).astype(str), prefix="tq", dtype=float)], axis=1)
    co = dict(zip(R.columns, PR(alpha=1e-2, max_iter=2000).fit(R, y / e, sample_weight=e).coef_))
    g = np.exp([co.get(f"tq_{k}", 0) for k in range(1, 11)])  # raw GLM relativities; filed plans must be monotone in risk
    rel = IsotonicRegression().fit(np.arange(10), g, sample_weight=dec.exposure.values).predict(np.arange(10)); rel /= np.average(rel[q - 1], weights=e)
    M = dict(gini_base=gb, gini_tel=gt, gini_lift=gt - gb, lift_ci_lo=lo, lift_ci_hi=hi, dev_base=dev(y, ob), dev_tel=dev(y, ot),
             top_bottom=dec.freq.iloc[-1] / dec.freq.iloc[0] if dec.freq.iloc[0] else -1, claims=y.sum(), drivers=len(d))
    m = xgb.XGBRegressor(**XP).fit(X, y, base_margin=np.log(e))
    with mlflow.start_run(run_name="ubi_risk_score"):
        mlflow.log_params(XP); mlflow.log_metrics({k: float(v) for k, v in M.items()})
        art = dict(cols=T.columns.tolist(), cuts=np.quantile(ot / e, np.linspace(.1, .9, 9)).tolist(), rel=rel.tolist(), rel_glm=g.tolist(),
                   deciles=dec.reset_index().to_dict("records"), metrics={k: float(v) for k, v in M.items()})
        mlflow.log_dict(art, "rating.json")
        mi = mlflow.xgboost.log_model(m, name="model", input_example=X.head(3), registered_model_name=UCM)
    cl.set_registered_model_alias(UCM, "challenger", mi.registered_model_version)  # never self-promotes

# COMMAND ----------
# Score consented drivers with the champion; bins + relativities from its run (filed plan)
pl, T = json.load(open(f"{V}/model_promotion.json")), lambda x: pd.Timestamp(x[:16]).timestamp()  # refuse unlogged alias moves
al = spark.sql(f"""SELECT string(event_time) at, request_params.version_num champion FROM system.access.audit WHERE event_date >= '2026-11-01' AND
  action_name = 'setRegisteredModelAlias' AND request_params.full_name_arg = '{UCM}' AND request_params.alias_arg = 'champion'
  AND response.status_code = 200""").collect()
un = [a.at for a in al if T(a.at) >= T(pl[0]["at"]) and not any(p["champion"] == a.champion and abs(T(a.at) - T(p["at"])) <= 120 for p in pl)]
au = dict(alias_changes=len(al), before_gate=sum(T(a.at) < T(pl[0]["at"]) for a in al), unmatched_since_gate=len(un))
json.dump(au, open(f"{V}/alias_audit.json", "w")); assert not un, f"@champion moved outside promote mode at {un}"
mv = cl.get_model_version_by_alias(UCM, "champion"); m = mlflow.xgboost.load_model(f"models:/{UCM}@champion")
art = mlflow.artifacts.load_dict(f"runs:/{mv.run_id}/rating.json"); X = pd.concat([d[TEL], oh(art["cols"])], axis=1)
s = m.predict(X, base_margin=np.zeros(len(d)))  # annual claim frequency
dc = np.searchsorted(art["cuts"], s) + 1; mod = np.clip(np.array(art["rel"])[dc - 1] - 1, -0.30, 0.15)
sv, med = shap.TreeExplainer(m).shap_values(X)[:, :len(TEL)], d[TEL].median()
def rc(i):  # adverse-action style: top-3 telematics factors that raise this driver's score
    return [f"{LAB[t]} {d[t][i] / med[t]:.1f}x peer median" if med[t] else f"{LAB[t]} {d[t][i]:.2f} (peer median 0)"
            for t in (TEL[j] for j in np.argsort(-sv[i])[:3] if sv[i, j] > 0)]
out = pd.DataFrame(dict(driver_id=d.driver_id, score=s.round(5), decile=dc.astype("int32"), premium_modifier=mod.round(4),
                        reason_codes=[rc(i) for i in range(len(d))]))
spark.sql(f"""CREATE TABLE IF NOT EXISTS {S}.gold_driver_scores (driver_id STRING NOT NULL COMMENT 'Driver key',
  score DOUBLE COMMENT 'Predicted annual claim frequency (XGBoost Poisson)', decile INT COMMENT '1-10, 10 = riskiest (filed bins)',
  premium_modifier DOUBLE COMMENT 'Renewal-only modifier from filed GLM relativities, capped -0.30..+0.15',
  reason_codes ARRAY<STRING> COMMENT 'SHAP top-3 adverse telematics factors', model_version STRING COMMENT 'UC model version',
  scored_at TIMESTAMP, CONSTRAINT gold_driver_scores_pk PRIMARY KEY (driver_id))
  COMMENT 'Telematics risk score per consented driver; priced at renewal only' TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
spark.createDataFrame(out).createOrReplaceTempView("new_scores")
spark.sql(f"""MERGE INTO {S}.gold_driver_scores t USING (SELECT *, '{mv.version}' model_version, current_timestamp() scored_at FROM new_scores) u
  ON t.driver_id = u.driver_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * WHEN NOT MATCHED BY SOURCE THEN DELETE""")
r = out[out.driver_id == "D-0427"].iloc[0].to_dict(); r["reason_codes"] = list(r["reason_codes"])
open(f"{V}/model_metrics.json", "w").write(json.dumps(dict(art["metrics"], version=mv.version, deciles=art["deciles"], rel=art["rel"], rel_glm=art["rel_glm"],
    mod_dist=pd.Series(mod).describe().round(4).to_dict(), d0427=r), default=float, indent=1))
assert r["decile"] >= 8 and any(x.startswith("Phone") for x in r["reason_codes"]), r
