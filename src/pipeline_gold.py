# Databricks notebook source
# Gold MVs (2nd pipeline library): consented 30-day driver features (state row filter) + portfolio KPIs; comments feed
# Genie. Proof: evidence/model_and_governance.md (row filter), evidence/run_log.md (counts)
from pyspark import pipelines as dp
S, sql = "surajb.ubi_telematics", spark.sql
c = lambda *cols: ", ".join(f"{n} {t} COMMENT '{d}'" for n, t, d in cols)
EV = [("hb", "hard_brakes", "hard-brake"), ("hc", "harsh_corners", "harsh-corner"), ("ra", "rapid_accel", "rapid-acceleration")]
FEAT = c(("driver_id", "STRING", "Pseudonymous driver key; joins policies and scores"), ("state", "STRING", "Policy state; row filter"),
    *[(f"{n}_per_100mi", "DOUBLE", f"Device {d} events per 100 miles") for _, n, d in EV],
    ("pct_night_miles", "DOUBLE", "Share of miles 00:00-04:00"), ("speeding_index", "DOUBLE", "% of driving time >10 mph over limit"),
    ("phone_handling_per_hr", "DOUBLE", "Phone-handling events per hour"), ("total_miles", "DOUBLE", "Miles in the 30-day window"),
    ("pct_highway", "DOUBLE", "Share of miles on motorway/trunk"), ("as_of_date", "DATE", "Last trip date in window"))

@dp.materialized_view(comment="Per-driver 30-day telematics features, consented drivers only; PK driver_id; no location/PII",
                      schema=FEAT, row_filter=f"ROW FILTER {S}.state_filter ON (state)")
def gold_driver_features():
    r = ", ".join(f"round(nvl({a}, 0) * 100 / mi, 3) {n}_per_100mi" for a, n, _ in EV)
    return sql(f"""WITH t AS (SELECT * FROM silver_trips WHERE start_ts > (SELECT max(end_ts) - INTERVAL 30 DAYS FROM silver_trips)),
  a AS (SELECT driver_id, sum(distance_mi) mi, sum(duration_s) / 3600 h, sum(distance_mi * night_share) nmi, sum(distance_mi * highway_share) hmi,
    sum(speeding_seconds) sp, to_date(max(end_ts)) as_of_date FROM t GROUP BY 1),
  e AS (SELECT * FROM (SELECT driver_id, event_type FROM silver_events WHERE trip_id IN (SELECT trip_id FROM t))
    PIVOT (count(*) FOR event_type IN ('hard_brake' hb, 'harsh_corner' hc, 'rapid_accel' ra, 'phone_handling' ph)))
  SELECT driver_id, state, {r}, round(nmi / mi, 4) pct_night_miles, round(sp / h / 36, 3) speeding_index,
    round(nvl(ph, 0) / h, 3) phone_handling_per_hr, round(mi, 1) total_miles, round(hmi / mi, 4) pct_highway, as_of_date
  FROM a JOIN bronze_policies USING (driver_id) LEFT JOIN e USING (driver_id) WHERE consent""")

KPI = c(("state", "STRING", "Policy state; ALL = whole book"), ("policies", "BIGINT", "Policies in force"),
    ("ubi_enrolled", "BIGINT", "Telematics-consented policies"), ("earned_premium", "DOUBLE", "Premium x exposure years, USD"),
    ("claims", "BIGINT", "Claim count"), ("incurred", "DOUBLE", "Incurred losses, USD"), ("loss_ratio", "DOUBLE", "Incurred / earned premium"),
    ("claim_freq", "DOUBLE", "Claims per exposure year"))
@dp.materialized_view(comment="Portfolio KPIs by state (ALL = total)", schema=KPI)
def gold_portfolio_kpis():
    return sql("""SELECT nvl(state, 'ALL') state, count(*) policies, count_if(consent) ubi_enrolled,
  sum(base_premium * exposure_years) earned_premium, sum(nvl(n, 0)) claims, sum(nvl(inc, 0)) incurred,
  incurred / earned_premium loss_ratio, claims / sum(exposure_years) claim_freq
  FROM bronze_policies LEFT JOIN (SELECT policy_id, count(*) n, sum(incurred) inc FROM bronze_claims GROUP BY 1) USING (policy_id) GROUP BY ROLLUP (state)""")

# UC cannot sync a row-filtered table: Lakebase syncs this copy (not granted; the app enforces states)
@dp.materialized_view(comment="Unfiltered copy: Lakebase sync source only")
def gold_driver_features_sync():
    return spark.read.table("gold_driver_features")
