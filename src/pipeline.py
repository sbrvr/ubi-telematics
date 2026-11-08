# Databricks notebook source
# Lakeflow SDP: Auto Loader bronze -> silver (dedupe, quarantine, skew fix, H3 + ST_ map-match) -> consented gold.
# Proof: evidence/run_log.md (counts, expectations), evidence/journey.md (D-0427). Gold: pipeline_gold.py
from pyspark import pipelines as dp
from pyspark.sql.functions import broadcast as bc
V, S, sql, mv, rs, rt = "/Volumes/surajb/ubi_telematics", "surajb.ubi_telematics", spark.sql, dp.materialized_view, spark.readStream.table, spark.read.table
GPS = "device_id STRING, driver_id STRING, ts TIMESTAMP, lat DOUBLE, lon DOUBLE, speed_mph DOUBLE"
OK = {"parsed": "device_id IS NOT NULL", "ts_not_null": "ts IS NOT NULL", "in_metro": "lat BETWEEN 29 AND 31.5 AND lon BETWEEN -99 AND -96.5"}
FIX, TP = "timestampadd(SECOND, -coalesce(offset_s, 0), ts)", {"ubi.retention_days": "90"}  # GPS retention; Step 9 cleanup deletes

# COMMAND ----------
# Bronze: raw JSON + _corrupt; drop_ts = arrival hour (drop folder); ~1 day per batch
def al(nm, s):
    r = spark.readStream.format("cloudFiles").options(**{"cloudFiles.format": "json", "cloudFiles.maxFilesPerTrigger": 24, "cloudFiles.inferColumnTypes": True,
        "rescuedDataColumn": "_rescued_data", "columnNameOfCorruptRecord": "_corrupt", "timestampFormat": "yyyy-MM-dd HH:mm:ss"})
    return (r.schema(s + ", _corrupt STRING") if s else r).load(f"{V}/landing/{nm}").selectExpr("*", "_metadata.file_path _ingest_file",
        "current_timestamp() _ingest_ts", "to_timestamp(regexp_extract(_ingest_file, 'drop=([0-9-]+)', 1), 'yyyy-MM-dd-HH') drop_ts")
for nm, s in [("gps", GPS), ("events", GPS + ", event_type STRING, magnitude DOUBLE"), ("policies", ""), ("claims", "")]:
    dp.table(name="bronze_" + nm, comment="raw " + nm, table_properties=TP if s else None)(lambda nm=nm, s=s: al(nm, s))

C = "struct(segment_id, wkb, road_class, maxspeed_mph)"
@mv(comment="OSM segments + UTM WKB + res-10 cover")
def ref_road_segments():
    return sql(f"""SELECT * EXCEPT (g), st_asbinary(g) wkb, h3_coverash3(st_astext(st_transform(st_buffer(g, 40), 4326)), 10) h3_res10
  FROM (SELECT *, st_transform(st_geomfromwkt(wkt, 4326), 32614) g FROM parquet.`{V}/ref/road_segments`)""")
@mv(comment="stage-1 lookup")
def ref_seg_cells():
    return sql(f"SELECT c10, collect_list(c) cands FROM (SELECT explode(h3_res10) c10, {C} c FROM ref_road_segments) GROUP BY 1")
@mv(comment="stage-2 lookup (res-9 ring)")
def ref_seg_ring():
    return sql(f"""SELECT c9, array_distinct(collect_list(c)) ring FROM (SELECT explode(h3_kring(h, 1)) c9, c
  FROM (SELECT explode(h3_res9) h, {C} c FROM ref_road_segments)) GROUP BY 1""")

# Silver: watermark on drop_ts (arrival), not device ts (skewed clocks run ahead)
@mv(comment="per-device clock offset")
def silver_device_skew():
    return sql("""WITH d AS (SELECT device_id, median(timestampdiff(SECOND, drop_ts, ts)) / 60 med_min FROM bronze_gps
  WHERE ts IS NOT NULL GROUP BY 1), f AS (SELECT median(med_min) fleet_min FROM d)
  SELECT *, med_min > fleet_min + 20 skewed, int(if(skewed, med_min - fleet_min, 0) * 60) offset_s FROM d, f""")

@dp.temporary_view()
def gps_dedup():
    return (rs("bronze_gps").withWatermark("drop_ts", "15 minutes").dropDuplicatesWithinWatermark(["device_id", "ts", "_corrupt"])
            .join(rt("silver_device_skew").select("device_id", "offset_s"), "device_id", "left")
            .selectExpr("* EXCEPT (ts, offset_s)", "ts ts_device", "offset_s > 0 skew_corrected", f"{FIX} ts", f"""CASE WHEN _corrupt IS NOT NULL
              THEN 'malformed' WHEN ts IS NULL THEN 'null_ts' WHEN NOT coalesce({OK['in_metro']}, false) THEN 'out_of_range' END reason"""))

@dp.table(comment="rejected GPS rows + reason", table_properties=TP)
def silver_gps_quarantine():
    return rs("gps_dedup").where("reason IS NOT NULL")

near = lambda a: (f"array_min(transform({a}, c -> struct(st_distance(st_transform(st_point(lon, lat, 4326), 32614), "
                  "st_geomfromwkb(c.wkb, 32614)) d, c.segment_id id, c.road_class rc, c.maxspeed_mph mx)))")
M = f"DOUBLE MASK {S}.mask_"  # lat/lon: exact for access_allowlist, else H3 res-7 centroid (governance.sql)
GM = (f"device_id STRING, driver_id STRING, lat {M}lat USING COLUMNS (lon), lon {M}lon USING COLUMNS (lat), speed_mph DOUBLE, _ingest_file STRING, "
      "drop_ts TIMESTAMP, ts_device TIMESTAMP, skew_corrected BOOLEAN, ts TIMESTAMP, segment_id BIGINT, road_class STRING, maxspeed_mph DOUBLE, "
      "snap_m DOUBLE, match_stage INT, n_cand INT")
@dp.table(comment="deduped, skew-fixed, map-matched GPS", schema=GM, table_properties=TP)
@dp.expect_all_or_drop(OK)
def silver_gps_matched():
    p = rs("gps_dedup").selectExpr("*", "if(reason IS NULL, h3_longlatash3(lon, lat, 10), NULL) c10", "h3_toparent(c10, 9) c9")
    return p.join(bc(rt("ref_seg_cells")), "c10", "left").join(bc(rt("ref_seg_ring")), "c9", "left").selectExpr(
        "*", f"if(size(cands) > 0, {near('cands')}, {near('ring')}) b").selectExpr("* EXCEPT (c10, c9, cands, ring, b, reason, _corrupt, _rescued_data, _ingest_ts)",
        "b.id segment_id", "b.rc road_class", "b.mx maxspeed_mph", "round(b.d, 1) snap_m",
        "if(size(cands) > 0, 1, if(size(ring) > 0, 2, 0)) match_stage", "greatest(size(cands), size(ring), 0) n_cand")

@mv(comment="trips: >20 min gap sessions", row_filter=f"ROW FILTER {S}.state_filter ON (state)")
def silver_trips():
    return sql("""SELECT concat(device_id, '-', date_format(min(ts), 'yyyyMMddHHmmss')) trip_id, device_id, driver_id, state, min(ts) start_ts, max(ts) end_ts,
  timestampdiff(SECOND, min(ts), max(ts)) duration_s, sum(speed_mph) / 3600 distance_mi, count(*) points, avg(int(hour(ts) < 4)) night_share,
  avg(int(road_class RLIKE '^(motorway|trunk)')) highway_share, count_if(speed_mph > maxspeed_mph + 10) speeding_seconds
  FROM (SELECT *, count_if(nvl(gap > 1200, true)) OVER w n FROM (SELECT *, timestampdiff(SECOND, lag(ts) OVER w, ts) gap
  FROM silver_gps_matched JOIN (SELECT driver_id, state FROM bronze_policies) USING (driver_id) WINDOW w AS (PARTITION BY device_id ORDER BY ts))
  WINDOW w AS (PARTITION BY device_id ORDER BY ts)) GROUP BY 2, 3, 4, n""")

@mv(comment="deduped, skew-fixed events + trip_id")
@dp.expect_all_or_drop(dict(list(OK.items())[:2]))
def silver_events():
    return sql(f"""SELECT e.* EXCEPT (_corrupt), trip_id FROM (SELECT DISTINCT device_id, driver_id, {FIX} ts, event_type, magnitude, speed_mph, _corrupt FROM bronze_events
  LEFT JOIN silver_device_skew USING (device_id)) e LEFT JOIN silver_trips t ON t.device_id = e.device_id AND e.ts BETWEEN start_ts AND end_ts""")
