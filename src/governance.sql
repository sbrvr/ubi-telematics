-- UC governance: allowlists, mask/row-filter UDFs (attached in the pipeline definition), tags, grants. Idempotent;
-- :principal = identity to enable (job run-as, app SP). Proof: evidence/model_and_governance.md
USE CATALOG surajb;
USE SCHEMA ubi_telematics;
CREATE TABLE IF NOT EXISTS access_allowlist (user_name STRING NOT NULL, role STRING)
  COMMENT 'Who sees exact GPS (groups need an admin)';
CREATE TABLE IF NOT EXISTS underwriter_states (user_name STRING NOT NULL, state STRING NOT NULL)
  COMMENT 'Row filter map: identity -> state (* = all)';
MERGE INTO access_allowlist t USING (SELECT :principal u) s ON t.user_name = s.u WHEN NOT MATCHED THEN INSERT (user_name, role) VALUES (s.u, 'ds');
MERGE INTO underwriter_states t USING (SELECT :principal u) s ON t.user_name = s.u WHEN NOT MATCHED THEN INSERT (user_name, state) VALUES (s.u, '*');
CREATE OR REPLACE FUNCTION gps_exact_ok() RETURN EXISTS (SELECT 1 FROM access_allowlist WHERE user_name = current_user());
CREATE OR REPLACE FUNCTION mask_lat(lat DOUBLE, lon DOUBLE)
  RETURN IF(gps_exact_ok(), lat, st_y(st_geomfromwkt(h3_centeraswkt(h3_longlatash3(lon, lat, 7)))));
CREATE OR REPLACE FUNCTION mask_lon(lon DOUBLE, lat DOUBLE)
  RETURN IF(gps_exact_ok(), lon, st_x(st_geomfromwkt(h3_centeraswkt(h3_longlatash3(lon, lat, 7)))));
CREATE OR REPLACE FUNCTION state_filter(s STRING)  -- not `state`: u.state shadows it
  RETURN EXISTS (SELECT 1 FROM underwriter_states u WHERE u.user_name = current_user() AND u.state IN (s, '*'));
-- Tags: class.location (no name/email), 90-day GPS retention; consumer grants
BEGIN
  FOR r AS (SELECT * FROM VALUES ('bronze_gps'), ('bronze_events'), ('silver_gps_matched'), ('silver_gps_quarantine') v(t)) DO
    EXECUTE IMMEDIATE "ALTER TABLE " || r.t || " SET TAGS ('retention_days' = '90')";
    EXECUTE IMMEDIATE "ALTER TABLE " || r.t || " ALTER COLUMN lat SET TAGS ('class.location')";
    EXECUTE IMMEDIATE "ALTER TABLE " || r.t || " ALTER COLUMN lon SET TAGS ('class.location')";
  END FOR;
  FOR r AS (SELECT * FROM VALUES ('CATALOG surajb'), ('SCHEMA surajb.ubi_telematics') v(t)) DO
    EXECUTE IMMEDIATE 'GRANT USE ' || split(r.t, ' ')[0] || ' ON ' || r.t || ' TO `' || :principal || '`';
  END FOR;
  FOR r AS (SELECT * FROM VALUES ('silver_gps_matched'), ('silver_trips'), ('silver_events'), ('gold_driver_features'),
      ('gold_portfolio_kpis'), ('gold_driver_scores') v(t)) DO
    EXECUTE IMMEDIATE 'GRANT SELECT ON TABLE ' || r.t || ' TO `' || :principal || '`';
  END FOR;
END;
