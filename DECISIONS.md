# Decisions and trade-offs
Choices made for the prototype and what changed while building it. Proof: evidence/run_log.md, evidence/lakebase_genie_app.md

| Decision | Chosen | Alternative | Why |
|---|---|---|---|
| Pipeline | Lakeflow SDP + Auto Loader | hand-written Structured Streaming | expectations, lineage, retries, less code |
| Data | synthetic on the real OSM road network | public telematics data | few 1 Hz sets have claim labels; real data is PII; known truth validates lift |
| Dedupe watermark | arrival time (drop folder) | device `ts` | skewed clocks run up to 2 h ahead and would drop other drivers' rows |
| Map-matching | H3 res-10 cell + res-9 ring, ST_Distance in UTM | HMM (Valhalla/OSRM) | fits the budget, fine at 5-15 m GPS noise; weak at dense intersections |
| Model | XGBoost -> filed Poisson GLM, non-decreasing relativities | XGBoost straight to price | explainable for regulators and adverse-action notices |
| Model promotion | train -> @challenger; named approver -> @champion; rollback = promote prior | train sets champion | no unapproved model prices |
| Pricing cadence | renewal only, capped -30%..+15% | mid-term dynamic | filed rates, no premium shocks |
| Scoring | Spark batch; event scoring endpoint as the production path (not built) | everything online | cheaper at book scale; no KPI needs per-event scores |
| Operational store | Lakebase Autoscaling (scale to zero) | SQL warehouse / external Postgres | ms point reads + transactional writes, governed by UC |
| Sync | scores TRIGGERED from CDF; features SNAPSHOT | continuous | scores change once per run |
| Row-filtered features | sync an unfiltered gold copy; the app enforces states | sync the filtered table | UC cannot sync a table with row/column security |
| GenAI | drafts only; a human decides; four-eyes override | LLM decides | accountability; output varies run to run (logged) |
| Natural language | Genie + trusted SQL + native benchmark (12/12) | custom text-to-SQL | governed, measurable |
| Genie over live Lakebase | UC database catalog | Delta mirror | pre-check passed, no copy to keep fresh |
| System of record | policy admin system (stub) | Lakebase as ledger | approvals queue a renewal adjustment |
| Trips | gap-based sessionization as an MV | streaming session windows | simpler, re-computable |
| App | Streamlit, SQL on behalf of the user | FastAPI + htmx (built first) | UC masks/filters per user; smaller code; lost JSON API |
| Masking | allowlist table | workspace groups | creating groups needs an admin |
| OSM roads | cached once in the Volume | download per run | a public-API outage broke a run; runs now reproducible |

## Changed during the build
- The watermark moved from `ts` to arrival time after a skew analysis; a day-30 regression check proves no row loss.
- Raw GLM relativities were non-monotone (few claims), so an isotonic fit was added (filed plans must be monotone).
- The Genie benchmark uses the native eval API, so Genie's answers are judged against the expected SQL.
