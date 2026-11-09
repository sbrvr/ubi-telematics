# AI as a teammate
How the build used AI, written from the agent's running log. Proof that the result ran: evidence/run_log.md

## Tools
- **Claude Code agent (Opus 5.5)** wrote and ran all code through the Databricks CLI/SDK, read SDK source and `--help` before
  each new API, and kept a step log. One step per turn, each ending in a report: done / evidence / verified-by / budget.
- **Genie** answers business questions; its **native benchmark eval** grades its answers against our expected SQL (12/12).
- **Foundation Model endpoint** (`databricks-claude-fable-5-1`) drafts dispute summaries and customer letters; a human decides.
- **Browser automation** clicked through the live app as the signed-in user.

## Key prompts (trimmed)
1. Part A context once, then "Do Step n": role, rules (verify APIs, never DBFS, smallest compute, budget gate), domain facts.
2. "Do Step 2, test before build" made pre-checks the default: throwaway pipelines proved Auto Loader file order, row
   filters in SDP, and malformed-JSON handling before any real code.
3. "Did we lose any functionality after code reduction?" surfaced dropped fields; the human chose what to restore.
4. "Which other steps can have file length issues?" led to a repo-wide budget rebalance and file splits before overruns.
5. "I want to build the app with Streamlit" made a mid-build framework switch with a written plan and re-verification.

## Patterns that worked
- **Plan first, then pre-check:** each risky API (Lakebase, Genie, synced tables, Apps auth) was probed on throwaway objects.
- **Invariants in code:** the generator asserts its own defect rates; the pipeline reconciles to them exactly.
- **Generated evidence:** evidence/*.md is written by the job, never typed by hand.
- **Budget gate + allocation table:** ~85k-char scorer limit checked before every commit; measure before running.

## What humans decided or corrected
- Scope: Lakebase Autoscaling over provisioned; skip the serving endpoint; Streamlit over FastAPI; raise the generator cap.
- Design: a guide review after Step 1 moved the dedupe watermark from device time to arrival time.
- Quality: restore the realistic age/vehicle mix and event magnitudes after a size trim.

## Bugs the agent caught and fixed
- Row-filter UDF parameter shadowed by a column (every state passed); found by its own proof query.
- `SELECT DISTINCT` would have collapsed all malformed rows; WKB lost its SRID; Overpass 502 led to an OSM cache.
- `psycopg[binary]` aborts on serverless; LLM content arrived as content blocks; a number box stored a wrong value.
- Own mistakes, cleaned up: objects created in the wrong schema, a duplicate throwaway Genie space.

## Effort (estimate, not measured)
- **With AI:** ~18 h to a working build (2026-11-06/07), then mock-review hardening to 11-09; incl. job runs.
- **Without AI:** roughly 2-3 weeks (80-120 h) for a senior Databricks engineer, mostly API discovery and debugging.
