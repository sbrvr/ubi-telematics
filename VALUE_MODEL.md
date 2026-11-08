# Value model
Proof: platform cost from system.billing.usage + evidence/run_log.md task times. Illustrative assumptions for a fictional carrier (Meridian Mutual, ~$600M written premium, 400k policies, 60k UBI vehicles).
Industry benchmark, not our result: Insurance Outcome Map, UBI = 3-6 pp market-share improvement.

| Lever (Outcome Map KPI) | Assumption | Low | Base | High |
|---|---|---|---|---|
| Loss ratio | $90M UBI premium (60k x $1,500); UBI loss ratio -1 / -2 / -3 pts | $0.90M | $1.80M | $2.70M |
| Retention rate | 30k preferred drivers, +1 / +2 / +3 pts x $1,400, 10% margin | $0.04M | $0.08M | $0.13M |
| Processing time | 2,400 disputes/yr, 2.5 h -> 0.5 h at $60/h (half / full / 1.5x) | $0.14M | $0.29M | $0.43M |
| Processing time | blanket concessions 60% -> 30% of disputes at $150 | $0.05M | $0.11M | $0.16M |
| Market penetration | enrollment 15% -> 16.5 / 18 / 19.5%; half new to carrier; $1,500; 5% margin | $0.23M | $0.45M | $0.68M |
| **Profit impact / yr** | | **$1.36M** | **$2.73M** | **$4.10M** |
| New earned premium / yr | 3k / 6k / 9k new policies x $1,500 | $4.5M | $9.0M | $13.5M |

Not included (upside): retiring the vendor score fee (~$2/vehicle/month = up to $1.44M/yr).

## Platform cost: measured, then scaled
Measured with `system.billing.usage` x `system.billing.list_prices` (list price, serverless) for this build's pipeline, job,
warehouse, Lakebase project, synced tables and app (billing through 2026-11-07 14:45 UTC). Build total so far: **$20.64**.

| Component | Measured on the prototype (1k vehicles) | Scaled to 60k vehicles / yr |
|---|---|---|
| Lakeflow pipeline | full refresh of 53M GPS points = 2.27 DBU (~$1.02); +1.75M-point day = 1.07 DBU | ~107M points/day = ~2x that refresh: ~4.6 DBU/day -> **~$0.8k** |
| Scoring + sync + checks | rescore 73 s, syncs 34-54 s per run (evidence/run_log.md) | daily: **~$1k** |
| SQL warehouse (app, Genie) | 15.7 DBU over the build ($11.01) | 2X-Small, ~10 h/business day, 4 DBU/h at $0.70: **~$7k** |
| Lakebase Autoscaling (0.5-2 CU, scale to zero) | 0.18 DBU over ~2.7 h active ($0.09); synced-table pipelines 0.14 DBU ($0.06) | 60k rows, awake ~10 h/business day, up to 4x the load: **~$0.3k** |
| App + LLM drafts | app: Medium = 0.5 DBU/h x $0.95 (rate billed to apps here; ours not yet billed); LLM: $0.16 for <= $0.05/draft | app ~10 h/business day **~$1.2k** (always on: $4.2k); 2,400 drafts **~$0.1k** |
| **Platform total** | | **~$10k/yr** |

**Net base case: ~$2.7M/yr profit impact.** Platform cost (~$10k) is under 1% of the benefit, so the decision rests on the levers above.

**Sensitivity driver: loss-ratio points.** Each UBI loss-ratio point is $0.9M/yr, a third of the base case. If the pilot's parallel
run against the vendor score shows < 1 pt, the case falls to the low column. The lift in evidence/model_and_governance.md is synthetic, and one draw: 20 claim re-draws give median +0.105.
