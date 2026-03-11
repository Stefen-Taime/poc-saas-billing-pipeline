-- =============================================================================
-- mrr_current.sql — Materialized view: MRR by plan (refreshed every 15 min)
-- Simulates Apache Pinot sub-second OLAP on pre-aggregated data
--
-- NOTE: BigQuery MV restrictions:
--   - No CURRENT_DATE() (non-deterministic) → use DATE_TRUNC(DATE(updated_at))
--   - No scalar transforms on aggregates → ARR computed via query on top of MV
--   - Groups by statut to allow filtering active/churned at query time
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS `lightspeed_analytics.mrr_current`
OPTIONS (
  enable_refresh = true,
  refresh_interval_minutes = 15
)
AS
SELECT
  DATE_TRUNC(DATE(updated_at), MONTH)         AS mois,
  plan,
  statut,
  COUNT(*)                                     AS nb_abonnements,
  SUM(montant_mensuel_cad)                     AS mrr_cad
FROM `lightspeed_analytics.subscriptions_stream`
GROUP BY 1, 2, 3;

-- To query MRR for active subs with ARR:
-- SELECT mois, plan, nb_abonnements, mrr_cad, mrr_cad * 12 AS arr_cad
-- FROM `lightspeed_analytics.mrr_current`
-- WHERE statut = 'active';
