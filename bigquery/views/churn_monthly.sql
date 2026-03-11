-- =============================================================================
-- churn_monthly.sql — Materialized view: Monthly churn by statut
-- Voluntary (cancelled) + Involuntary (churned from failed payments)
--
-- NOTE: BigQuery MV restrictions:
--   - No COUNTIF / SAFE_DIVIDE → group by statut, compute rates at query time
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS `lightspeed_analytics.churn_monthly`
OPTIONS (
  enable_refresh = true,
  refresh_interval_minutes = 15
)
AS
SELECT
  DATE_TRUNC(DATE(updated_at), MONTH) AS mois,
  statut,
  COUNT(*) AS nb_subscriptions,
  SUM(montant_mensuel_cad) AS mrr_cad
FROM `lightspeed_analytics.subscriptions_stream`
GROUP BY 1, 2;

-- To compute churn rate from this MV:
-- SELECT mois,
--   SUM(CASE WHEN statut IN ('cancelled','churned') THEN nb_subscriptions ELSE 0 END) AS nb_churned,
--   SUM(nb_subscriptions) AS nb_total,
--   SAFE_DIVIDE(
--     SUM(CASE WHEN statut IN ('cancelled','churned') THEN nb_subscriptions ELSE 0 END),
--     SUM(nb_subscriptions)
--   ) AS churn_rate
-- FROM `lightspeed_analytics.churn_monthly`
-- GROUP BY 1;
