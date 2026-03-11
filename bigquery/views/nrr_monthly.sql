-- =============================================================================
-- nrr_monthly.sql — Net Revenue Retention (NRR) monthly
-- NRR = (MRR_end - churned + expansion) / MRR_start
--
-- NOTE: Regular VIEW (not materialized) — BigQuery MVs do not support
-- CTEs, JOINs, LAG(), or SAFE_DIVIDE required by NRR calculation.
-- =============================================================================

CREATE OR REPLACE VIEW `lightspeed_analytics.nrr_monthly`
AS
WITH monthly_mrr AS (
  SELECT
    DATE_TRUNC(DATE(updated_at), MONTH) AS mois,
    SUM(CASE WHEN statut = 'active' THEN montant_mensuel_cad ELSE 0 END) AS mrr_active
  FROM `lightspeed_analytics.subscriptions_stream`
  GROUP BY 1
),

plan_changes_agg AS (
  SELECT
    DATE_TRUNC(date_effet, MONTH) AS mois,
    SUM(CASE WHEN nouveau_montant_cad > ancien_montant_cad
         THEN nouveau_montant_cad - ancien_montant_cad ELSE 0 END) AS expansion_mrr,
    SUM(CASE WHEN nouveau_montant_cad < ancien_montant_cad
         THEN ancien_montant_cad - nouveau_montant_cad ELSE 0 END) AS contraction_mrr
  FROM `lightspeed_analytics.plan_changes_stream`
  GROUP BY 1
),

churned_mrr AS (
  SELECT
    DATE_TRUNC(DATE(updated_at), MONTH) AS mois,
    SUM(montant_mensuel_cad) AS churned_mrr
  FROM `lightspeed_analytics.subscriptions_stream`
  WHERE statut IN ('cancelled', 'churned')
  GROUP BY 1
)

SELECT
  m.mois,
  m.mrr_active                                           AS mrr_end,
  COALESCE(c.churned_mrr, 0)                             AS churned_mrr,
  COALESCE(p.expansion_mrr, 0)                            AS expansion_mrr,
  COALESCE(p.contraction_mrr, 0)                          AS contraction_mrr,

  -- NRR approximation
  SAFE_DIVIDE(
    m.mrr_active + COALESCE(c.churned_mrr, 0) - COALESCE(p.expansion_mrr, 0),
    LAG(m.mrr_active) OVER (ORDER BY m.mois)
  ) AS nrr

FROM monthly_mrr m
LEFT JOIN plan_changes_agg p ON m.mois = p.mois
LEFT JOIN churned_mrr c ON m.mois = c.mois
ORDER BY m.mois;
