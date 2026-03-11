-- =============================================================================
-- mrr_waterfall.sql — MRR Movement Waterfall
-- New + Expansion - Contraction - Churned = Net MRR change
-- =============================================================================

CREATE OR REPLACE VIEW `lightspeed_analytics.mrr_waterfall`
AS
WITH monthly_new AS (
  SELECT
    DATE_TRUNC(date_debut, MONTH) AS mois,
    SUM(montant_mensuel_cad) AS new_mrr
  FROM `lightspeed_analytics.subscriptions_stream`
  WHERE cdc_operation = 'c' AND statut IN ('active', 'trial')
  GROUP BY 1
),

monthly_churned AS (
  SELECT
    DATE_TRUNC(DATE(updated_at), MONTH) AS mois,
    SUM(montant_mensuel_cad) AS churned_mrr
  FROM `lightspeed_analytics.subscriptions_stream`
  WHERE statut IN ('cancelled', 'churned')
  GROUP BY 1
),

monthly_expansion AS (
  SELECT
    DATE_TRUNC(date_effet, MONTH) AS mois,
    SUM(CASE WHEN nouveau_montant_cad > ancien_montant_cad
         THEN nouveau_montant_cad - ancien_montant_cad ELSE 0 END) AS expansion_mrr,
    SUM(CASE WHEN nouveau_montant_cad < ancien_montant_cad
         THEN ancien_montant_cad - nouveau_montant_cad ELSE 0 END) AS contraction_mrr
  FROM `lightspeed_analytics.plan_changes_stream`
  GROUP BY 1
),

monthly_active AS (
  SELECT
    DATE_TRUNC(DATE(updated_at), MONTH) AS mois,
    SUM(CASE WHEN statut = 'active' THEN montant_mensuel_cad ELSE 0 END) AS mrr_end
  FROM `lightspeed_analytics.subscriptions_stream`
  GROUP BY 1
)

SELECT
  a.mois,
  COALESCE(n.new_mrr, 0) AS new_mrr,
  COALESCE(e.expansion_mrr, 0) AS expansion_mrr,
  COALESCE(e.contraction_mrr, 0) AS contraction_mrr,
  COALESCE(c.churned_mrr, 0) AS churned_mrr,
  a.mrr_end,
  a.mrr_end * 12 AS arr_end
FROM monthly_active a
LEFT JOIN monthly_new n ON a.mois = n.mois
LEFT JOIN monthly_churned c ON a.mois = c.mois
LEFT JOIN monthly_expansion e ON a.mois = e.mois
ORDER BY a.mois;
