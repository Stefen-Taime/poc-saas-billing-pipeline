-- =============================================================================
-- failed_payment_rate.sql — Failed payment rate with error breakdown
-- Tracks payment failures, recovery rate, and revenue at risk
-- =============================================================================

CREATE OR REPLACE VIEW `lightspeed_analytics.failed_payment_rate`
AS
SELECT
  DATE_TRUNC(DATE(created_at), MONTH) AS mois,
  COUNT(*) AS total_attempts,
  COUNTIF(statut = 'success') AS nb_success,
  COUNTIF(statut = 'failed') AS nb_failed,
  SAFE_DIVIDE(COUNTIF(statut = 'failed'), COUNT(*)) AS failed_rate,
  -- Breakdown by error code
  COUNTIF(code_erreur = 'insufficient_funds') AS err_insufficient_funds,
  COUNTIF(code_erreur = 'card_expired') AS err_card_expired,
  COUNTIF(code_erreur = 'do_not_honor') AS err_do_not_honor,
  COUNTIF(code_erreur = 'lost_card') AS err_lost_card,
  -- Recovery rate (retry success after initial failure)
  SAFE_DIVIDE(
    COUNTIF(statut = 'success' AND tentative_numero > 1),
    COUNTIF(statut = 'failed')
  ) AS recovery_rate,
  -- Revenue at risk from failed payments
  SUM(CASE WHEN statut = 'failed' THEN montant_cad ELSE 0 END) AS revenue_at_risk_cad
FROM `lightspeed_analytics.payment_attempts_stream`
GROUP BY 1
ORDER BY 1;
