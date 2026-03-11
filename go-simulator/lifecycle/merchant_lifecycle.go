package lifecycle

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/poc-stripe-lightspeed/go-simulator/scenarios"
)

// Manager orchestrates the full merchant lifecycle simulation.
type Manager struct {
	db     *sql.DB
	cfg    *scenarios.Config
	rng    *rand.Rand
	idSeq  int64
}

// NewManager creates a lifecycle manager.
func NewManager(db *sql.DB, cfg *scenarios.Config) *Manager {
	return &Manager{
		db:    db,
		cfg:   cfg,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
		idSeq: 0,
	}
}

// NextID returns an auto-incremented ID with a given prefix.
func (m *Manager) NextID(prefix string) string {
	m.idSeq++
	return fmt.Sprintf("%s-%06d", prefix, m.idSeq)
}

// PickPlan selects a plan based on configured ratios.
func (m *Manager) PickPlan() (string, float64) {
	r := m.rng.Float64()
	cumulative := 0.0
	for name, plan := range m.cfg.Plans {
		cumulative += plan.Ratio
		if r <= cumulative {
			return name, plan.MontantCAD
		}
	}
	// Fallback
	return "starter", 99.0
}

// PickProvince selects a province based on geographic distribution.
func (m *Manager) PickProvince() string {
	r := m.rng.Float64()
	cumulative := 0.0
	for _, geo := range m.cfg.Geography.Distributions {
		cumulative += geo.Poids
		if r <= cumulative {
			return geo.Province
		}
	}
	return "QC"
}

// PickChurnMotif selects a voluntary churn reason.
func (m *Manager) PickChurnMotif() string {
	r := m.rng.Float64()
	cumulative := 0.0
	for _, item := range m.cfg.Lifecycle.Churn.Voluntary.Motifs {
		cumulative += item.Poids
		if r <= cumulative {
			return item.Motif
		}
	}
	return "trop_cher"
}

// PickErrorCode selects a failed payment error code.
func (m *Manager) PickErrorCode() string {
	r := m.rng.Float64()
	cumulative := 0.0
	for _, item := range m.cfg.Lifecycle.FailedPayments.Codes {
		cumulative += item.Poids
		if r <= cumulative {
			return item.Code
		}
	}
	return "insufficient_funds"
}

// SeasonalityFactor returns a multiplier for new merchant rate based on month.
func (m *Manager) SeasonalityFactor(month time.Month) float64 {
	if !m.cfg.Seasonality.Enabled {
		return 1.0
	}
	for _, mo := range m.cfg.Seasonality.SummerSlowdown.Mois {
		if int(month) == mo {
			return m.cfg.Seasonality.SummerSlowdown.NewMerchantRate
		}
	}
	for _, mo := range m.cfg.Seasonality.JanuarySpike.Mois {
		if int(month) == mo {
			return m.cfg.Seasonality.JanuarySpike.NewMerchantRate
		}
	}
	return 1.0
}

// SeedHistory generates the full 4-year historical dataset.
func (m *Manager) SeedHistory() error {
	log.Printf("[lifecycle] seeding %d merchants over %d years of history...",
		m.cfg.Simulation.Merchants, m.cfg.Simulation.HistoryYears)

	startDate := time.Now().AddDate(-m.cfg.Simulation.HistoryYears, 0, 0)
	endDate := time.Now()

	merchantsPerMonth := m.cfg.Simulation.Merchants / (m.cfg.Simulation.HistoryYears * 12)

	current := startDate
	for current.Before(endDate) {
		factor := m.SeasonalityFactor(current.Month())
		count := int(float64(merchantsPerMonth) * factor)

		for i := 0; i < count; i++ {
			if err := m.createMerchantLifecycle(current); err != nil {
				return fmt.Errorf("create merchant at %s: %w", current.Format("2006-01"), err)
			}
		}

		log.Printf("[lifecycle] seeded month %s — %d merchants", current.Format("2006-01"), count)
		current = current.AddDate(0, 1, 0)
	}

	log.Printf("[lifecycle] historical seed complete")
	return nil
}

// createMerchantLifecycle creates a full merchant history from inscription date.
func (m *Manager) createMerchantLifecycle(inscriptionDate time.Time) error {
	merchantID := m.NextID("MCH")
	subID := m.NextID("SUB")
	plan, montant := m.PickPlan()
	province := m.PickProvince()

	// Randomize day within month
	day := m.rng.Intn(28) + 1
	inscDate := time.Date(inscriptionDate.Year(), inscriptionDate.Month(), day, 0, 0, 0, 0, time.UTC)

	// --- INSERT merchant ---
	_, err := m.db.Exec(`
		INSERT INTO merchants (merchant_id, nom, email, plan, statut, date_inscription, pays, province, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, 'CA', ?, ?)`,
		merchantID,
		fmt.Sprintf("Marchand %s", merchantID),
		fmt.Sprintf("%s@example.ca", merchantID),
		plan,
		"trial",
		inscDate.Format("2006-01-02"),
		province,
		inscDate,
	)
	if err != nil {
		return fmt.Errorf("insert merchant: %w", err)
	}

	// --- Trial period ---
	trialEnd := inscDate.AddDate(0, 0, m.cfg.Lifecycle.Trial.DureeJours)

	// --- INSERT subscription (trial) ---
	_, err = m.db.Exec(`
		INSERT INTO subscriptions (subscription_id, merchant_id, plan, statut, montant_mensuel_cad, devise, date_debut, date_prochain_paiement, periode_essai_fin, annulation_schedulee, updated_at)
		VALUES (?, ?, ?, 'trial', ?, 'CAD', ?, ?, ?, FALSE, ?)`,
		subID, merchantID, plan, montant,
		inscDate.Format("2006-01-02"),
		trialEnd.Format("2006-01-02"),
		trialEnd.Format("2006-01-02"),
		inscDate,
	)
	if err != nil {
		return fmt.Errorf("insert subscription: %w", err)
	}

	// --- Trial conversion ---
	if m.rng.Float64() > m.cfg.Lifecycle.Trial.ConversionRate {
		// Did not convert — churned at trial end
		m.updateMerchantStatus(merchantID, "churned", trialEnd)
		m.updateSubscriptionStatus(subID, "churned", trialEnd)
		return nil
	}

	// Converted to active
	m.updateMerchantStatus(merchantID, "active", trialEnd)
	m.updateSubscriptionStatus(subID, "active", trialEnd)

	// --- Generate monthly invoices from trial end to now ---
	currentMonth := trialEnd
	for currentMonth.Before(time.Now()) {
		if err := m.generateMonthlyInvoice(merchantID, subID, montant, currentMonth); err != nil {
			return err
		}

		// --- Monthly lifecycle events ---

		// Voluntary churn check
		if m.rng.Float64() < m.cfg.Lifecycle.Churn.Voluntary.MonthlyRate {
			motif := m.PickChurnMotif()
			m.updateMerchantStatus(merchantID, "churned", currentMonth)
			m.updateSubscriptionStatus(subID, "cancelled", currentMonth)
			_ = motif // Used in plan_changes if needed
			return nil
		}

		// Upgrade check
		if m.rng.Float64() < m.cfg.Lifecycle.PlanChanges.Upgrade.MonthlyRate {
			m.handleUpgrade(merchantID, subID, plan, montant, currentMonth)
		}

		currentMonth = currentMonth.AddDate(0, 1, 0)
	}

	return nil
}

func (m *Manager) generateMonthlyInvoice(merchantID, subID string, montant float64, date time.Time) error {
	invoiceID := m.NextID("INV")

	// Check if payment fails
	failed := m.rng.Float64() < m.cfg.Lifecycle.FailedPayments.Rate

	statut := "paid"
	if failed {
		statut = "failed"
	}

	_, err := m.db.Exec(`
		INSERT INTO invoices (invoice_id, merchant_id, subscription_id, montant_cad, statut, date_emission, date_echeance, date_paiement, tentatives_paiement, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		invoiceID, merchantID, subID, montant, statut,
		date.Format("2006-01-02"),
		date.Format("2006-01-02"),
		date,
		1,
		date,
	)
	if err != nil {
		return fmt.Errorf("insert invoice: %w", err)
	}

	// Payment attempt
	attemptID := m.NextID("ATT")
	attemptStatus := "succeeded"
	var codeErreur *string
	if failed {
		attemptStatus = "failed"
		code := m.PickErrorCode()
		codeErreur = &code
	}

	_, err = m.db.Exec(`
		INSERT INTO payment_attempts (attempt_id, invoice_id, merchant_id, montant_cad, statut, code_erreur, gateway, tentative_numero, created_at)
		VALUES (?, ?, ?, ?, ?, ?, 'stripe', 1, ?)`,
		attemptID, invoiceID, merchantID, montant, attemptStatus, codeErreur, date,
	)
	if err != nil {
		return fmt.Errorf("insert payment_attempt: %w", err)
	}

	// Handle retries for failed payments
	if failed {
		m.handleFailedPaymentRetries(invoiceID, merchantID, montant, date)
	}

	return nil
}

func (m *Manager) handleFailedPaymentRetries(invoiceID, merchantID string, montant float64, baseDate time.Time) {
	delays := m.cfg.Lifecycle.Churn.Involuntary.DelaiRetryJours

	// Retry 1
	if len(delays) > 0 {
		retryDate := baseDate.AddDate(0, 0, delays[0])
		recovered := m.rng.Float64() < m.cfg.Lifecycle.FailedPayments.Recovery.Retry1SuccessRate
		status := "failed"
		if recovered {
			status = "succeeded"
		}

		attemptID := m.NextID("ATT")
		var codeErreur *string
		if !recovered {
			code := m.PickErrorCode()
			codeErreur = &code
		}

		m.db.Exec(`
			INSERT INTO payment_attempts (attempt_id, invoice_id, merchant_id, montant_cad, statut, code_erreur, gateway, tentative_numero, created_at)
			VALUES (?, ?, ?, ?, ?, ?, 'stripe', 2, ?)`,
			attemptID, invoiceID, merchantID, montant, status, codeErreur, retryDate,
		)

		if recovered {
			m.db.Exec(`UPDATE invoices SET statut = 'paid', tentatives_paiement = 2, date_paiement = ?, updated_at = ? WHERE invoice_id = ?`,
				retryDate, retryDate, invoiceID)
			return
		}
	}

	// Retry 2
	if len(delays) > 1 {
		retryDate := baseDate.AddDate(0, 0, delays[0]+delays[1])
		recovered := m.rng.Float64() < m.cfg.Lifecycle.FailedPayments.Recovery.Retry2SuccessRate
		status := "failed"
		if recovered {
			status = "succeeded"
		}

		attemptID := m.NextID("ATT")
		var codeErreur *string
		if !recovered {
			code := m.PickErrorCode()
			codeErreur = &code
		}

		m.db.Exec(`
			INSERT INTO payment_attempts (attempt_id, invoice_id, merchant_id, montant_cad, statut, code_erreur, gateway, tentative_numero, created_at)
			VALUES (?, ?, ?, ?, ?, ?, 'stripe', 3, ?)`,
			attemptID, invoiceID, merchantID, montant, status, codeErreur, retryDate,
		)

		if recovered {
			m.db.Exec(`UPDATE invoices SET statut = 'paid', tentatives_paiement = 3, date_paiement = ?, updated_at = ? WHERE invoice_id = ?`,
				retryDate, retryDate, invoiceID)
			return
		}
	}

	// 3 failures → involuntary churn
	m.db.Exec(`UPDATE invoices SET tentatives_paiement = 3, updated_at = ? WHERE invoice_id = ?`,
		baseDate, invoiceID)
}

func (m *Manager) handleUpgrade(merchantID, subID, currentPlan string, currentMontant float64, date time.Time) {
	for _, t := range m.cfg.Lifecycle.PlanChanges.Upgrade.Transitions {
		if t.From == currentPlan && m.rng.Float64() < t.Poids {
			newMontant := m.cfg.Plans[t.To].MontantCAD
			changeID := m.NextID("CHG")

			m.db.Exec(`
				INSERT INTO plan_changes (change_id, merchant_id, subscription_id, ancien_plan, nouveau_plan, ancien_montant_cad, nouveau_montant_cad, date_effet, motif, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'upgrade_volontaire', ?)`,
				changeID, merchantID, subID, currentPlan, t.To, currentMontant, newMontant, date.Format("2006-01-02"), date,
			)

			m.db.Exec(`UPDATE subscriptions SET plan = ?, montant_mensuel_cad = ?, updated_at = ? WHERE subscription_id = ?`,
				t.To, newMontant, date, subID)
			m.db.Exec(`UPDATE merchants SET plan = ?, updated_at = ? WHERE merchant_id = ?`,
				t.To, date, merchantID)
			return
		}
	}
}

// SeedNewMerchantRealtime creates a single new merchant in real-time mode.
func (m *Manager) SeedNewMerchantRealtime() error {
	return m.createMerchantLifecycle(time.Now())
}

func (m *Manager) updateMerchantStatus(merchantID, status string, date time.Time) {
	m.db.Exec(`UPDATE merchants SET statut = ?, updated_at = ? WHERE merchant_id = ?`,
		status, date, merchantID)
}

func (m *Manager) updateSubscriptionStatus(subID, status string, date time.Time) {
	m.db.Exec(`UPDATE subscriptions SET statut = ?, updated_at = ? WHERE subscription_id = ?`,
		status, date, subID)
}
