package simulators

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// PaymentAttemptsSimulator handles retries for failed invoices.
func PaymentAttemptsSimulator(db *sql.DB, stop <-chan struct{}) {
	log.Println("[payment_attempts] real-time simulator started — ~220 events/hour")

	ticker := time.NewTicker(16 * time.Second) // ~220 per hour
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	seq := int64(800000)

	errorCodes := []string{"insufficient_funds", "card_expired", "do_not_honor", "lost_card"}
	errorWeights := []float64{0.40, 0.25, 0.20, 0.15}

	pickError := func() string {
		r := rng.Float64()
		cum := 0.0
		for i, w := range errorWeights {
			cum += w
			if r <= cum {
				return errorCodes[i]
			}
		}
		return errorCodes[0]
	}

	for {
		select {
		case <-stop:
			log.Println("[payment_attempts] simulator stopped")
			return
		case <-ticker.C:
			// Find failed invoices eligible for retry (< 3 attempts, failed > 3 days ago)
			rows, err := db.Query(`
				SELECT i.invoice_id, i.merchant_id, i.montant_cad, i.tentatives_paiement
				FROM invoices i
				WHERE i.statut = 'failed'
				  AND i.tentatives_paiement < 3
				  AND i.updated_at < DATE_SUB(NOW(), INTERVAL 3 DAY)
				LIMIT 3`)
			if err != nil {
				log.Printf("[payment_attempts] query error: %v", err)
				continue
			}

			for rows.Next() {
				var invoiceID, merchantID string
				var montant float64
				var attempts int
				if err := rows.Scan(&invoiceID, &merchantID, &montant, &attempts); err != nil {
					continue
				}

				seq++
				attemptID := fmt.Sprintf("ATT-%06d", seq)
				now := time.Now()
				newAttempt := attempts + 1

				// Recovery rate decreases with each attempt
				recoveryRate := 0.70
				if newAttempt == 3 {
					recoveryRate = 0.50
				}

				succeeded := rng.Float64() < recoveryRate
				status := "failed"
				var codeErreur *string
				if succeeded {
					status = "succeeded"
				} else {
					code := pickError()
					codeErreur = &code
				}

				_, err := db.Exec(`
					INSERT INTO payment_attempts (attempt_id, invoice_id, merchant_id, montant_cad, statut, code_erreur, gateway, tentative_numero, created_at)
					VALUES (?, ?, ?, ?, ?, ?, 'stripe', ?, ?)`,
					attemptID, invoiceID, merchantID, montant, status, codeErreur, newAttempt, now)
				if err != nil {
					log.Printf("[payment_attempts] insert error: %v", err)
					continue
				}

				if succeeded {
					db.Exec(`UPDATE invoices SET statut = 'paid', tentatives_paiement = ?, date_paiement = ?, updated_at = ? WHERE invoice_id = ?`,
						newAttempt, now, now, invoiceID)
				} else {
					db.Exec(`UPDATE invoices SET tentatives_paiement = ?, updated_at = ? WHERE invoice_id = ?`,
						newAttempt, now, invoiceID)

					// 3rd failure → involuntary churn
					if newAttempt >= 3 {
						db.Exec(`UPDATE subscriptions SET statut = 'churned', updated_at = ? WHERE merchant_id = ? AND statut = 'active'`,
							now, merchantID)
						db.Exec(`UPDATE merchants SET statut = 'churned', updated_at = ? WHERE merchant_id = ?`,
							now, merchantID)
						log.Printf("[payment_attempts] involuntary churn: %s (3 failed payments)", merchantID)
					}
				}
			}
			rows.Close()
		}
	}
}
