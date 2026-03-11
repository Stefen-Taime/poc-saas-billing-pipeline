package simulators

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// InvoicesSimulator generates monthly invoices for active subscriptions.
func InvoicesSimulator(db *sql.DB, stop <-chan struct{}) {
	log.Println("[invoices] real-time simulator started — ~200 events/hour")

	ticker := time.NewTicker(18 * time.Second) // ~200 per hour
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	seq := int64(900000)

	for {
		select {
		case <-stop:
			log.Println("[invoices] simulator stopped")
			return
		case <-ticker.C:
			// Find subscriptions due for billing
			rows, err := db.Query(`
				SELECT subscription_id, merchant_id, montant_mensuel_cad
				FROM subscriptions
				WHERE statut = 'active'
				  AND date_prochain_paiement <= CURDATE()
				LIMIT 5`)
			if err != nil {
				log.Printf("[invoices] query error: %v", err)
				continue
			}

			for rows.Next() {
				var subID, merchantID string
				var montant float64
				if err := rows.Scan(&subID, &merchantID, &montant); err != nil {
					continue
				}

				seq++
				invoiceID := fmt.Sprintf("INV-%06d", seq)
				now := time.Now()

				statut := "paid"
				if rng.Float64() < 0.06 { // 6% failure rate
					statut = "failed"
				}

				_, err := db.Exec(`
					INSERT INTO invoices (invoice_id, merchant_id, subscription_id, montant_cad, statut, date_emission, date_echeance, date_paiement, tentatives_paiement, updated_at)
					VALUES (?, ?, ?, ?, ?, CURDATE(), CURDATE(), ?, 1, ?)`,
					invoiceID, merchantID, subID, montant, statut, now, now)
				if err != nil {
					log.Printf("[invoices] insert error: %v", err)
					continue
				}

				// Advance next payment date
				db.Exec(`UPDATE subscriptions SET date_prochain_paiement = DATE_ADD(date_prochain_paiement, INTERVAL 1 MONTH), updated_at = ? WHERE subscription_id = ?`,
					now, subID)
			}
			rows.Close()
		}
	}
}
