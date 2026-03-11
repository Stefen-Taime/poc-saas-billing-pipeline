package simulators

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// PlanChangesSimulator handles upgrades and downgrades in real-time.
func PlanChangesSimulator(db *sql.DB, stop <-chan struct{}) {
	log.Println("[plan_changes] real-time simulator started — ~5 events/hour")

	ticker := time.NewTicker(12 * time.Minute) // ~5 per hour
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	seq := int64(700000)

	planMontants := map[string]float64{
		"starter":         99.0,
		"pro":             299.0,
		"enterprise":      799.0,
		"enterprise_plus": 1499.0,
	}

	upgrades := []struct {
		from, to string
		poids    float64
	}{
		{"starter", "pro", 0.60},
		{"pro", "enterprise", 0.30},
		{"enterprise", "enterprise_plus", 0.10},
	}

	for {
		select {
		case <-stop:
			log.Println("[plan_changes] simulator stopped")
			return
		case <-ticker.C:
			// Pick a random active subscription for upgrade
			var subID, merchantID, currentPlan string
			var currentMontant float64
			err := db.QueryRow(`
				SELECT subscription_id, merchant_id, plan, montant_mensuel_cad
				FROM subscriptions
				WHERE statut = 'active'
				ORDER BY RAND()
				LIMIT 1`).Scan(&subID, &merchantID, &currentPlan, &currentMontant)
			if err != nil {
				continue
			}

			// Decide upgrade based on weights
			for _, u := range upgrades {
				if u.from == currentPlan && rng.Float64() < u.poids {
					seq++
					changeID := fmt.Sprintf("CHG-%06d", seq)
					newMontant := planMontants[u.to]
					now := time.Now()

					_, err := db.Exec(`
						INSERT INTO plan_changes (change_id, merchant_id, subscription_id, ancien_plan, nouveau_plan, ancien_montant_cad, nouveau_montant_cad, date_effet, motif, updated_at)
						VALUES (?, ?, ?, ?, ?, ?, ?, CURDATE(), 'upgrade_volontaire', ?)`,
						changeID, merchantID, subID, currentPlan, u.to, currentMontant, newMontant, now)
					if err != nil {
						log.Printf("[plan_changes] insert error: %v", err)
						break
					}

					db.Exec(`UPDATE subscriptions SET plan = ?, montant_mensuel_cad = ?, updated_at = ? WHERE subscription_id = ?`,
						u.to, newMontant, now, subID)
					db.Exec(`UPDATE merchants SET plan = ?, updated_at = ? WHERE merchant_id = ?`,
						u.to, now, merchantID)

					log.Printf("[plan_changes] upgrade: %s %s → %s ($%.0f → $%.0f)",
						merchantID, currentPlan, u.to, currentMontant, newMontant)
					break
				}
			}
		}
	}
}
