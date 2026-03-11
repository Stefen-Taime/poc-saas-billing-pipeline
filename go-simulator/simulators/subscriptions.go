package simulators

import (
	"database/sql"
	"log"
	"time"
)

// SubscriptionsSimulator processes subscription status updates in real-time.
func SubscriptionsSimulator(db *sql.DB, stop <-chan struct{}) {
	log.Println("[subscriptions] real-time simulator started — ~30 events/hour")

	ticker := time.NewTicker(2 * time.Minute) // ~30 per hour
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			log.Println("[subscriptions] simulator stopped")
			return
		case <-ticker.C:
			// Process trial expirations
			_, err := db.Exec(`
				UPDATE subscriptions
				SET statut = 'churned', updated_at = NOW()
				WHERE statut = 'trial'
				  AND periode_essai_fin < NOW()
				  AND periode_essai_fin IS NOT NULL
				LIMIT 5`)
			if err != nil {
				log.Printf("[subscriptions] trial expiry error: %v", err)
			}

			// Process scheduled cancellations
			_, err = db.Exec(`
				UPDATE subscriptions
				SET statut = 'cancelled', annulation_schedulee = FALSE, updated_at = NOW()
				WHERE annulation_schedulee = TRUE
				  AND date_prochain_paiement <= CURDATE()
				LIMIT 3`)
			if err != nil {
				log.Printf("[subscriptions] cancellation error: %v", err)
			}
		}
	}
}
