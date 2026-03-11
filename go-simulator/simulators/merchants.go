package simulators

import (
	"database/sql"
	"log"
	"time"

	"github.com/poc-stripe-lightspeed/go-simulator/lifecycle"
)

// MerchantsSimulator generates new merchant sign-ups in real-time.
func MerchantsSimulator(db *sql.DB, mgr *lifecycle.Manager, stop <-chan struct{}) {
	log.Println("[merchants] real-time simulator started — ~10 events/hour")

	ticker := time.NewTicker(6 * time.Minute) // ~10 per hour
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			log.Println("[merchants] simulator stopped")
			return
		case <-ticker.C:
			if err := mgr.SeedNewMerchantRealtime(); err != nil {
				log.Printf("[merchants] ERROR: %v", err)
			}
		}
	}
}
