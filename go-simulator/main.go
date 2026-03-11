package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/poc-stripe-lightspeed/go-simulator/db"
	"github.com/poc-stripe-lightspeed/go-simulator/lifecycle"
	"github.com/poc-stripe-lightspeed/go-simulator/scenarios"
	"github.com/poc-stripe-lightspeed/go-simulator/simulators"
)

func main() {
	// --- Flags ---
	mysqlHost := flag.String("mysql-host", "127.0.0.1", "MySQL host (GKE LoadBalancer IP)")
	mysqlPort := flag.Int("mysql-port", 3306, "MySQL port")
	mysqlUser := flag.String("mysql-user", "root", "MySQL user")
	mysqlPass := flag.String("mysql-password", "", "MySQL password (use env MYSQL_PASSWORD if empty)")
	mysqlDB := flag.String("mysql-database", "lightspeed_db", "MySQL database")
	configPath := flag.String("config", "config/scenarios.yaml", "Path to scenarios.yaml")
	seedOnly := flag.Bool("seed-only", false, "Only seed historical data, then exit")
	skipSeed := flag.Bool("skip-seed", false, "Skip historical seed, go straight to real-time")
	flag.Parse()

	// Password from flag or env
	password := *mysqlPass
	if password == "" {
		password = os.Getenv("MYSQL_PASSWORD")
	}
	if password == "" {
		log.Fatal("MySQL password required: use --mysql-password or MYSQL_PASSWORD env var")
	}

	// --- Load config ---
	cfg, err := scenarios.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	log.Printf("[main] config loaded: %d merchants, %d years history",
		cfg.Simulation.Merchants, cfg.Simulation.HistoryYears)

	// --- Connect MySQL ---
	conn, err := db.Connect(db.Config{
		Host:     *mysqlHost,
		Port:     *mysqlPort,
		User:     *mysqlUser,
		Password: password,
		Database: *mysqlDB,
	})
	if err != nil {
		log.Fatalf("failed to connect MySQL: %v", err)
	}
	defer conn.Close()

	// --- Lifecycle manager ---
	mgr := lifecycle.NewManager(conn, cfg)

	// --- Phase 1: Historical seed ---
	if !*skipSeed {
		log.Println("[main] === Phase 1: Seeding historical data ===")
		if err := mgr.SeedHistory(); err != nil {
			log.Fatalf("seed failed: %v", err)
		}
		log.Println("[main] === Historical seed complete ===")
	}

	if *seedOnly {
		log.Println("[main] seed-only mode — exiting")
		return
	}

	if !cfg.Simulation.RealtimeEnabled {
		log.Println("[main] real-time disabled in config — exiting")
		return
	}

	// --- Phase 2: Real-time simulation (goroutines) ---
	log.Println("[main] === Phase 2: Starting real-time simulators ===")

	stop := make(chan struct{})

	go simulators.MerchantsSimulator(conn, mgr, stop)
	go simulators.SubscriptionsSimulator(conn, stop)
	go simulators.InvoicesSimulator(conn, stop)
	go simulators.PaymentAttemptsSimulator(conn, stop)
	go simulators.PlanChangesSimulator(conn, stop)

	log.Println("[main] all 5 goroutines running — press Ctrl+C to stop")

	// --- Graceful shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[main] shutdown signal received — stopping simulators...")
	close(stop)
	log.Println("[main] done")
}
