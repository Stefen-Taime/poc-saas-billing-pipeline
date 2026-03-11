package db

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Config holds MySQL connection parameters.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// Connect establishes and returns a MySQL connection pool.
func Connect(cfg Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=UTC",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connectivity with retries
	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			log.Printf("[mysql] connected to %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)
			return db, nil
		}
		log.Printf("[mysql] waiting for connection (attempt %d/30)...", i+1)
		time.Sleep(2 * time.Second)
	}

	return nil, fmt.Errorf("could not connect to MySQL after 30 attempts")
}
