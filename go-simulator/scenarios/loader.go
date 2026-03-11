package scenarios

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the full scenarios.yaml structure.
type Config struct {
	Simulation  SimulationConfig  `yaml:"simulation"`
	Benchmarks  BenchmarksConfig  `yaml:"benchmarks"`
	Plans       map[string]Plan   `yaml:"plans"`
	Lifecycle   LifecycleConfig   `yaml:"lifecycle"`
	Seasonality SeasonalityConfig `yaml:"seasonality"`
	Geography   GeographyConfig   `yaml:"geography"`
}

type SimulationConfig struct {
	Merchants       int  `yaml:"merchants"`
	HistoryYears    int  `yaml:"history_years"`
	RealtimeEnabled bool `yaml:"realtime_enabled"`
}

type BenchmarksConfig struct {
	MonthlyChurnRate    float64 `yaml:"monthly_churn_rate"`
	FailedPaymentRate   float64 `yaml:"failed_payment_rate"`
	TrialConversionRate float64 `yaml:"trial_conversion_rate"`
	NRRTarget           float64 `yaml:"nrr_target"`
	MRRGrowthMonthly    float64 `yaml:"mrr_growth_monthly"`
	UpgradeRateMonthly  float64 `yaml:"upgrade_rate_monthly"`
	ReactivationRate    float64 `yaml:"reactivation_rate"`
}

type Plan struct {
	MontantCAD float64 `yaml:"montant_cad"`
	Ratio      float64 `yaml:"ratio"`
}

type LifecycleConfig struct {
	Trial          TrialConfig          `yaml:"trial"`
	Churn          ChurnConfig          `yaml:"churn"`
	FailedPayments FailedPaymentsConfig `yaml:"failed_payments"`
	PlanChanges    PlanChangesConfig    `yaml:"plan_changes"`
	Reactivation   ReactivationConfig   `yaml:"reactivation"`
}

type TrialConfig struct {
	Enabled        bool    `yaml:"enabled"`
	DureeJours     int     `yaml:"duree_jours"`
	ConversionRate float64 `yaml:"conversion_rate"`
}

type ChurnConfig struct {
	Voluntary   VoluntaryChurn   `yaml:"voluntary"`
	Involuntary InvoluntaryChurn `yaml:"involuntary"`
}

type VoluntaryChurn struct {
	MonthlyRate float64        `yaml:"monthly_rate"`
	Motifs      []WeightedItem `yaml:"motifs"`
}

type InvoluntaryChurn struct {
	MonthlyRate    float64 `yaml:"monthly_rate"`
	MaxTentatives  int     `yaml:"max_tentatives"`
	DelaiRetryJours []int  `yaml:"delai_retry_jours"`
}

type FailedPaymentsConfig struct {
	Rate     float64            `yaml:"rate"`
	Recovery RecoveryConfig     `yaml:"recovery"`
	Codes    []WeightedCodeItem `yaml:"codes_erreur"`
}

type RecoveryConfig struct {
	Retry1SuccessRate float64 `yaml:"retry_1_success_rate"`
	Retry2SuccessRate float64 `yaml:"retry_2_success_rate"`
}

type PlanChangesConfig struct {
	Upgrade   UpgradeConfig   `yaml:"upgrade"`
	Downgrade DowngradeConfig `yaml:"downgrade"`
}

type UpgradeConfig struct {
	MonthlyRate float64              `yaml:"monthly_rate"`
	Transitions []TransitionItem     `yaml:"transitions"`
}

type DowngradeConfig struct {
	MonthlyRate float64 `yaml:"monthly_rate"`
}

type ReactivationConfig struct {
	Rate             float64 `yaml:"rate"`
	DelaiMedianJours int     `yaml:"delai_median_jours"`
}

type SeasonalityConfig struct {
	Enabled         bool           `yaml:"enabled"`
	SummerSlowdown  SeasonalPeriod `yaml:"summer_slowdown"`
	JanuarySpike    SeasonalPeriod `yaml:"january_spike"`
}

type SeasonalPeriod struct {
	Mois            []int   `yaml:"mois"`
	NewMerchantRate float64 `yaml:"new_merchant_rate"`
}

type GeographyConfig struct {
	Distributions []GeoDistribution `yaml:"distributions"`
}

// Shared types
type WeightedItem struct {
	Motif string  `yaml:"motif"`
	Poids float64 `yaml:"poids"`
}

type WeightedCodeItem struct {
	Code  string  `yaml:"code"`
	Poids float64 `yaml:"poids"`
}

type TransitionItem struct {
	From  string  `yaml:"from"`
	To    string  `yaml:"to"`
	Poids float64 `yaml:"poids"`
}

type GeoDistribution struct {
	Province string  `yaml:"province"`
	Poids    float64 `yaml:"poids"`
}

// Load reads and parses the scenarios.yaml file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	return &cfg, nil
}
