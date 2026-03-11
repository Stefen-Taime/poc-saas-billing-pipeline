-- =============================================================================
-- lightspeed_db.sql — Schema MySQL pour POC Stripe Lightspeed
-- Binlog ROW activé pour Debezium CDC
-- =============================================================================

CREATE DATABASE IF NOT EXISTS lightspeed_db;
USE lightspeed_db;

-- -----------------------------------------------------------------------------
-- merchants — Compte marchand SaaS
-- -----------------------------------------------------------------------------
CREATE TABLE merchants (
    merchant_id     VARCHAR(20)  NOT NULL PRIMARY KEY,
    nom             VARCHAR(255) NOT NULL,
    email           VARCHAR(255) NOT NULL,
    plan            ENUM('starter', 'pro', 'enterprise', 'enterprise_plus') NOT NULL DEFAULT 'starter',
    statut          ENUM('trial', 'active', 'cancelled', 'churned') NOT NULL DEFAULT 'trial',
    date_inscription DATE        NOT NULL,
    pays            VARCHAR(2)   NOT NULL DEFAULT 'CA',
    province        VARCHAR(10)  NOT NULL DEFAULT 'QC',
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_merchants_statut (statut),
    INDEX idx_merchants_plan (plan),
    INDEX idx_merchants_province (province),
    INDEX idx_merchants_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
-- subscriptions — Abonnement actif
-- -----------------------------------------------------------------------------
CREATE TABLE subscriptions (
    subscription_id         VARCHAR(20)    NOT NULL PRIMARY KEY,
    merchant_id             VARCHAR(20)    NOT NULL,
    plan                    ENUM('starter', 'pro', 'enterprise', 'enterprise_plus') NOT NULL,
    statut                  ENUM('trial', 'active', 'past_due', 'cancelled', 'churned') NOT NULL DEFAULT 'trial',
    montant_mensuel_cad     DECIMAL(10,2)  NOT NULL,
    devise                  VARCHAR(3)     NOT NULL DEFAULT 'CAD',
    date_debut              DATE           NOT NULL,
    date_prochain_paiement  DATE           NULL,
    periode_essai_fin       DATE           NULL,
    annulation_schedulee    BOOLEAN        NOT NULL DEFAULT FALSE,
    updated_at              TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_subs_merchant (merchant_id),
    INDEX idx_subs_statut (statut),
    INDEX idx_subs_plan (plan),
    INDEX idx_subs_paiement (date_prochain_paiement),
    INDEX idx_subs_updated (updated_at),

    CONSTRAINT fk_subs_merchant FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
-- invoices — Factures mensuelles
-- -----------------------------------------------------------------------------
CREATE TABLE invoices (
    invoice_id            VARCHAR(20)    NOT NULL PRIMARY KEY,
    merchant_id           VARCHAR(20)    NOT NULL,
    subscription_id       VARCHAR(20)    NOT NULL,
    montant_cad           DECIMAL(10,2)  NOT NULL,
    statut                ENUM('pending', 'paid', 'failed', 'refunded') NOT NULL DEFAULT 'pending',
    date_emission         DATE           NOT NULL,
    date_echeance         DATE           NOT NULL,
    date_paiement         TIMESTAMP      NULL,
    tentatives_paiement   INT            NOT NULL DEFAULT 0,
    updated_at            TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_inv_merchant (merchant_id),
    INDEX idx_inv_subscription (subscription_id),
    INDEX idx_inv_statut (statut),
    INDEX idx_inv_emission (date_emission),
    INDEX idx_inv_updated (updated_at),

    CONSTRAINT fk_inv_merchant FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id),
    CONSTRAINT fk_inv_subscription FOREIGN KEY (subscription_id) REFERENCES subscriptions(subscription_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
-- payment_attempts — Tentatives de paiement (retries inclus)
-- -----------------------------------------------------------------------------
CREATE TABLE payment_attempts (
    attempt_id        VARCHAR(20)    NOT NULL PRIMARY KEY,
    invoice_id        VARCHAR(20)    NOT NULL,
    merchant_id       VARCHAR(20)    NOT NULL,
    montant_cad       DECIMAL(10,2)  NOT NULL,
    statut            ENUM('succeeded', 'failed') NOT NULL,
    code_erreur       VARCHAR(50)    NULL,
    gateway           VARCHAR(20)    NOT NULL DEFAULT 'stripe',
    tentative_numero  INT            NOT NULL DEFAULT 1,
    created_at        TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_att_invoice (invoice_id),
    INDEX idx_att_merchant (merchant_id),
    INDEX idx_att_statut (statut),
    INDEX idx_att_created (created_at),

    CONSTRAINT fk_att_invoice FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id),
    CONSTRAINT fk_att_merchant FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
-- plan_changes — Upgrades / Downgrades
-- -----------------------------------------------------------------------------
CREATE TABLE plan_changes (
    change_id           VARCHAR(20)    NOT NULL PRIMARY KEY,
    merchant_id         VARCHAR(20)    NOT NULL,
    subscription_id     VARCHAR(20)    NOT NULL,
    ancien_plan         ENUM('starter', 'pro', 'enterprise', 'enterprise_plus') NOT NULL,
    nouveau_plan        ENUM('starter', 'pro', 'enterprise', 'enterprise_plus') NOT NULL,
    ancien_montant_cad  DECIMAL(10,2)  NOT NULL,
    nouveau_montant_cad DECIMAL(10,2)  NOT NULL,
    date_effet          DATE           NOT NULL,
    motif               VARCHAR(50)    NULL,
    updated_at          TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_chg_merchant (merchant_id),
    INDEX idx_chg_subscription (subscription_id),
    INDEX idx_chg_date (date_effet),
    INDEX idx_chg_updated (updated_at),

    CONSTRAINT fk_chg_merchant FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id),
    CONSTRAINT fk_chg_subscription FOREIGN KEY (subscription_id) REFERENCES subscriptions(subscription_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
-- Debezium CDC user — read-only + replication privileges
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbz_cdc_2026';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
