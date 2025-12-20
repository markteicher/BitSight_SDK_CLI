/* ============================================================
   BitSight SDK + CLI
   Full MSSQL Schema
   (Revised – core schema corrections applied)
   ============================================================ */

-- =========================
-- USERS
-- =========================
CREATE TABLE dbo.bitsight_users (
    user_guid                  UNIQUEIDENTIFIER NOT NULL,
    friendly_name              NVARCHAR(255) NULL,
    formal_name                NVARCHAR(255) NULL,
    email                      NVARCHAR(255) NOT NULL,
    status                     NVARCHAR(64) NULL,
    landing_page               NVARCHAR(64) NULL,
    mfa_status                 NVARCHAR(64) NULL,
    last_login_time            DATETIME2 NULL,
    joined_time                DATETIME2 NULL,
    is_available_for_contact   BIT NULL,
    is_company_api_token       BIT NULL,
    group_guid                 UNIQUEIDENTIFIER NULL,
    group_name                 NVARCHAR(255) NULL,
    ingested_at                DATETIME2 NOT NULL,
    raw_payload                NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_users PRIMARY KEY (user_guid)
);

-- =========================
-- USER QUOTA
-- =========================
CREATE TABLE dbo.bitsight_user_quota (
    quota_type     NVARCHAR(128) NOT NULL,
    total          INT NULL,
    used           INT NULL,
    remaining      INT NULL,
    ingested_at    DATETIME2 NOT NULL,
    raw_payload    NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_user_quota PRIMARY KEY (quota_type)
);

-- =========================
-- COMPANIES
-- =========================
CREATE TABLE dbo.bitsight_companies (
    company_guid        UNIQUEIDENTIFIER NOT NULL,
    name                NVARCHAR(255) NOT NULL,
    domain              NVARCHAR(255) NULL,
    industry_name       NVARCHAR(255) NULL,
    industry_slug       NVARCHAR(255) NULL,
    sub_industry_name   NVARCHAR(255) NULL,
    sub_industry_slug   NVARCHAR(255) NULL,
    country             NVARCHAR(64) NULL,
    added_date          DATETIME2 NULL,
    rating              INT NULL,
    ingested_at         DATETIME2 NOT NULL,
    raw_payload         NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_companies PRIMARY KEY (company_guid)
);

-- =========================
-- PORTFOLIO
-- =========================
CREATE TABLE dbo.bitsight_portfolio (
    company_guid             UNIQUEIDENTIFIER NOT NULL,
    name                     NVARCHAR(255) NOT NULL,
    rating                   INT NULL,
    rating_date              DATE NULL,
    tier_name                NVARCHAR(255) NULL,
    relationship_name        NVARCHAR(255) NULL,
    subscription_type_name   NVARCHAR(255) NULL,
    subscription_type_slug   NVARCHAR(255) NULL,
    life_cycle_name          NVARCHAR(255) NULL,
    life_cycle_slug          NVARCHAR(255) NULL,
    network_size_v4          INT NULL,
    added_date               DATE NULL,
    ingested_at              DATETIME2 NOT NULL,
    raw_payload              NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_portfolio PRIMARY KEY (company_guid)
);

-- =========================
-- CURRENT RATINGS
-- =========================
CREATE TABLE dbo.bitsight_current_ratings (
    company_guid        UNIQUEIDENTIFIER NOT NULL,
    rating              INT NOT NULL,
    rating_date         DATE NOT NULL,
    network_size_v4     INT NULL,
    industry_name       NVARCHAR(255) NULL,
    industry_slug       NVARCHAR(255) NULL,
    sub_industry_name   NVARCHAR(255) NULL,
    sub_industry_slug   NVARCHAR(255) NULL,
    ingested_at         DATETIME2 NOT NULL,
    raw_payload         NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_current_ratings PRIMARY KEY (company_guid)
);

-- =========================
-- RATINGS HISTORY
-- =========================
CREATE TABLE dbo.bitsight_ratings_history (
    company_guid      UNIQUEIDENTIFIER NOT NULL,
    rating_date       DATE NOT NULL,
    rating            INT NOT NULL,
    ingested_at       DATETIME2 NOT NULL,
    raw_payload       NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_ratings_history
        PRIMARY KEY (company_guid, rating_date)
);

-- =========================
-- FINDINGS
-- =========================
CREATE TABLE dbo.bitsight_findings (
    finding_guid       UNIQUEIDENTIFIER NOT NULL,
    company_guid       UNIQUEIDENTIFIER NOT NULL,
    risk_vector        NVARCHAR(128) NULL,
    severity           NVARCHAR(64) NULL,
    status             NVARCHAR(64) NULL,
    first_seen_date    DATE NULL,
    last_seen_date     DATE NULL,
    remediation_date   DATE NULL,
    ingested_at        DATETIME2 NOT NULL,
    raw_payload        NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_findings PRIMARY KEY (finding_guid)
);

-- =========================
-- OBSERVATIONS
-- =========================
CREATE TABLE dbo.bitsight_observations (
    observation_guid UNIQUEIDENTIFIER NOT NULL,
    finding_guid     UNIQUEIDENTIFIER NULL,
    company_guid     UNIQUEIDENTIFIER NOT NULL,
    observed_date    DATE NULL,
    observation_type NVARCHAR(128) NULL,
    ingested_at      DATETIME2 NOT NULL,
    raw_payload      NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_observations PRIMARY KEY (observation_guid)
);

-- =========================
-- THREAT INTELLIGENCE (v2)
-- =========================
CREATE TABLE dbo.bitsight_threat_intel (
    threat_guid        UNIQUEIDENTIFIER NOT NULL,
    name               NVARCHAR(255) NOT NULL,
    category_name      NVARCHAR(128) NULL,
    severity_level     NVARCHAR(64) NULL,
    first_seen_date    DATE NULL,
    last_seen_date     DATE NULL,
    exposed_count      INT NULL,
    mitigated_count    INT NULL,
    epss_score         FLOAT NULL,
    epss_percentile    FLOAT NULL,
    evidence_certainty NVARCHAR(64) NULL,
    ingested_at        DATETIME2 NOT NULL,
    raw_payload        NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_threat_intel PRIMARY KEY (threat_guid)
);

-- =========================
-- ALERTS
-- =========================
CREATE TABLE dbo.bitsight_alerts (
    alert_guid     UNIQUEIDENTIFIER NOT NULL,
    alert_type     NVARCHAR(128) NULL,
    company_guid   UNIQUEIDENTIFIER NULL,
    severity       NVARCHAR(64) NULL,
    created_at     DATETIME2 NULL,
    ingested_at    DATETIME2 NOT NULL,
    raw_payload    NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_alerts PRIMARY KEY (alert_guid)
);

-- =========================
-- EXPOSED CREDENTIALS
-- =========================
CREATE TABLE dbo.bitsight_exposed_credentials (
    credential_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid    UNIQUEIDENTIFIER NULL,
    exposure_type   NVARCHAR(128) NULL,
    breach_name     NVARCHAR(255) NULL,
    first_seen_date DATE NULL,
    last_seen_date  DATE NULL,
    ingested_at     DATETIME2 NOT NULL,
    raw_payload     NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_exposed_credentials PRIMARY KEY (credential_guid)
);
