/* ============================================================
   BitSight SDK + CLI
   COMPLETE MSSQL SCHEMA
   1:1 Physical Representation of ALL BitSight API Endpoints
   ============================================================ */

---------------------------------------------------------------
-- GLOBAL METADATA (pagination, cursors, collection state)
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_collection_state (
    endpoint_name     NVARCHAR(255) NOT NULL,
    last_run_at       DATETIME2 NULL,
    last_offset       INT NULL,
    last_cursor       NVARCHAR(512) NULL,
    last_status       NVARCHAR(64) NULL,
    ingested_at       DATETIME2 NOT NULL,
    CONSTRAINT PK_bitsight_collection_state PRIMARY KEY (endpoint_name)
);

---------------------------------------------------------------
-- USERS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_users (
    user_guid        UNIQUEIDENTIFIER NOT NULL,
    friendly_name    NVARCHAR(255),
    formal_name      NVARCHAR(255),
    email            NVARCHAR(255),
    status           NVARCHAR(64),
    landing_page     NVARCHAR(64),
    mfa_status       NVARCHAR(64),
    last_login_time  DATETIME2,
    joined_time      DATETIME2,
    ingested_at      DATETIME2 NOT NULL,
    raw_payload      NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_users PRIMARY KEY (user_guid)
);

CREATE TABLE dbo.bitsight_user_details (
    user_guid    UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_user_details PRIMARY KEY (user_guid)
);

CREATE TABLE dbo.bitsight_user_quota (
    quota_type   NVARCHAR(128) NOT NULL,
    total        INT,
    used         INT,
    remaining    INT,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_user_quota PRIMARY KEY (quota_type)
);

CREATE TABLE dbo.bitsight_user_company_views (
    user_guid    UNIQUEIDENTIFIER NOT NULL,
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_user_company_views
        PRIMARY KEY (user_guid, company_guid)
);

---------------------------------------------------------------
-- COMPANIES
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_companies (
    company_guid      UNIQUEIDENTIFIER NOT NULL,
    name              NVARCHAR(255),
    domain            NVARCHAR(255),
    country           NVARCHAR(64),
    added_date        DATETIME2,
    ingested_at       DATETIME2 NOT NULL,
    raw_payload       NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_companies PRIMARY KEY (company_guid)
);

CREATE TABLE dbo.bitsight_company_details (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_details PRIMARY KEY (company_guid)
);

---------------------------------------------------------------
-- PORTFOLIO
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_portfolio (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_portfolio PRIMARY KEY (company_guid)
);

---------------------------------------------------------------
-- CURRENT RATINGS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_current_ratings (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    rating       INT,
    rating_date  DATE,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_current_ratings PRIMARY KEY (company_guid)
);

---------------------------------------------------------------
-- RATINGS HISTORY
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_ratings_history (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    rating_date  DATE NOT NULL,
    rating       INT,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_ratings_history
        PRIMARY KEY (company_guid, rating_date)
);

---------------------------------------------------------------
-- FINDINGS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_findings (
    finding_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid UNIQUEIDENTIFIER,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_findings PRIMARY KEY (finding_guid)
);

CREATE TABLE dbo.bitsight_findings_statistics (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_findings_statistics PRIMARY KEY (company_guid)
);

---------------------------------------------------------------
-- OBSERVATIONS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_observations (
    observation_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid     UNIQUEIDENTIFIER,
    ingested_at      DATETIME2 NOT NULL,
    raw_payload      NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_observations PRIMARY KEY (observation_guid)
);

---------------------------------------------------------------
-- THREATS (v2)
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_threats (
    threat_guid UNIQUEIDENTIFIER NOT NULL,
    name        NVARCHAR(255),
    ingested_at DATETIME2 NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_threats PRIMARY KEY (threat_guid)
);

---------------------------------------------------------------
-- ALERTS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_alerts (
    alert_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at DATETIME2 NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_alerts PRIMARY KEY (alert_guid)
);

---------------------------------------------------------------
-- EXPOSED CREDENTIALS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_exposed_credentials (
    credential_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid    UNIQUEIDENTIFIER,
    ingested_at     DATETIME2 NOT NULL,
    raw_payload     NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_exposed_credentials PRIMARY KEY (credential_guid)
);

---------------------------------------------------------------
-- NEWS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_news (
    news_guid   UNIQUEIDENTIFIER NOT NULL,
    published_at DATETIME2,
    ingested_at DATETIME2 NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_news PRIMARY KEY (news_guid)
);

---------------------------------------------------------------
-- INSIGHTS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_insights (
    insight_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid UNIQUEIDENTIFIER,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_insights PRIMARY KEY (insight_guid)
);

---------------------------------------------------------------
-- SUBSIDIARIES
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_subsidiaries (
    subsidiary_guid UNIQUEIDENTIFIER NOT NULL,
    parent_company_guid UNIQUEIDENTIFIER,
    ingested_at DATETIME2 NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_subsidiaries PRIMARY KEY (subsidiary_guid)
);

CREATE TABLE dbo.bitsight_subsidiary_statistics (
    subsidiary_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at DATETIME2 NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_subsidiary_statistics PRIMARY KEY (subsidiary_guid)
);

---------------------------------------------------------------
-- LOOKUP / CATALOG ENDPOINTS (STATIC BUT PHYSICAL)
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_industries (
    industry_slug NVARCHAR(255) NOT NULL,
    ingested_at   DATETIME2 NOT NULL,
    raw_payload   NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_industries PRIMARY KEY (industry_slug)
);

CREATE TABLE dbo.bitsight_tiers (
    tier_slug   NVARCHAR(255) NOT NULL,
    ingested_at DATETIME2 NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_tiers PRIMARY KEY (tier_slug)
);

CREATE TABLE dbo.bitsight_lifecycle_states (
    lifecycle_slug NVARCHAR(255) NOT NULL,
    ingested_at    DATETIME2 NOT NULL,
    raw_payload    NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_lifecycle_states PRIMARY KEY (lifecycle_slug)
);
