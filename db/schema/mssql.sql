/* ============================================================
   BitSight SDK + CLI
   COMPLETE MSSQL SCHEMA
   1:1 Physical Representation of ALL BitSight API Endpoints
   ============================================================ */

---------------------------------------------------------------
-- COLLECTION STATE / INGESTION METADATA
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
    company_guid UNIQUEIDENTIFIER NOT NULL,
    name         NVARCHAR(255),
    domain       NVARCHAR(255),
    country      NVARCHAR(64),
    added_date   DATETIME2,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_companies PRIMARY KEY (company_guid)
);

CREATE TABLE dbo.bitsight_company_details (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_details PRIMARY KEY (company_guid)
);

---------------------------------------------------------------
-- COMPANY RELATIONSHIPS / GOVERNANCE
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_company_relationships (
    relationship_guid   UNIQUEIDENTIFIER NOT NULL,
    company_guid        UNIQUEIDENTIFIER NOT NULL,
    company_name        NVARCHAR(255),
    relationship_type   NVARCHAR(64),
    creator             NVARCHAR(255),
    last_editor         NVARCHAR(255),
    created_time        DATETIME2,
    last_edited_time    DATETIME2,
    ingested_at         DATETIME2 NOT NULL,
    raw_payload         NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_relationships
        PRIMARY KEY (relationship_guid)
);

CREATE TABLE dbo.bitsight_company_requests (
    request_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_requests PRIMARY KEY (request_guid)
);

CREATE TABLE dbo.bitsight_client_access_links (
    link_guid    UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_client_access_links PRIMARY KEY (link_guid)
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
-- RATINGS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_current_ratings (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    rating       INT,
    rating_date  DATE,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_current_ratings PRIMARY KEY (company_guid)
);

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
-- FINDINGS / OBSERVATIONS / COMMENTS
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

CREATE TABLE dbo.bitsight_observations (
    observation_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid     UNIQUEIDENTIFIER,
    ingested_at      DATETIME2 NOT NULL,
    raw_payload      NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_observations PRIMARY KEY (observation_guid)
);

CREATE TABLE dbo.bitsight_finding_comments (
    finding_guid     UNIQUEIDENTIFIER NOT NULL,
    comment_guid     UNIQUEIDENTIFIER NOT NULL,
    thread_guid      UNIQUEIDENTIFIER,
    created_time     DATETIME2,
    last_update_time DATETIME2,
    message          NVARCHAR(MAX),
    is_public        BIT,
    is_deleted       BIT,
    parent_guid      UNIQUEIDENTIFIER,
    author_guid      UNIQUEIDENTIFIER,
    author_name      NVARCHAR(255),
    company_guid     UNIQUEIDENTIFIER,
    tagged_users     NVARCHAR(MAX),
    remediation      NVARCHAR(MAX),
    ingested_at      DATETIME2 NOT NULL,
    raw_payload      NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_finding_comments
        PRIMARY KEY (finding_guid, comment_guid)
);

---------------------------------------------------------------
-- ASSETS / INFRASTRUCTURE
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_company_infrastructure (
    company_guid    UNIQUEIDENTIFIER NOT NULL,
    temporary_id    NVARCHAR(255) NOT NULL,
    value           NVARCHAR(255),
    asset_type      NVARCHAR(64),
    source          NVARCHAR(255),
    country         NVARCHAR(64),
    start_date      DATE,
    end_date        DATE,
    is_active       BIT,
    attributed_guid UNIQUEIDENTIFIER,
    attributed_name NVARCHAR(255),
    ip_count        INT,
    is_suppressed   BIT,
    asn             NVARCHAR(64),
    tags            NVARCHAR(MAX),
    ingested_at     DATETIME2 NOT NULL,
    raw_payload     NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_infrastructure
        PRIMARY KEY (company_guid, temporary_id)
);

CREATE TABLE dbo.bitsight_asset_summaries (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_asset_summaries PRIMARY KEY (company_guid)
);

CREATE TABLE dbo.bitsight_asset_risk_matrix (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_asset_risk_matrix PRIMARY KEY (company_guid)
);

---------------------------------------------------------------
-- THREAT INTELLIGENCE (v2)
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_threats (
    threat_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at DATETIME2 NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_threats PRIMARY KEY (threat_guid)
);

CREATE TABLE dbo.bitsight_threat_statistics (
    scope       NVARCHAR(64) NOT NULL,
    ingested_at DATETIME2 NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_threat_statistics PRIMARY KEY (scope)
);

---------------------------------------------------------------
-- PRODUCTS / PROVIDERS / RATINGS TREE
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_company_products (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    product_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_products
        PRIMARY KEY (company_guid, product_guid)
);

CREATE TABLE dbo.bitsight_domain_products (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    domain_name  NVARCHAR(255) NOT NULL,
    product_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_domain_products
        PRIMARY KEY (company_guid, domain_name, product_guid)
);

CREATE TABLE dbo.bitsight_domain_providers (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    domain_name  NVARCHAR(255) NOT NULL,
    provider_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_domain_providers
        PRIMARY KEY (company_guid, domain_name, provider_guid)
);

CREATE TABLE dbo.bitsight_provider_products (
    provider_guid UNIQUEIDENTIFIER NOT NULL,
    product_guid  UNIQUEIDENTIFIER NOT NULL,
    ingested_at   DATETIME2 NOT NULL,
    raw_payload   NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_provider_products
        PRIMARY KEY (provider_guid, product_guid)
);

CREATE TABLE dbo.bitsight_ratings_tree_product_types (
    product_type NVARCHAR(255) NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_ratings_tree_product_types PRIMARY KEY (product_type)
);

---------------------------------------------------------------
-- NEWS / ALERTS / INSIGHTS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_news (
    news_guid    UNIQUEIDENTIFIER NOT NULL,
    published_at DATETIME2,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_news PRIMARY KEY (news_guid)
);

CREATE TABLE dbo.bitsight_alerts (
    alert_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at DATETIME2 NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_alerts PRIMARY KEY (alert_guid)
);

CREATE TABLE dbo.bitsight_insights (
    insight_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid UNIQUEIDENTIFIER,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_insights PRIMARY KEY (insight_guid)
);

---------------------------------------------------------------
-- COMPLIANCE / REPORTING
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_nist_csf_reports (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_nist_csf_reports
        PRIMARY KEY (company_guid, ingested_at)
);

CREATE TABLE dbo.bitsight_reports (
    report_id    NVARCHAR(255) NOT NULL,
    report_type  NVARCHAR(128),
    company_guid UNIQUEIDENTIFIER,
    status       NVARCHAR(64),
    requested_at DATETIME2,
    completed_at DATETIME2,
    ingested_at  DATETIME2 NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_reports PRIMARY KEY (report_id)
);

---------------------------------------------------------------
-- LOOKUPS / CATALOGS
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
