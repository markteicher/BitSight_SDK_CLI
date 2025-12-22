/* ============================================================
   BitSight SDK + CLI
   COMPLETE MSSQL SCHEMA
   1:1 Physical Representation of ALL BitSight API Endpoints
   ============================================================ */

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

---------------------------------------------------------------
-- COLLECTION STATE / INGESTION METADATA
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_collection_state (
    endpoint_name     NVARCHAR(255) NOT NULL,
    last_run_at       DATETIME2(7) NULL,
    last_offset       INT NULL,
    last_cursor       NVARCHAR(512) NULL,
    last_status       NVARCHAR(64) NULL,
    ingested_at       DATETIME2(7) NOT NULL,
    CONSTRAINT PK_bitsight_collection_state PRIMARY KEY CLUSTERED (endpoint_name)
);
GO

---------------------------------------------------------------
-- USERS
-- (Aligned to ingest/users.py: group_guid/group_name + JSON fields)
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_users (
    user_guid        UNIQUEIDENTIFIER NOT NULL,
    friendly_name    NVARCHAR(255),
    formal_name      NVARCHAR(255),
    email            NVARCHAR(255),

    group_guid       UNIQUEIDENTIFIER NULL,
    group_name       NVARCHAR(255) NULL,

    landing_page     NVARCHAR(64),
    status           NVARCHAR(64),
    last_login_time  DATETIME2(7),
    joined_time      DATETIME2(7),
    mfa_status       NVARCHAR(64),

    is_available_for_contact BIT NULL,
    is_company_api_token     BIT NULL,

    roles                          NVARCHAR(MAX) NULL,
    features                       NVARCHAR(MAX) NULL,
    preferred_contact_for_entities NVARCHAR(MAX) NULL,

    ingested_at      DATETIME2(7) NOT NULL,
    raw_payload      NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_users PRIMARY KEY CLUSTERED (user_guid)
);
GO

CREATE TABLE dbo.bitsight_user_details (
    user_guid    UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_user_details PRIMARY KEY CLUSTERED (user_guid)
);
GO

CREATE TABLE dbo.bitsight_user_quota (
    quota_type   NVARCHAR(128) NOT NULL,
    total        INT,
    used         INT,
    remaining    INT,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_user_quota PRIMARY KEY CLUSTERED (quota_type)
);
GO

CREATE TABLE dbo.bitsight_user_company_views (
    user_guid    UNIQUEIDENTIFIER NOT NULL,
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_user_company_views
        PRIMARY KEY CLUSTERED (user_guid, company_guid)
);
GO

---------------------------------------------------------------
-- COMPANIES
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_companies (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    name         NVARCHAR(255),
    domain       NVARCHAR(255),
    country      NVARCHAR(64),
    added_date   DATETIME2(7),
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_companies PRIMARY KEY CLUSTERED (company_guid)
);
GO

CREATE TABLE dbo.bitsight_company_details (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_details PRIMARY KEY CLUSTERED (company_guid)
);
GO

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
    created_time        DATETIME2(7),
    last_edited_time    DATETIME2(7),
    ingested_at         DATETIME2(7) NOT NULL,
    raw_payload         NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_relationships
        PRIMARY KEY CLUSTERED (relationship_guid)
);
GO

CREATE TABLE dbo.bitsight_company_requests (
    request_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_requests PRIMARY KEY CLUSTERED (request_guid)
);
GO

CREATE TABLE dbo.bitsight_client_access_links (
    link_guid    UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_client_access_links PRIMARY KEY CLUSTERED (link_guid)
);
GO

---------------------------------------------------------------
-- PORTFOLIO
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_portfolio (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_portfolio PRIMARY KEY CLUSTERED (company_guid)
);
GO

---------------------------------------------------------------
-- RATINGS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_current_ratings (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    rating       INT,
    rating_date  DATE,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_current_ratings PRIMARY KEY CLUSTERED (company_guid)
);
GO

CREATE TABLE dbo.bitsight_ratings_history (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    rating_date  DATE NOT NULL,
    rating       INT,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_ratings_history
        PRIMARY KEY CLUSTERED (company_guid, rating_date)
);
GO

---------------------------------------------------------------
-- FINDINGS / OBSERVATIONS / COMMENTS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_findings (
    finding_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid UNIQUEIDENTIFIER,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_findings PRIMARY KEY CLUSTERED (finding_guid)
);
GO

CREATE TABLE dbo.bitsight_findings_statistics (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_findings_statistics PRIMARY KEY CLUSTERED (company_guid)
);
GO

CREATE TABLE dbo.bitsight_observations (
    observation_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid     UNIQUEIDENTIFIER,
    ingested_at      DATETIME2(7) NOT NULL,
    raw_payload      NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_observations PRIMARY KEY CLUSTERED (observation_guid)
);
GO

CREATE TABLE dbo.bitsight_finding_comments (
    finding_guid     UNIQUEIDENTIFIER NOT NULL,
    comment_guid     UNIQUEIDENTIFIER NOT NULL,
    thread_guid      UNIQUEIDENTIFIER,
    created_time     DATETIME2(7),
    last_update_time DATETIME2(7),
    message          NVARCHAR(MAX),
    is_public        BIT,
    is_deleted       BIT,
    parent_guid      UNIQUEIDENTIFIER,
    author_guid      UNIQUEIDENTIFIER,
    author_name      NVARCHAR(255),
    company_guid     UNIQUEIDENTIFIER,
    company_name     NVARCHAR(255) NULL,
    tagged_users     NVARCHAR(MAX),
    remediation      NVARCHAR(MAX),
    ingested_at      DATETIME2(7) NOT NULL,
    raw_payload      NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_finding_comments
        PRIMARY KEY CLUSTERED (finding_guid, comment_guid)
);
GO

-- Global findings summaries/statistics (non-company scoped)
CREATE TABLE dbo.bitsight_findings_statistics_global (
    scope       NVARCHAR(64) NOT NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_findings_statistics_global PRIMARY KEY CLUSTERED (scope)
);
GO

CREATE TABLE dbo.bitsight_findings_summaries (
    scope       NVARCHAR(64) NOT NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_findings_summaries PRIMARY KEY CLUSTERED (scope)
);
GO

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
    ingested_at     DATETIME2(7) NOT NULL,
    raw_payload     NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_infrastructure
        PRIMARY KEY CLUSTERED (company_guid, temporary_id)
);
GO

CREATE TABLE dbo.bitsight_asset_summaries (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_asset_summaries PRIMARY KEY CLUSTERED (company_guid)
);
GO

CREATE TABLE dbo.bitsight_asset_risk_matrix (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_asset_risk_matrix PRIMARY KEY CLUSTERED (company_guid)
);
GO

CREATE TABLE dbo.bitsight_my_infrastructure (
    asset_guid    UNIQUEIDENTIFIER NULL,
    asset_type    NVARCHAR(64) NULL,
    ip_address    NVARCHAR(64) NULL,
    domain        NVARCHAR(255) NULL,
    first_seen_date DATETIME2(7) NULL,
    last_seen_date  DATETIME2(7) NULL,
    ingested_at   DATETIME2(7) NOT NULL,
    raw_payload   NVARCHAR(MAX) NOT NULL,
    -- no deterministic natural key guaranteed in your ingest; store by ingested_at + raw hash via payload externally if needed
    CONSTRAINT PK_bitsight_my_infrastructure PRIMARY KEY CLUSTERED (ingested_at, raw_payload)
);
GO

---------------------------------------------------------------
-- THREAT INTELLIGENCE (v2)
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_threats (
    threat_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_threats PRIMARY KEY CLUSTERED (threat_guid)
);
GO

CREATE TABLE dbo.bitsight_threat_statistics (
    scope       NVARCHAR(64) NOT NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_threat_statistics PRIMARY KEY CLUSTERED (scope)
);
GO

CREATE TABLE dbo.bitsight_threats_impact (
    threat_guid  UNIQUEIDENTIFIER NOT NULL,
    company_guid UNIQUEIDENTIFIER NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_threats_impact PRIMARY KEY CLUSTERED (threat_guid, ingested_at)
);
GO

CREATE TABLE dbo.bitsight_threats_evidence (
    threat_guid UNIQUEIDENTIFIER NOT NULL,
    entity_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_threats_evidence PRIMARY KEY CLUSTERED (threat_guid, entity_guid, ingested_at)
);
GO

---------------------------------------------------------------
-- PRODUCTS / PROVIDERS / RATINGS TREE
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_company_products (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    product_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_company_products
        PRIMARY KEY CLUSTERED (company_guid, product_guid)
);
GO

CREATE TABLE dbo.bitsight_domain_products (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    domain_name  NVARCHAR(255) NOT NULL,
    product_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_domain_products
        PRIMARY KEY CLUSTERED (company_guid, domain_name, product_guid)
);
GO

CREATE TABLE dbo.bitsight_domain_providers (
    company_guid  UNIQUEIDENTIFIER NOT NULL,
    domain_name   NVARCHAR(255) NOT NULL,
    provider_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at   DATETIME2(7) NOT NULL,
    raw_payload   NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_domain_providers
        PRIMARY KEY CLUSTERED (company_guid, domain_name, provider_guid)
);
GO

CREATE TABLE dbo.bitsight_provider_products (
    provider_guid UNIQUEIDENTIFIER NOT NULL,
    product_guid  UNIQUEIDENTIFIER NOT NULL,
    ingested_at   DATETIME2(7) NOT NULL,
    raw_payload   NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_provider_products
        PRIMARY KEY CLUSTERED (provider_guid, product_guid)
);
GO

CREATE TABLE dbo.bitsight_provider_dependencies (
    provider_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid  UNIQUEIDENTIFIER NOT NULL,
    ingested_at   DATETIME2(7) NOT NULL,
    raw_payload   NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_provider_dependencies
        PRIMARY KEY CLUSTERED (provider_guid, company_guid)
);
GO

CREATE TABLE dbo.bitsight_ratings_tree_product_companies (
    product_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_ratings_tree_product_companies
        PRIMARY KEY CLUSTERED (product_guid, company_guid)
);
GO

CREATE TABLE dbo.bitsight_ratings_tree_product_types (
    product_type NVARCHAR(255) NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_ratings_tree_product_types PRIMARY KEY CLUSTERED (product_type)
);
GO

CREATE TABLE dbo.bitsight_service_providers (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    provider_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_service_providers PRIMARY KEY CLUSTERED (company_guid, provider_guid)
);
GO

---------------------------------------------------------------
-- NEWS / ALERTS / INSIGHTS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_news (
    news_guid    UNIQUEIDENTIFIER NOT NULL,
    published_at DATETIME2(7),
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_news PRIMARY KEY CLUSTERED (news_guid)
);
GO

CREATE TABLE dbo.bitsight_alerts (
    alert_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_alerts PRIMARY KEY CLUSTERED (alert_guid)
);
GO

CREATE TABLE dbo.bitsight_insights (
    insight_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid UNIQUEIDENTIFIER,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_insights PRIMARY KEY CLUSTERED (insight_guid)
);
GO

---------------------------------------------------------------
-- COMPLIANCE / REPORTING
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_nist_csf_reports (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_nist_csf_reports
        PRIMARY KEY CLUSTERED (company_guid, ingested_at)
);
GO

CREATE TABLE dbo.bitsight_reports (
    report_id    NVARCHAR(255) NOT NULL,
    report_type  NVARCHAR(128),
    company_guid UNIQUEIDENTIFIER,
    status       NVARCHAR(64),
    requested_at DATETIME2(7),
    completed_at DATETIME2(7),
    ingested_at  DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_reports PRIMARY KEY CLUSTERED (report_id)
);
GO

CREATE TABLE dbo.bitsight_rapid_underwriting_assessments (
    company_name NVARCHAR(255) NOT NULL,
    domain       NVARCHAR(255) NULL,
    requested_at DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_rapid_underwriting_assessments
        PRIMARY KEY CLUSTERED (company_name, requested_at)
);
GO

---------------------------------------------------------------
-- LOOKUPS / CATALOGS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_industries (
    industry_slug NVARCHAR(255) NOT NULL,
    ingested_at   DATETIME2(7) NOT NULL,
    raw_payload   NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_industries PRIMARY KEY CLUSTERED (industry_slug)
);
GO

CREATE TABLE dbo.bitsight_tiers (
    tier_slug   NVARCHAR(255) NOT NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_tiers PRIMARY KEY CLUSTERED (tier_slug)
);
GO

CREATE TABLE dbo.bitsight_lifecycle_states (
    lifecycle_slug NVARCHAR(255) NOT NULL,
    ingested_at    DATETIME2(7) NOT NULL,
    raw_payload    NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_lifecycle_states PRIMARY KEY CLUSTERED (lifecycle_slug)
);
GO

CREATE TABLE dbo.bitsight_static_data (
    scope       NVARCHAR(64) NOT NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_static_data PRIMARY KEY CLUSTERED (scope)
);
GO

CREATE TABLE dbo.bitsight_statistics (
    scope       NVARCHAR(64) NOT NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_statistics PRIMARY KEY CLUSTERED (scope)
);
GO

CREATE TABLE dbo.bitsight_risk_vectors_summary (
    scope       NVARCHAR(64) NOT NULL,
    company_guid UNIQUEIDENTIFIER NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_risk_vectors_summary PRIMARY KEY CLUSTERED (scope, ingested_at)
);
GO

CREATE TABLE dbo.bitsight_peer_analytics (
    scope       NVARCHAR(64) NOT NULL,
    company_guid UNIQUEIDENTIFIER NULL,
    industry_slug NVARCHAR(255) NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_peer_analytics PRIMARY KEY CLUSTERED (scope, ingested_at)
);
GO

---------------------------------------------------------------
-- SUBSIDIARIES (no /ratings prefix endpoints)
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_subsidiaries (
    subsidiary_guid UNIQUEIDENTIFIER NOT NULL,
    parent_company_guid UNIQUEIDENTIFIER NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_subsidiaries PRIMARY KEY CLUSTERED (subsidiary_guid)
);
GO

CREATE TABLE dbo.bitsight_subsidiary_statistics (
    subsidiary_guid UNIQUEIDENTIFIER NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_subsidiary_statistics PRIMARY KEY CLUSTERED (ingested_at, raw_payload)
);
GO

---------------------------------------------------------------
-- FOLDERS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_folders (
    folder_guid UNIQUEIDENTIFIER NOT NULL,
    name        NVARCHAR(255) NULL,
    owner_guid  UNIQUEIDENTIFIER NULL,
    owner_email NVARCHAR(255) NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_folders PRIMARY KEY CLUSTERED (folder_guid)
);
GO

---------------------------------------------------------------
-- EXPOSED CREDENTIALS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_exposed_credentials (
    credential_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid    UNIQUEIDENTIFIER NULL,
    exposure_type   NVARCHAR(128) NULL,
    breach_name     NVARCHAR(255) NULL,
    first_seen_date DATETIME2(7) NULL,
    last_seen_date  DATETIME2(7) NULL,
    ingested_at     DATETIME2(7) NOT NULL,
    raw_payload     NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_exposed_credentials PRIMARY KEY CLUSTERED (credential_guid)
);
GO

---------------------------------------------------------------
-- SUBSCRIPTIONS
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_subscriptions (
    subscription_guid UNIQUEIDENTIFIER NOT NULL,
    company_guid UNIQUEIDENTIFIER NULL,
    subscription_type_name NVARCHAR(255) NULL,
    subscription_type_slug NVARCHAR(255) NULL,
    life_cycle_name NVARCHAR(255) NULL,
    life_cycle_slug NVARCHAR(255) NULL,
    start_date DATETIME2(7) NULL,
    end_date   DATETIME2(7) NULL,
    ingested_at DATETIME2(7) NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_subscriptions PRIMARY KEY CLUSTERED (subscription_guid)
);
GO

---------------------------------------------------------------
-- CURRENT RATINGS LICENSE CONSUMPTION (v2 action)
---------------------------------------------------------------
CREATE TABLE dbo.bitsight_use_current_ratings_license (
    company_guid UNIQUEIDENTIFIER NOT NULL,
    requested_at DATETIME2(7) NOT NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_bitsight_use_current_ratings_license PRIMARY KEY CLUSTERED (company_guid, requested_at)
);
GO
