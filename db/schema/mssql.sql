/* ============================================================
   BitSight SDK + CLI
   COMPLETE MSSQL SCHEMA (DEPLOYMENT-GUARDED / RE-RUN SAFE)
   1:1 Physical Representation of ALL BitSight API Endpoints

   Notes:
   - No session-level SETs (don’t force instance/session behavior)
   - Every object is guarded (combat/resilient re-deploy)
   ============================================================ */

---------------------------------------------------------------
-- SCHEMA GUARD
---------------------------------------------------------------
IF SCHEMA_ID(N'dbo') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA dbo');
END;

---------------------------------------------------------------
-- COLLECTION STATE / INGESTION METADATA
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_collection_state', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_collection_state (
        endpoint_name     NVARCHAR(255) NOT NULL,
        last_run_at       DATETIME2(7) NULL,
        last_offset       INT NULL,
        last_cursor       NVARCHAR(512) NULL,
        last_status       NVARCHAR(64) NULL,
        ingested_at       DATETIME2(7) NOT NULL,
        CONSTRAINT PK_bitsight_collection_state PRIMARY KEY CLUSTERED (endpoint_name)
    );
END;

---------------------------------------------------------------
-- USERS
-- (Aligned to ingest/users.py: group_guid/group_name + JSON fields)
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_users', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_users (
        user_guid        UNIQUEIDENTIFIER NOT NULL,
        friendly_name    NVARCHAR(255) NULL,
        formal_name      NVARCHAR(255) NULL,
        email            NVARCHAR(255) NULL,

        group_guid       UNIQUEIDENTIFIER NULL,
        group_name       NVARCHAR(255) NULL,

        landing_page     NVARCHAR(64) NULL,
        status           NVARCHAR(64) NULL,
        last_login_time  DATETIME2(7) NULL,
        joined_time      DATETIME2(7) NULL,
        mfa_status       NVARCHAR(64) NULL,

        is_available_for_contact BIT NULL,
        is_company_api_token     BIT NULL,

        roles                          NVARCHAR(MAX) NULL,
        features                       NVARCHAR(MAX) NULL,
        preferred_contact_for_entities NVARCHAR(MAX) NULL,

        ingested_at      DATETIME2(7) NOT NULL,
        raw_payload      NVARCHAR(MAX) NOT NULL,

        CONSTRAINT PK_bitsight_users PRIMARY KEY CLUSTERED (user_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_user_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_user_details (
        user_guid    UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_user_details PRIMARY KEY CLUSTERED (user_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_user_quota', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_user_quota (
        quota_type   NVARCHAR(128) NOT NULL,
        total        INT NULL,
        used         INT NULL,
        remaining    INT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_user_quota PRIMARY KEY CLUSTERED (quota_type)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_user_company_views', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_user_company_views (
        user_guid    UNIQUEIDENTIFIER NOT NULL,
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_user_company_views PRIMARY KEY CLUSTERED (user_guid, company_guid)
    );
END;

---------------------------------------------------------------
-- COMPANIES
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_companies', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_companies (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        name         NVARCHAR(255) NULL,
        domain       NVARCHAR(255) NULL,
        country      NVARCHAR(64) NULL,
        added_date   DATETIME2(7) NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_companies PRIMARY KEY CLUSTERED (company_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_details (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_details PRIMARY KEY CLUSTERED (company_guid)
    );
END;

---------------------------------------------------------------
-- COMPANY RELATIONSHIPS / GOVERNANCE
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_company_relationships', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_relationships (
        relationship_guid   UNIQUEIDENTIFIER NOT NULL,
        company_guid        UNIQUEIDENTIFIER NOT NULL,
        company_name        NVARCHAR(255) NULL,
        relationship_type   NVARCHAR(64) NULL,
        creator             NVARCHAR(255) NULL,
        last_editor         NVARCHAR(255) NULL,
        created_time        DATETIME2(7) NULL,
        last_edited_time    DATETIME2(7) NULL,
        ingested_at         DATETIME2(7) NOT NULL,
        raw_payload         NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_relationships PRIMARY KEY CLUSTERED (relationship_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_requests', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_requests (
        request_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_requests PRIMARY KEY CLUSTERED (request_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_client_access_links', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_client_access_links (
        link_guid    UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_client_access_links PRIMARY KEY CLUSTERED (link_guid)
    );
END;

---------------------------------------------------------------
-- PORTFOLIO
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_portfolio', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_portfolio (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_portfolio PRIMARY KEY CLUSTERED (company_guid)
    );
END;

---------------------------------------------------------------
-- RATINGS
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_current_ratings', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_current_ratings (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        rating       INT NULL,
        rating_date  DATE NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_current_ratings PRIMARY KEY CLUSTERED (company_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_ratings_history', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_ratings_history (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        rating_date  DATE NOT NULL,
        rating       INT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_ratings_history PRIMARY KEY CLUSTERED (company_guid, rating_date)
    );
END;

---------------------------------------------------------------
-- FINDINGS / OBSERVATIONS / COMMENTS
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_findings', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_findings (
        finding_guid UNIQUEIDENTIFIER NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_findings PRIMARY KEY CLUSTERED (finding_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_findings_statistics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_findings_statistics (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_findings_statistics PRIMARY KEY CLUSTERED (company_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_observations', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_observations (
        observation_guid UNIQUEIDENTIFIER NOT NULL,
        company_guid     UNIQUEIDENTIFIER NULL,
        ingested_at      DATETIME2(7) NOT NULL,
        raw_payload      NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_observations PRIMARY KEY CLUSTERED (observation_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_finding_comments', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_finding_comments (
        finding_guid     UNIQUEIDENTIFIER NOT NULL,
        comment_guid     UNIQUEIDENTIFIER NOT NULL,
        thread_guid      UNIQUEIDENTIFIER NULL,
        created_time     DATETIME2(7) NULL,
        last_update_time DATETIME2(7) NULL,
        message          NVARCHAR(MAX) NULL,
        is_public        BIT NULL,
        is_deleted       BIT NULL,
        parent_guid      UNIQUEIDENTIFIER NULL,
        author_guid      UNIQUEIDENTIFIER NULL,
        author_name      NVARCHAR(255) NULL,
        company_guid     UNIQUEIDENTIFIER NULL,
        company_name     NVARCHAR(255) NULL,
        tagged_users     NVARCHAR(MAX) NULL,
        remediation      NVARCHAR(MAX) NULL,
        ingested_at      DATETIME2(7) NOT NULL,
        raw_payload      NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_finding_comments PRIMARY KEY CLUSTERED (finding_guid, comment_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_findings_statistics_global', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_findings_statistics_global (
        scope       NVARCHAR(64) NOT NULL CONSTRAINT DF_bitsight_findings_statistics_global_scope DEFAULT (N'global'),
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_findings_statistics_global PRIMARY KEY CLUSTERED (scope)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_findings_summaries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_findings_summaries (
        scope       NVARCHAR(64) NOT NULL CONSTRAINT DF_bitsight_findings_summaries_scope DEFAULT (N'global'),
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_findings_summaries PRIMARY KEY CLUSTERED (scope)
    );
END;

---------------------------------------------------------------
-- ASSETS / INFRASTRUCTURE
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_company_infrastructure', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_infrastructure (
        company_guid    UNIQUEIDENTIFIER NOT NULL,
        temporary_id    NVARCHAR(255) NOT NULL,
        value           NVARCHAR(255) NULL,
        asset_type      NVARCHAR(64) NULL,
        source          NVARCHAR(255) NULL,
        country         NVARCHAR(64) NULL,
        start_date      DATE NULL,
        end_date        DATE NULL,
        is_active       BIT NULL,
        attributed_guid UNIQUEIDENTIFIER NULL,
        attributed_name NVARCHAR(255) NULL,
        ip_count        INT NULL,
        is_suppressed   BIT NULL,
        asn             NVARCHAR(64) NULL,
        tags            NVARCHAR(MAX) NULL,
        ingested_at     DATETIME2(7) NOT NULL,
        raw_payload     NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_infrastructure PRIMARY KEY CLUSTERED (company_guid, temporary_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_asset_summaries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_asset_summaries (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_asset_summaries PRIMARY KEY CLUSTERED (company_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_asset_risk_matrix', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_asset_risk_matrix (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_asset_risk_matrix PRIMARY KEY CLUSTERED (company_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_my_infrastructure', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_my_infrastructure (
        ingest_id       BIGINT IDENTITY(1,1) NOT NULL,
        asset_guid      UNIQUEIDENTIFIER NULL,
        asset_type      NVARCHAR(64) NULL,
        ip_address      NVARCHAR(64) NULL,
        domain          NVARCHAR(255) NULL,
        first_seen_date DATETIME2(7) NULL,
        last_seen_date  DATETIME2(7) NULL,
        ingested_at     DATETIME2(7) NOT NULL,
        raw_payload     NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_my_infrastructure PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

---------------------------------------------------------------
-- THREAT INTELLIGENCE (v2)
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_threats', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threats (
        threat_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threats PRIMARY KEY CLUSTERED (threat_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_threat_statistics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threat_statistics (
        scope       NVARCHAR(64) NOT NULL CONSTRAINT DF_bitsight_threat_statistics_scope DEFAULT (N'global'),
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threat_statistics PRIMARY KEY CLUSTERED (scope)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_threats_impact', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threats_impact (
        threat_guid  UNIQUEIDENTIFIER NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threats_impact PRIMARY KEY CLUSTERED (threat_guid, ingested_at)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_threats_evidence', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threats_evidence (
        threat_guid UNIQUEIDENTIFIER NOT NULL,
        entity_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threats_evidence PRIMARY KEY CLUSTERED (threat_guid, entity_guid, ingested_at)
    );
END;

---------------------------------------------------------------
-- PRODUCTS / PROVIDERS / RATINGS TREE
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_company_products', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_products (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        product_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_products PRIMARY KEY CLUSTERED (company_guid, product_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_domain_products', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_domain_products (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        domain_name  NVARCHAR(255) NOT NULL,
        product_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_domain_products PRIMARY KEY CLUSTERED (company_guid, domain_name, product_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_domain_providers', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_domain_providers (
        company_guid  UNIQUEIDENTIFIER NOT NULL,
        domain_name   NVARCHAR(255) NOT NULL,
        provider_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_domain_providers PRIMARY KEY CLUSTERED (company_guid, domain_name, provider_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_provider_products', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_provider_products (
        provider_guid UNIQUEIDENTIFIER NOT NULL,
        product_guid  UNIQUEIDENTIFIER NOT NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_provider_products PRIMARY KEY CLUSTERED (provider_guid, product_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_provider_dependencies', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_provider_dependencies (
        provider_guid UNIQUEIDENTIFIER NOT NULL,
        company_guid  UNIQUEIDENTIFIER NOT NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_provider_dependencies PRIMARY KEY CLUSTERED (provider_guid, company_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_ratings_tree_product_companies', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_ratings_tree_product_companies (
        product_guid UNIQUEIDENTIFIER NOT NULL,
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_ratings_tree_product_companies PRIMARY KEY CLUSTERED (product_guid, company_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_ratings_tree_product_types', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_ratings_tree_product_types (
        product_type NVARCHAR(255) NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_ratings_tree_product_types PRIMARY KEY CLUSTERED (product_type)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_service_providers', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_service_providers (
        company_guid  UNIQUEIDENTIFIER NOT NULL,
        provider_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_service_providers PRIMARY KEY CLUSTERED (company_guid, provider_guid)
    );
END;

---------------------------------------------------------------
-- NEWS / ALERTS / INSIGHTS
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_news', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_news (
        news_guid    UNIQUEIDENTIFIER NOT NULL,
        published_at DATETIME2(7) NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_news PRIMARY KEY CLUSTERED (news_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_alerts', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_alerts (
        alert_guid  UNIQUEIDENTIFIER NOT NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_alerts PRIMARY KEY CLUSTERED (alert_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_insights', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_insights (
        insight_guid UNIQUEIDENTIFIER NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_insights PRIMARY KEY CLUSTERED (insight_guid)
    );
END;

---------------------------------------------------------------
-- COMPLIANCE / REPORTING
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_nist_csf_reports', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_nist_csf_reports (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_nist_csf_reports PRIMARY KEY CLUSTERED (company_guid, ingested_at)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_reports', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_reports (
        report_id    NVARCHAR(255) NOT NULL,
        report_type  NVARCHAR(128) NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        status       NVARCHAR(64) NULL,
        requested_at DATETIME2(7) NULL,
        completed_at DATETIME2(7) NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_reports PRIMARY KEY CLUSTERED (report_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_rapid_underwriting_assessments', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_rapid_underwriting_assessments (
        company_name NVARCHAR(255) NOT NULL,
        domain       NVARCHAR(255) NULL,
        requested_at DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_rapid_underwriting_assessments PRIMARY KEY CLUSTERED (company_name, requested_at)
    );
END;

---------------------------------------------------------------
-- LOOKUPS / CATALOGS
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_industries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_industries (
        industry_slug NVARCHAR(255) NOT NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_industries PRIMARY KEY CLUSTERED (industry_slug)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_tiers', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_tiers (
        tier_slug   NVARCHAR(255) NOT NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_tiers PRIMARY KEY CLUSTERED (tier_slug)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_lifecycle_states', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_lifecycle_states (
        lifecycle_slug NVARCHAR(255) NOT NULL,
        ingested_at    DATETIME2(7) NOT NULL,
        raw_payload    NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_lifecycle_states PRIMARY KEY CLUSTERED (lifecycle_slug)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_static_data', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_static_data (
        scope       NVARCHAR(64) NOT NULL CONSTRAINT DF_bitsight_static_data_scope DEFAULT (N'global'),
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_static_data PRIMARY KEY CLUSTERED (scope)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_statistics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_statistics (
        scope       NVARCHAR(64) NOT NULL CONSTRAINT DF_bitsight_statistics_scope DEFAULT (N'global'),
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_statistics PRIMARY KEY CLUSTERED (scope)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_risk_vectors_summary', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_risk_vectors_summary (
        scope        NVARCHAR(64) NOT NULL CONSTRAINT DF_bitsight_risk_vectors_summary_scope DEFAULT (N'global'),
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_risk_vectors_summary PRIMARY KEY CLUSTERED (scope, ingested_at)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_peer_analytics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_peer_analytics (
        scope         NVARCHAR(64) NOT NULL CONSTRAINT DF_bitsight_peer_analytics_scope DEFAULT (N'global'),
        company_guid  UNIQUEIDENTIFIER NULL,
        industry_slug NVARCHAR(255) NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_peer_analytics PRIMARY KEY CLUSTERED (scope, ingested_at)
    );
END;

---------------------------------------------------------------
-- SUBSIDIARIES (no /ratings prefix endpoints)
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_subsidiaries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_subsidiaries (
        subsidiary_guid     UNIQUEIDENTIFIER NOT NULL,
        parent_company_guid UNIQUEIDENTIFIER NULL,
        ingested_at         DATETIME2(7) NOT NULL,
        raw_payload         NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_subsidiaries PRIMARY KEY CLUSTERED (subsidiary_guid)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_subsidiary_statistics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_subsidiary_statistics (
        ingest_id       BIGINT IDENTITY(1,1) NOT NULL,
        subsidiary_guid UNIQUEIDENTIFIER NULL,
        ingested_at     DATETIME2(7) NOT NULL,
        raw_payload     NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_subsidiary_statistics PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

---------------------------------------------------------------
-- FOLDERS
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_folders', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_folders (
        folder_guid UNIQUEIDENTIFIER NOT NULL,
        name        NVARCHAR(255) NULL,
        owner_guid  UNIQUEIDENTIFIER NULL,
        owner_email NVARCHAR(255) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_folders PRIMARY KEY CLUSTERED (folder_guid)
    );
END;

---------------------------------------------------------------
-- EXPOSED CREDENTIALS
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_exposed_credentials', N'U') IS NULL
BEGIN
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
END;

---------------------------------------------------------------
-- SUBSCRIPTIONS
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_subscriptions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_subscriptions (
        subscription_guid      UNIQUEIDENTIFIER NOT NULL,
        company_guid           UNIQUEIDENTIFIER NULL,
        subscription_type_name NVARCHAR(255) NULL,
        subscription_type_slug NVARCHAR(255) NULL,
        life_cycle_name        NVARCHAR(255) NULL,
        life_cycle_slug        NVARCHAR(255) NULL,
        start_date             DATETIME2(7) NULL,
        end_date               DATETIME2(7) NULL,
        ingested_at            DATETIME2(7) NOT NULL,
        raw_payload            NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_subscriptions PRIMARY KEY CLUSTERED (subscription_guid)
    );
END;

---------------------------------------------------------------
-- CURRENT RATINGS LICENSE CONSUMPTION (v2 action)
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_use_current_ratings_license', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_use_current_ratings_license (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        requested_at DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_use_current_ratings_license PRIMARY KEY CLUSTERED (company_guid, requested_at)
    );
END;

---------------------------------------------------------------
-- OPERATOR-FRIENDLY INDEXES (GUARDED)
---------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_users_email' AND object_id = OBJECT_ID(N'dbo.bitsight_users')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_users_email
    ON dbo.bitsight_users (email)
    WHERE email IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_companies_domain' AND object_id = OBJECT_ID(N'dbo.bitsight_companies')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_companies_domain
    ON dbo.bitsight_companies (domain)
    WHERE domain IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_findings_company_guid' AND object_id = OBJECT_ID(N'dbo.bitsight_findings')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_findings_company_guid
    ON dbo.bitsight_findings (company_guid)
    WHERE company_guid IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_observations_company_guid' AND object_id = OBJECT_ID(N'dbo.bitsight_observations')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_observations_company_guid
    ON dbo.bitsight_observations (company_guid)
    WHERE company_guid IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_news_published_at' AND object_id = OBJECT_ID(N'dbo.bitsight_news')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_news_published_at
    ON dbo.bitsight_news (published_at)
    WHERE published_at IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_ratings_history_company_date' AND object_id = OBJECT_ID(N'dbo.bitsight_ratings_history')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_ratings_history_company_date
    ON dbo.bitsight_ratings_history (company_guid, rating_date);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_my_infrastructure_ingested_at' AND object_id = OBJECT_ID(N'dbo.bitsight_my_infrastructure')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_my_infrastructure_ingested_at
    ON dbo.bitsight_my_infrastructure (ingested_at);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_subsidiary_statistics_ingested_at' AND object_id = OBJECT_ID(N'dbo.bitsight_subsidiary_statistics')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_subsidiary_statistics_ingested_at
    ON dbo.bitsight_subsidiary_statistics (ingested_at);
END;
