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
        CONSTRAINT PK_bitsight_users PRIMARY KEY CLUSTERED (user_guid),
        CONSTRAINT CK_bitsight_users_raw_payload_json CHECK (ISJSON(raw_payload) = 1),
        CONSTRAINT CK_bitsight_users_roles_json CHECK (roles IS NULL OR ISJSON(roles) = 1),
        CONSTRAINT CK_bitsight_users_features_json CHECK (features IS NULL OR ISJSON(features) = 1),
        CONSTRAINT CK_bitsight_users_preferred_contacts_json
            CHECK (preferred_contact_for_entities IS NULL OR ISJSON(preferred_contact_for_entities) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_user_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_user_details (
        user_guid    UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_user_details PRIMARY KEY CLUSTERED (user_guid),
        CONSTRAINT CK_bitsight_user_details_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_user_quota PRIMARY KEY CLUSTERED (quota_type),
        CONSTRAINT CK_bitsight_user_quota_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_user_company_views', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_user_company_views (
        user_guid    UNIQUEIDENTIFIER NOT NULL,
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_user_company_views PRIMARY KEY CLUSTERED (user_guid, company_guid),
        CONSTRAINT CK_bitsight_user_company_views_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_companies PRIMARY KEY CLUSTERED (company_guid),
        CONSTRAINT CK_bitsight_companies_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_details (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_details PRIMARY KEY CLUSTERED (company_guid),
        CONSTRAINT CK_bitsight_company_details_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

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
        CONSTRAINT PK_bitsight_company_relationships PRIMARY KEY CLUSTERED (relationship_guid),
        CONSTRAINT CK_bitsight_company_relationships_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_requests', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_requests (
        request_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_requests PRIMARY KEY CLUSTERED (request_guid),
        CONSTRAINT CK_bitsight_company_requests_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_client_access_links', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_client_access_links (
        link_guid    UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_client_access_links PRIMARY KEY CLUSTERED (link_guid),
        CONSTRAINT CK_bitsight_client_access_links_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_portfolio PRIMARY KEY CLUSTERED (company_guid),
        CONSTRAINT CK_bitsight_portfolio_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_current_ratings PRIMARY KEY CLUSTERED (company_guid),
        CONSTRAINT CK_bitsight_current_ratings_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_ratings_history PRIMARY KEY CLUSTERED (company_guid, rating_date),
        CONSTRAINT CK_bitsight_ratings_history_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_findings PRIMARY KEY CLUSTERED (finding_guid),
        CONSTRAINT CK_bitsight_findings_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_findings_statistics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_findings_statistics (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_findings_statistics PRIMARY KEY CLUSTERED (company_guid),
        CONSTRAINT CK_bitsight_findings_statistics_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_observations', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_observations (
        observation_guid UNIQUEIDENTIFIER NOT NULL,
        company_guid     UNIQUEIDENTIFIER NULL,
        ingested_at      DATETIME2(7) NOT NULL,
        raw_payload      NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_observations PRIMARY KEY CLUSTERED (observation_guid),
        CONSTRAINT CK_bitsight_observations_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_finding_comments PRIMARY KEY CLUSTERED (finding_guid, comment_guid),
        CONSTRAINT CK_bitsight_finding_comments_raw_payload_json CHECK (ISJSON(raw_payload) = 1),
        CONSTRAINT CK_bitsight_finding_comments_tagged_users_json
            CHECK (tagged_users IS NULL OR ISJSON(tagged_users) = 1),
        CONSTRAINT CK_bitsight_finding_comments_remediation_json
            CHECK (remediation IS NULL OR ISJSON(remediation) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_findings_statistics_global', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_findings_statistics_global (
        scope       NVARCHAR(64) NOT NULL
            CONSTRAINT DF_bitsight_findings_statistics_global_scope DEFAULT (N'global'),
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_findings_statistics_global PRIMARY KEY CLUSTERED (scope),
        CONSTRAINT CK_bitsight_findings_statistics_global_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_findings_summaries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_findings_summaries (
        scope       NVARCHAR(64) NOT NULL
            CONSTRAINT DF_bitsight_findings_summaries_scope DEFAULT (N'global'),
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_findings_summaries PRIMARY KEY CLUSTERED (scope),
        CONSTRAINT CK_bitsight_findings_summaries_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_company_infrastructure PRIMARY KEY CLUSTERED (company_guid, temporary_id),
        CONSTRAINT CK_bitsight_company_infrastructure_raw_payload_json CHECK (ISJSON(raw_payload) = 1),
        CONSTRAINT CK_bitsight_company_infrastructure_tags_json CHECK (tags IS NULL OR ISJSON(tags) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_asset_summaries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_asset_summaries (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_asset_summaries PRIMARY KEY CLUSTERED (company_guid),
        CONSTRAINT CK_bitsight_asset_summaries_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_asset_risk_matrix', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_asset_risk_matrix (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_asset_risk_matrix PRIMARY KEY CLUSTERED (company_guid),
        CONSTRAINT CK_bitsight_asset_risk_matrix_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_my_infrastructure PRIMARY KEY CLUSTERED (ingest_id),
        CONSTRAINT CK_bitsight_my_infrastructure_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_threats', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threats (
        threat_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threats PRIMARY KEY CLUSTERED (threat_guid),
        CONSTRAINT CK_bitsight_threats_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_threat_statistics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threat_statistics (
        scope       NVARCHAR(64) NOT NULL
            CONSTRAINT DF_bitsight_threat_statistics_scope DEFAULT (N'global'),
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threat_statistics PRIMARY KEY CLUSTERED (scope),
        CONSTRAINT CK_bitsight_threat_statistics_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_threats_impact', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threats_impact (
        threat_guid  UNIQUEIDENTIFIER NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threats_impact PRIMARY KEY CLUSTERED (threat_guid, ingested_at),
        CONSTRAINT CK_bitsight_threats_impact_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_threats_evidence', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threats_evidence (
        threat_guid UNIQUEIDENTIFIER NOT NULL,
        entity_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threats_evidence PRIMARY KEY CLUSTERED (threat_guid, entity_guid, ingested_at),
        CONSTRAINT CK_bitsight_threats_evidence_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_products', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_products (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        product_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_products PRIMARY KEY CLUSTERED (company_guid, product_guid),
        CONSTRAINT CK_bitsight_company_products_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_domain_products PRIMARY KEY CLUSTERED (company_guid, domain_name, product_guid),
        CONSTRAINT CK_bitsight_domain_products_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_domain_providers PRIMARY KEY CLUSTERED (company_guid, domain_name, provider_guid),
        CONSTRAINT CK_bitsight_domain_providers_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_provider_products', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_provider_products (
        provider_guid UNIQUEIDENTIFIER NOT NULL,
        product_guid  UNIQUEIDENTIFIER NOT NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_provider_products PRIMARY KEY CLUSTERED (provider_guid, product_guid),
        CONSTRAINT CK_bitsight_provider_products_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_provider_dependencies', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_provider_dependencies (
        provider_guid UNIQUEIDENTIFIER NOT NULL,
        company_guid  UNIQUEIDENTIFIER NOT NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_provider_dependencies PRIMARY KEY CLUSTERED (provider_guid, company_guid),
        CONSTRAINT CK_bitsight_provider_dependencies_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_ratings_tree_product_companies', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_ratings_tree_product_companies (
        product_guid UNIQUEIDENTIFIER NOT NULL,
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_ratings_tree_product_companies PRIMARY KEY CLUSTERED (product_guid, company_guid),
        CONSTRAINT CK_bitsight_ratings_tree_product_companies_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_ratings_tree_product_types', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_ratings_tree_product_types (
        product_type NVARCHAR(255) NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_ratings_tree_product_types PRIMARY KEY CLUSTERED (product_type),
        CONSTRAINT CK_bitsight_ratings_tree_product_types_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_service_providers', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_service_providers (
        company_guid  UNIQUEIDENTIFIER NOT NULL,
        provider_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_service_providers PRIMARY KEY CLUSTERED (company_guid, provider_guid),
        CONSTRAINT CK_bitsight_service_providers_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_news', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_news (
        news_guid    UNIQUEIDENTIFIER NOT NULL,
        published_at DATETIME2(7) NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_news PRIMARY KEY CLUSTERED (news_guid),
        CONSTRAINT CK_bitsight_news_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_alerts', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_alerts (
        alert_guid  UNIQUEIDENTIFIER NOT NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_alerts PRIMARY KEY CLUSTERED (alert_guid),
        CONSTRAINT CK_bitsight_alerts_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_insights', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_insights (
        insight_guid UNIQUEIDENTIFIER NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_insights PRIMARY KEY CLUSTERED (insight_guid),
        CONSTRAINT CK_bitsight_insights_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_nist_csf_reports', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_nist_csf_reports (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_nist_csf_reports PRIMARY KEY CLUSTERED (company_guid, ingested_at),
        CONSTRAINT CK_bitsight_nist_csf_reports_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_reports PRIMARY KEY CLUSTERED (report_id),
        CONSTRAINT CK_bitsight_reports_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_rapid_underwriting_assessments', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_rapid_underwriting_assessments (
        company_name NVARCHAR(255) NOT NULL,
        domain       NVARCHAR(255) NULL,
        requested_at DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_rapid_underwriting_assessments PRIMARY KEY CLUSTERED (company_name, requested_at),
        CONSTRAINT CK_bitsight_rapid_underwriting_assessments_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_industries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_industries (
        industry_slug NVARCHAR(255) NOT NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_industries PRIMARY KEY CLUSTERED (industry_slug),
        CONSTRAINT CK_bitsight_industries_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_tiers', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_tiers (
        tier_slug   NVARCHAR(255) NOT NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_tiers PRIMARY KEY CLUSTERED (tier_slug),
        CONSTRAINT CK_bitsight_tiers_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_lifecycle_states', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_lifecycle_states (
        lifecycle_slug NVARCHAR(255) NOT NULL,
        ingested_at    DATETIME2(7) NOT NULL,
        raw_payload    NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_lifecycle_states PRIMARY KEY CLUSTERED (lifecycle_slug),
        CONSTRAINT CK_bitsight_lifecycle_states_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_static_data', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_static_data (
        scope       NVARCHAR(64) NOT NULL
            CONSTRAINT DF_bitsight_static_data_scope DEFAULT (N'global'),
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_static_data PRIMARY KEY CLUSTERED (scope),
        CONSTRAINT CK_bitsight_static_data_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_statistics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_statistics (
        scope       NVARCHAR(64) NOT NULL
            CONSTRAINT DF_bitsight_statistics_scope DEFAULT (N'global'),
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_statistics PRIMARY KEY CLUSTERED (scope),
        CONSTRAINT CK_bitsight_statistics_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_risk_vectors_summary', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_risk_vectors_summary (
        scope        NVARCHAR(64) NOT NULL
            CONSTRAINT DF_bitsight_risk_vectors_summary_scope DEFAULT (N'global'),
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at  DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_risk_vectors_summary PRIMARY KEY CLUSTERED (scope, ingested_at),
        CONSTRAINT CK_bitsight_risk_vectors_summary_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_peer_analytics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_peer_analytics (
        scope         NVARCHAR(64) NOT NULL
            CONSTRAINT DF_bitsight_peer_analytics_scope DEFAULT (N'global'),
        company_guid  UNIQUEIDENTIFIER NULL,
        industry_slug NVARCHAR(255) NULL,
        ingested_at   DATETIME2(7) NOT NULL,
        raw_payload   NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_peer_analytics PRIMARY KEY CLUSTERED (scope, ingested_at),
        CONSTRAINT CK_bitsight_peer_analytics_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

---------------------------------------------------------------
-- SUBSIDIARIES
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_subsidiaries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_subsidiaries (
        subsidiary_guid     UNIQUEIDENTIFIER NOT NULL,
        parent_company_guid UNIQUEIDENTIFIER NULL,
        ingested_at         DATETIME2(7) NOT NULL,
        raw_payload         NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_subsidiaries PRIMARY KEY CLUSTERED (subsidiary_guid),
        CONSTRAINT CK_bitsight_subsidiaries_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_subsidiary_statistics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_subsidiary_statistics (
        ingest_id       BIGINT IDENTITY(1,1) NOT NULL,
        subsidiary_guid UNIQUEIDENTIFIER NULL,
        ingested_at     DATETIME2(7) NOT NULL,
        raw_payload     NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_subsidiary_statistics PRIMARY KEY CLUSTERED (ingest_id),
        CONSTRAINT CK_bitsight_subsidiary_statistics_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_folders PRIMARY KEY CLUSTERED (folder_guid),
        CONSTRAINT CK_bitsight_folders_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_exposed_credentials PRIMARY KEY CLUSTERED (credential_guid),
        CONSTRAINT CK_bitsight_exposed_credentials_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
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
        CONSTRAINT PK_bitsight_subscriptions PRIMARY KEY CLUSTERED (subscription_guid),
        CONSTRAINT CK_bitsight_subscriptions_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_use_current_ratings_license', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_use_current_ratings_license (
        company_guid UNIQUEIDENTIFIER NOT NULL,
        requested_at DATETIME2(7) NOT NULL,
        raw_payload  NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_use_current_ratings_license PRIMARY KEY CLUSTERED (company_guid, requested_at),
        CONSTRAINT CK_bitsight_use_current_ratings_license_raw_payload_json CHECK (ISJSON(raw_payload) = 1)
    );
END;

---------------------------------------------------------------
-- GENERIC BITSIGHT API RESPONSE STORAGE
---------------------------------------------------------------
IF OBJECT_ID(N'dbo.bitsight_api_responses', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_api_responses (
        response_id      BIGINT IDENTITY(1,1) NOT NULL,
        endpoint_name    NVARCHAR(255) NOT NULL,
        endpoint_path    NVARCHAR(2048) NOT NULL,
        http_method      NVARCHAR(16) NOT NULL,
        api_version      NVARCHAR(64) NULL,
        company_guid     UNIQUEIDENTIFIER NULL,
        request_params   NVARCHAR(MAX) NULL,
        http_status      INT NOT NULL,
        response_headers NVARCHAR(MAX) NULL,
        response_payload NVARCHAR(MAX) NULL,
        content_type     NVARCHAR(255) NULL,
        page_offset      BIGINT NULL,
        page_cursor      NVARCHAR(2048) NULL,
        next_page_url    NVARCHAR(2048) NULL,
        requested_at     DATETIME2(7) NOT NULL,
        received_at      DATETIME2(7) NOT NULL,
        payload_hash     VARBINARY(32) NULL,

        CONSTRAINT PK_bitsight_api_responses PRIMARY KEY CLUSTERED (response_id),
        CONSTRAINT CK_bitsight_api_responses_http_method
            CHECK (http_method IN (N'GET', N'POST', N'PUT', N'PATCH', N'DELETE')),
        CONSTRAINT CK_bitsight_api_responses_http_status
            CHECK (http_status BETWEEN 100 AND 599),
        CONSTRAINT CK_bitsight_api_responses_request_params_json
            CHECK (request_params IS NULL OR ISJSON(request_params) = 1),
        CONSTRAINT CK_bitsight_api_responses_response_headers_json
            CHECK (response_headers IS NULL OR ISJSON(response_headers) = 1)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_api_records', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_api_records (
        record_id         BIGINT IDENTITY(1,1) NOT NULL,
        response_id       BIGINT NULL,
        endpoint_name     NVARCHAR(255) NOT NULL,
        endpoint_path     NVARCHAR(2048) NOT NULL,
        api_version       NVARCHAR(64) NULL,
        resource_type     NVARCHAR(255) NULL,
        resource_key      NVARCHAR(1024) NULL,
        parent_key        NVARCHAR(1024) NULL,
        company_guid      UNIQUEIDENTIFIER NULL,
        source_updated_at DATETIME2(7) NULL,
        ingested_at       DATETIME2(7) NOT NULL,
        raw_payload       NVARCHAR(MAX) NOT NULL,
        payload_hash      VARBINARY(32) NULL,

        CONSTRAINT PK_bitsight_api_records PRIMARY KEY CLUSTERED (record_id),
        CONSTRAINT FK_bitsight_api_records_response
            FOREIGN KEY (response_id)
            REFERENCES dbo.bitsight_api_responses (response_id),
        CONSTRAINT CK_bitsight_api_records_raw_payload_json
            CHECK (ISJSON(raw_payload) = 1)
    );
END;

---------------------------------------------------------------
-- OPERATOR-FRIENDLY INDEXES
---------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_users_email'
      AND object_id = OBJECT_ID(N'dbo.bitsight_users')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_users_email
    ON dbo.bitsight_users (email)
    WHERE email IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_companies_domain'
      AND object_id = OBJECT_ID(N'dbo.bitsight_companies')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_companies_domain
    ON dbo.bitsight_companies (domain)
    WHERE domain IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_findings_company_guid'
      AND object_id = OBJECT_ID(N'dbo.bitsight_findings')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_findings_company_guid
    ON dbo.bitsight_findings (company_guid)
    WHERE company_guid IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_observations_company_guid'
      AND object_id = OBJECT_ID(N'dbo.bitsight_observations')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_observations_company_guid
    ON dbo.bitsight_observations (company_guid)
    WHERE company_guid IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_news_published_at'
      AND object_id = OBJECT_ID(N'dbo.bitsight_news')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_news_published_at
    ON dbo.bitsight_news (published_at)
    WHERE published_at IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_ratings_history_company_date'
      AND object_id = OBJECT_ID(N'dbo.bitsight_ratings_history')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_ratings_history_company_date
    ON dbo.bitsight_ratings_history (company_guid, rating_date);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_my_infrastructure_ingested_at'
      AND object_id = OBJECT_ID(N'dbo.bitsight_my_infrastructure')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_my_infrastructure_ingested_at
    ON dbo.bitsight_my_infrastructure (ingested_at);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_subsidiary_statistics_ingested_at'
      AND object_id = OBJECT_ID(N'dbo.bitsight_subsidiary_statistics')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_subsidiary_statistics_ingested_at
    ON dbo.bitsight_subsidiary_statistics (ingested_at);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_api_responses_endpoint_received_at'
      AND object_id = OBJECT_ID(N'dbo.bitsight_api_responses')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_api_responses_endpoint_received_at
    ON dbo.bitsight_api_responses (endpoint_name, received_at);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_api_responses_company_guid'
      AND object_id = OBJECT_ID(N'dbo.bitsight_api_responses')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_api_responses_company_guid
    ON dbo.bitsight_api_responses (company_guid, received_at)
    WHERE company_guid IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_api_records_endpoint_resource_key'
      AND object_id = OBJECT_ID(N'dbo.bitsight_api_records')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_api_records_endpoint_resource_key
    ON dbo.bitsight_api_records (endpoint_name, resource_key)
    WHERE resource_key IS NOT NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_bitsight_api_records_company_guid'
      AND object_id = OBJECT_ID(N'dbo.bitsight_api_records')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bitsight_api_records_company_guid
    ON dbo.bitsight_api_records (company_guid, ingested_at)
    WHERE company_guid IS NOT NULL;
END;

IF OBJECT_ID(N'dbo.bitsight_access_groups', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_access_groups (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        access_group_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_access_groups PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_access_group_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_access_group_details (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        access_group_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_access_group_details PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_access_requests_outbox', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_access_requests_outbox (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        collaboration_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_access_requests_outbox PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_access_requests_outbox_summary', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_access_requests_outbox_summary (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        scope NVARCHAR(64) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_access_requests_outbox_summary PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_access_requests_inbox', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_access_requests_inbox (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        collaboration_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_access_requests_inbox PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_access_request_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_access_request_details (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        collaboration_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_access_request_details PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_alert_affected_companies', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_alert_affected_companies (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        alert_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_alert_affected_companies PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_assessment_report_data', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_assessment_report_data (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        assessment_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_assessment_report_data PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_assessment_report_templates', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_assessment_report_templates (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        template_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_assessment_report_templates PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_customer_api_tokens', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_customer_api_tokens (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        token_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_customer_api_tokens PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_country_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_country_details (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        country_code NVARCHAR(16) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_country_details PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_assets', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_assets (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        asset_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_assets PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_finding_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_finding_details (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_finding_details PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_compromised_systems_findings', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_compromised_systems_findings (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_compromised_systems_findings PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_botnet_infections_findings', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_botnet_infections_findings (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_botnet_infections_findings PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_spam_propagation_findings', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_spam_propagation_findings (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_spam_propagation_findings PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_malware_servers_findings', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_malware_servers_findings (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_malware_servers_findings PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_unsolicited_communications_findings', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_unsolicited_communications_findings (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_unsolicited_communications_findings PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_infrastructure_changes', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_infrastructure_changes (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_infrastructure_changes PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_infrastructure_attribution_reasons', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_infrastructure_attribution_reasons (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_infrastructure_attribution_reasons PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_infrastructure_tags', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_infrastructure_tags (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        tag_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_infrastructure_tags PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_probable_infrastructure_requests', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_probable_infrastructure_requests (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        request_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_probable_infrastructure_requests PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_probable_infrastructure_responses', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_probable_infrastructure_responses (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        request_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_probable_infrastructure_responses PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_was_broken_authentication_evidence', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_was_broken_authentication_evidence (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_was_broken_authentication_evidence PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_was_known_vulnerabilities_evidence', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_was_known_vulnerabilities_evidence (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_was_known_vulnerabilities_evidence PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_was_cross_site_scripting_evidence', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_was_cross_site_scripting_evidence (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_was_cross_site_scripting_evidence PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_was_security_misconfiguration_evidence', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_was_security_misconfiguration_evidence (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_was_security_misconfiguration_evidence PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_was_sensitive_data_exposure_evidence', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_was_sensitive_data_exposure_evidence (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_was_sensitive_data_exposure_evidence PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_lifecycle_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_lifecycle_details (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        lifecycle_slug NVARCHAR(255) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_lifecycle_details PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_domain_squatting', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_domain_squatting (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        domain_name NVARCHAR(255) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_domain_squatting PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_industry_country_ratings', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_industry_country_ratings (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        industry_slug NVARCHAR(255) NULL,
        country_code NVARCHAR(16) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_industry_country_ratings PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_industry_ratings_history', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_industry_ratings_history (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        industry_slug NVARCHAR(255) NULL,
        country_code NVARCHAR(16) NULL,
        rating_date DATE NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_industry_ratings_history PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_rating_change_explanations', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_rating_change_explanations (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_rating_change_explanations PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_peer_group_companies', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_peer_group_companies (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_peer_group_companies PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_peer_group_configuration', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_peer_group_configuration (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_peer_group_configuration PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_portfolio_summaries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_portfolio_summaries (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        scope NVARCHAR(64) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_portfolio_summaries PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_portfolio_public_disclosures', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_portfolio_public_disclosures (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        disclosure_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_portfolio_public_disclosures PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_portfolio_contacts', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_portfolio_contacts (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        user_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_portfolio_contacts PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_product_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_product_details (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        product_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_product_details PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_provider_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_provider_details (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        provider_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_provider_details PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_remediation_tracking', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_remediation_tracking (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        remediation_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_remediation_tracking PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_sovereign_network_resources', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_sovereign_network_resources (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        resource_guid UNIQUEIDENTIFIER NULL,
        country_code NVARCHAR(16) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_sovereign_network_resources PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_sovereign_observations', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_sovereign_observations (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        observation_guid UNIQUEIDENTIFIER NULL,
        country_code NVARCHAR(16) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_sovereign_observations PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_sovereign_observation_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_sovereign_observation_details (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        observation_guid UNIQUEIDENTIFIER NULL,
        risk_type NVARCHAR(128) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_sovereign_observation_details PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_sovereign_company_kpi', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_sovereign_company_kpi (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        country_code NVARCHAR(16) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_sovereign_company_kpi PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_enterprise_analytics', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_enterprise_analytics (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_enterprise_analytics PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_territory_country_industry_grades', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_territory_country_industry_grades (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        country_code NVARCHAR(16) NULL,
        industry_slug NVARCHAR(255) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_territory_country_industry_grades PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_threat_attestation_summaries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threat_attestation_summaries (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        threat_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threat_attestation_summaries PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_threat_attestation_queries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threat_attestation_queries (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        query_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threat_attestation_queries PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_threat_attestations', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_threat_attestations (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        threat_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_threat_attestations PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_threat_attestation_summaries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_threat_attestation_summaries (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        threat_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_threat_attestation_summaries PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_threat_attestation_queries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_threat_attestation_queries (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        query_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_threat_attestation_queries PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_company_threat_attestations', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_company_threat_attestations (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        threat_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_company_threat_attestations PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_tier_summaries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_tier_summaries (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        tier_slug NVARCHAR(255) NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_tier_summaries PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_tier_threshold_alerts', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_tier_threshold_alerts (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        tier_slug NVARCHAR(255) NULL,
        alert_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_tier_threshold_alerts PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_underwriting_guidelines', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_underwriting_guidelines (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        guideline_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_underwriting_guidelines PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_default_underwriting_guidelines', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_default_underwriting_guidelines (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        guideline_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_default_underwriting_guidelines PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_wfh_findings', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_wfh_findings (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_wfh_findings PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_wfh_finding_details', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_wfh_finding_details (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        finding_guid UNIQUEIDENTIFIER NULL,
        company_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_wfh_finding_details PRIMARY KEY CLUSTERED (ingest_id)
    );
END;

IF OBJECT_ID(N'dbo.bitsight_wfh_bulk_requests', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_wfh_bulk_requests (
        ingest_id   BIGINT IDENTITY(1,1) NOT NULL,
        request_guid UNIQUEIDENTIFIER NULL,
        ingested_at DATETIME2(7) NOT NULL,
        raw_payload NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_wfh_bulk_requests PRIMARY KEY CLUSTERED (ingest_id)
    );
END;
